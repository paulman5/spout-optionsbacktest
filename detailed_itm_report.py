#!/usr/bin/env python3
"""
Detailed ITM liquidation report per ticker, per year, with exact dates and counts.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import sys

base_dir = Path("python-boilerplate/data")
output_file = Path("detailed_itm_report.txt")

# Create a class that writes to both file and console
class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()

# Open output file and redirect stdout
f = open(output_file, 'w')
original_stdout = sys.stdout
sys.stdout = Tee(original_stdout, f)

print("=" * 120)
print("DETAILED ITM LIQUIDATION REPORT")
print("=" * 120)
print("Per ticker, per year, with exact dates and ITM counts")
print()

# Calculate IV threshold
all_ivs = []
for ticker_dir in base_dir.iterdir():
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        for file in monthly_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    all_ivs.extend(ivs.tolist())
            except:
                continue

iv_threshold = np.median(all_ivs) if all_ivs else 0.30

# Determine monthly IV level for each ticker
ticker_monthly_iv = {}
for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_ivs = []
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        for file in monthly_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    monthly_ivs.extend(ivs.tolist())
            except:
                continue
    
    if monthly_ivs:
        avg_monthly_iv = np.mean(monthly_ivs)
        ticker_monthly_iv[ticker] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
    else:
        ticker_monthly_iv[ticker] = 'low'

# Collect detailed data
ticker_detailed_data = defaultdict(lambda: defaultdict(lambda: {
    'dates': [],
    'total_options': 0,
    'itm_options': 0
}))

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    
    # Process weekly if high monthly IV
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by 4-7% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.04) & 
                                  (df['probability_itm'] <= 0.07) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Group by year and expiration date
                    for year, year_group in filtered.groupby('year'):
                        for exp_date, exp_group in year_group.groupby('expiration_date'):
                            total = len(exp_group)
                            itm_count = (exp_group['ITM'] == 'YES').sum()
                            
                            ticker_detailed_data[ticker][year]['dates'].append({
                                'expiration_date': exp_date,
                                'total': total,
                                'itm': itm_count,
                                'type': 'Weekly'
                            })
                            ticker_detailed_data[ticker][year]['total_options'] += total
                            ticker_detailed_data[ticker][year]['itm_options'] += itm_count
                            
                except Exception as e:
                    continue
    
    # Process monthly if low monthly IV
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by 10-15% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.10) & 
                                  (df['probability_itm'] <= 0.15) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Group by year and expiration date
                    for year, year_group in filtered.groupby('year'):
                        for exp_date, exp_group in year_group.groupby('expiration_date'):
                            total = len(exp_group)
                            itm_count = (exp_group['ITM'] == 'YES').sum()
                            
                            ticker_detailed_data[ticker][year]['dates'].append({
                                'expiration_date': exp_date,
                                'total': total,
                                'itm': itm_count,
                                'type': 'Monthly'
                            })
                            ticker_detailed_data[ticker][year]['total_options'] += total
                            ticker_detailed_data[ticker][year]['itm_options'] += itm_count
                            
                except Exception as e:
                    continue

# Print detailed report
print("=" * 120)
print("DETAILED ITM REPORT BY TICKER AND YEAR")
print("=" * 120)
print()

for ticker in sorted(ticker_detailed_data.keys()):
    ticker_data = ticker_detailed_data[ticker]
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    option_type = 'Weekly (4-7% prob ITM)' if monthly_iv_level == 'high' else 'Monthly (10-15% prob ITM)'
    
    print(f"\n{'='*120}")
    print(f"{ticker} - {option_type}")
    print(f"{'='*120}")
    
    total_ticker_options = 0
    total_ticker_itm = 0
    
    for year in sorted(ticker_data.keys()):
        year_data = ticker_data[year]
        total = year_data['total_options']
        itm = year_data['itm_options']
        rate = (itm / total * 100) if total > 0 else 0
        
        total_ticker_options += total
        total_ticker_itm += itm
        
        print(f"\n  {year}:")
        print(f"    Total Options: {total:,}")
        print(f"    ITM Options: {itm:,}")
        print(f"    ITM Rate: {rate:.2f}%")
        print(f"    Expiration Dates ({len(year_data['dates'])} dates):")
        
        # Sort dates by expiration_date
        sorted_dates = sorted(year_data['dates'], key=lambda x: x['expiration_date'])
        
        for date_info in sorted_dates:
            date_rate = (date_info['itm'] / date_info['total'] * 100) if date_info['total'] > 0 else 0
            print(f"      {date_info['expiration_date']}: {date_info['total']:,} options, {date_info['itm']:,} ITM ({date_rate:.2f}%)")
    
    # Ticker summary
    ticker_rate = (total_ticker_itm / total_ticker_options * 100) if total_ticker_options > 0 else 0
    print(f"\n  Ticker Summary:")
    print(f"    Total Options: {total_ticker_options:,}")
    print(f"    Total ITM: {total_ticker_itm:,}")
    print(f"    Overall ITM Rate: {ticker_rate:.2f}%")

# Summary lists
print("\n" + "=" * 120)
print("SUMMARY LISTS")
print("=" * 120)

weekly_tickers = []
monthly_tickers = []
weekly_rates = []
monthly_rates = []

for ticker in sorted(ticker_detailed_data.keys()):
    ticker_data = ticker_detailed_data[ticker]
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    
    total_options = sum(year_data['total_options'] for year_data in ticker_data.values())
    total_itm = sum(year_data['itm_options'] for year_data in ticker_data.values())
    rate = (total_itm / total_options * 100) if total_options > 0 else 0
    
    if monthly_iv_level == 'high':
        weekly_tickers.append(ticker)
        weekly_rates.append(round(rate, 2))
    else:
        monthly_tickers.append(ticker)
        monthly_rates.append(round(rate, 2))

print("\nWeekly Tickers (High Monthly IV):")
print(weekly_tickers)
print("\nWeekly ITM Rates (%) - with ticker names:")
weekly_list = [f"{t}: {r:.2f}%" for t, r in zip(weekly_tickers, weekly_rates)]
print(weekly_list)
print("\nWeekly ITM Rates (%) - numbers only:")
print([round(r, 2) for r in weekly_rates])

print("\nMonthly Tickers (Low Monthly IV):")
print(monthly_tickers)
print("\nMonthly ITM Rates (%) - with ticker names:")
monthly_list = [f"{t}: {r:.2f}%" for t, r in zip(monthly_tickers, monthly_rates)]
print(monthly_list)
print("\nMonthly ITM Rates (%) - numbers only:")
print([round(r, 2) for r in monthly_rates])

print("\n" + "=" * 120)

# Close file and restore stdout
sys.stdout = original_stdout
f.close()
print(f"\nFull report saved to: {output_file}")

