#!/usr/bin/env python3
"""
Calculate ITM liquidation rate for options matching our probability ITM criteria:
- High monthly IV → Weekly with 4-7% probability ITM
- Low monthly IV → Monthly with 10-15% probability ITM
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

print("=" * 100)
print("CALCULATING ITM LIQUIDATION RATE FOR FILTERED OPTIONS")
print("=" * 100)
print("Only counting options that match our probability ITM criteria")
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
print(f"IV Threshold: {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
print()

ticker_stats = defaultdict(lambda: {
    'total_filtered': 0,
    'itm_filtered': 0,
    'by_year': defaultdict(lambda: {'total': 0, 'itm': 0})
})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    # Calculate monthly IV
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
        monthly_iv_level = 'high' if avg_monthly_iv >= iv_threshold else 'low'
    else:
        monthly_iv_level = 'low'
    
    # Process weekly if high monthly IV
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'ITM' not in df.columns or 'probability_itm' not in df.columns:
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
                    
                    # Filter by probability_itm: 4-7% for weekly when monthly IV is high
                    filtered = df[(df['probability_itm'] >= 0.04) & 
                                 (df['probability_itm'] <= 0.07) & 
                                 (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Count ITM
                    total_filtered = len(filtered)
                    itm_filtered = (filtered['ITM'] == 'YES').sum()
                    
                    ticker_stats[ticker]['total_filtered'] += total_filtered
                    ticker_stats[ticker]['itm_filtered'] += itm_filtered
                    
                    # Count by year
                    for year, group in filtered.groupby('year'):
                        year_total = len(group)
                        year_itm = (group['ITM'] == 'YES').sum()
                        ticker_stats[ticker]['by_year'][year]['total'] += year_total
                        ticker_stats[ticker]['by_year'][year]['itm'] += year_itm
                        
                except Exception as e:
                    continue
    
    # Process monthly if low monthly IV
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'ITM' not in df.columns or 'probability_itm' not in df.columns:
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
                    
                    # Filter by probability_itm: 10-15% for monthly when monthly IV is low
                    filtered = df[(df['probability_itm'] >= 0.10) & 
                                 (df['probability_itm'] <= 0.15) & 
                                 (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Count ITM
                    total_filtered = len(filtered)
                    itm_filtered = (filtered['ITM'] == 'YES').sum()
                    
                    ticker_stats[ticker]['total_filtered'] += total_filtered
                    ticker_stats[ticker]['itm_filtered'] += itm_filtered
                    
                    # Count by year
                    for year, group in filtered.groupby('year'):
                        year_total = len(group)
                        year_itm = (group['ITM'] == 'YES').sum()
                        ticker_stats[ticker]['by_year'][year]['total'] += year_total
                        ticker_stats[ticker]['by_year'][year]['itm'] += year_itm
                        
                except Exception as e:
                    continue

# Calculate percentages
print("=" * 100)
print("ITM LIQUIDATION RATE FOR FILTERED OPTIONS BY TICKER")
print("=" * 100)
print(f"{'Ticker':<8} {'Filtered Options':<18} {'ITM Options':<15} {'ITM Rate':<12} {'Years':<10} {'Avg per Year':<15}")
print("-" * 100)

ticker_itm_rates = {}
ticker_avg_yearly_rates = {}

for ticker in sorted(ticker_stats.keys()):
    stats = ticker_stats[ticker]
    total = stats['total_filtered']
    itm = stats['itm_filtered']
    
    if total == 0:
        continue
    
    overall_rate = (itm / total) * 100
    
    # Calculate average yearly rate
    yearly_rates = []
    for year, year_data in stats['by_year'].items():
        if year_data['total'] > 0:
            year_rate = (year_data['itm'] / year_data['total']) * 100
            yearly_rates.append(year_rate)
    
    avg_yearly_rate = np.mean(yearly_rates) if yearly_rates else 0
    num_years = len(yearly_rates)
    
    ticker_itm_rates[ticker] = overall_rate
    ticker_avg_yearly_rates[ticker] = avg_yearly_rate
    
    print(f"{ticker:<8} {total:>17,} {itm:>14,} {overall_rate:>11.2f}% {num_years:>9} {avg_yearly_rate:>14.2f}%")

print("-" * 100)

# Overall statistics
total_all = sum(s['total_filtered'] for s in ticker_stats.values())
itm_all = sum(s['itm_filtered'] for s in ticker_stats.values())
overall_rate_all = (itm_all / total_all) * 100 if total_all > 0 else 0

print(f"{'TOTAL':<8} {total_all:>17,} {itm_all:>14,} {overall_rate_all:>11.2f}%")

# Print as lists
print("\n" + "=" * 100)
print("ITM LIQUIDATION RATES AS LISTS (FILTERED OPTIONS ONLY)")
print("=" * 100)

print("\nOverall ITM Rate per Ticker (%):")
print([round(ticker_itm_rates[t], 2) for t in sorted(ticker_itm_rates.keys())])

print("\nAverage Yearly ITM Rate per Ticker (%):")
print([round(ticker_avg_yearly_rates[t], 2) for t in sorted(ticker_avg_yearly_rates.keys())])

print("\nTicker Names (alphabetical):")
print(sorted(ticker_itm_rates.keys()))

print("\n" + "=" * 100)

