#!/usr/bin/env python3
"""
Calculate ITM liquidation counts per year for all stocks based on IV levels:
- Low IV → Monthly with 10-13% probability ITM
- High IV → Weekly with 4-7% probability ITM
Count ITM == 'YES' per year for liquidation tracking.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

print("=" * 120)
print("CALCULATING ITM LIQUIDATION COUNTS PER YEAR FOR ALL STOCKS")
print("=" * 120)
print("Logic:")
print("  - Low Monthly IV → Monthly options with 10-13% probability ITM")
print("  - High Monthly IV → Weekly options with 4-7% probability ITM")
print("  - Count ITM == 'YES' per year (liquidation events)")
print()

# Step 1: Calculate IV threshold
print("Step 1: Calculating IV threshold...")
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
print(f"IV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
print()

# Step 2: Determine monthly IV level for each ticker
print("Step 2: Determining monthly IV level for each ticker...")
ticker_monthly_iv = {}
ticker_avg_iv = {}

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
        ticker_avg_iv[ticker] = avg_monthly_iv
        ticker_monthly_iv[ticker] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
    else:
        ticker_avg_iv[ticker] = None
        ticker_monthly_iv[ticker] = 'low'

print(f"Found {len(ticker_monthly_iv)} tickers")
print()

# Step 3: Calculate ITM counts per year
print("Step 3: Calculating ITM liquidation counts per year...")
print()

ticker_results = {}

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    avg_iv = ticker_avg_iv.get(ticker, 0)
    
    # Store per-year data
    yearly_data = defaultdict(lambda: {'total': 0, 'itm_yes': 0})
    
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
                    
                    # Count per year
                    for year, year_group in filtered.groupby('year'):
                        yearly_data[year]['total'] += len(year_group)
                        yearly_data[year]['itm_yes'] += (year_group['ITM'] == 'YES').sum()
                        
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
                    
                    # Filter by 10-13% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.10) & 
                                  (df['probability_itm'] <= 0.13) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Count per year
                    for year, year_group in filtered.groupby('year'):
                        yearly_data[year]['total'] += len(year_group)
                        yearly_data[year]['itm_yes'] += (year_group['ITM'] == 'YES').sum()
                        
                except Exception as e:
                    continue
    
    if yearly_data:
        ticker_results[ticker] = {
            'iv_level': monthly_iv_level,
            'avg_iv': avg_iv,
            'option_type': 'Weekly' if monthly_iv_level == 'high' else 'Monthly',
            'prob_range': '4-7%' if monthly_iv_level == 'high' else '10-13%',
            'yearly_data': dict(yearly_data)
        }

# Create output file
output_file = Path("itm_liquidation_per_year.txt")

with open(output_file, 'w') as f:
    f.write("=" * 120 + "\n")
    f.write("ITM LIQUIDATION COUNTS PER YEAR FOR ALL STOCKS\n")
    f.write("=" * 120 + "\n")
    f.write(f"\nIV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)\n")
    f.write(f"Logic: Low IV → Monthly (10-13% prob ITM), High IV → Weekly (4-7% prob ITM)\n")
    f.write(f"Count: Number of ITM == 'YES' per year (liquidation events)\n")
    f.write("\n")
    
    # Sort by ticker
    sorted_tickers = sorted(ticker_results.keys())
    
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        f.write("\n" + "=" * 120 + "\n")
        f.write(f"TICKER: {ticker}\n")
        f.write("=" * 120 + "\n")
        f.write(f"  IV Level:           {r['iv_level'].upper()}\n")
        f.write(f"  Average IV:          {r['avg_iv']*100 if r['avg_iv'] else 0:.2f}%\n")
        f.write(f"  Option Type:         {r['option_type']}\n")
        f.write(f"  Probability Range:   {r['prob_range']}\n")
        f.write(f"\n")
        f.write(f"  Per Year Breakdown:\n")
        f.write(f"  {'Year':<8} {'Total Options':<15} {'ITM YES (Liquidations)':<25} {'ITM Rate':<12}\n")
        f.write(f"  {'-'*8} {'-'*15} {'-'*25} {'-'*12}\n")
        
        total_all_years = 0
        itm_all_years = 0
        
        for year in sorted(r['yearly_data'].keys()):
            year_data = r['yearly_data'][year]
            total = year_data['total']
            itm_yes = year_data['itm_yes']
            rate = (itm_yes / total * 100) if total > 0 else 0
            
            total_all_years += total
            itm_all_years += itm_yes
            
            f.write(f"  {year:<8} {total:>14,} {itm_yes:>24,} {rate:>11.2f}%\n")
        
        overall_rate = (itm_all_years / total_all_years * 100) if total_all_years > 0 else 0
        f.write(f"  {'-'*8} {'-'*15} {'-'*25} {'-'*12}\n")
        f.write(f"  {'TOTAL':<8} {total_all_years:>14,} {itm_all_years:>24,} {overall_rate:>11.2f}%\n")
    
    # Summary table
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("SUMMARY TABLE (Overall Totals)\n")
    f.write("=" * 120 + "\n")
    f.write(f"{'Ticker':<8} {'IV%':<8} {'Type':<8} {'Prob':<8} {'Total':<10} {'ITM YES':<10} {'ITM%':<8}\n")
    f.write("-" * 120 + "\n")
    
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        total_all = sum(yd['total'] for yd in r['yearly_data'].values())
        itm_all = sum(yd['itm_yes'] for yd in r['yearly_data'].values())
        rate = (itm_all / total_all * 100) if total_all > 0 else 0
        
        f.write(f"{ticker:<8} {r['avg_iv']*100 if r['avg_iv'] else 0:>6.2f}% {r['option_type']:<8} {r['prob_range']:<8} {total_all:>9,} {itm_all:>9,} {rate:>7.2f}%\n")
    
    # Overall summary
    total_all = sum(sum(yd['total'] for yd in r['yearly_data'].values()) for r in ticker_results.values())
    itm_all = sum(sum(yd['itm_yes'] for yd in r['yearly_data'].values()) for r in ticker_results.values())
    overall_rate = (itm_all / total_all * 100) if total_all > 0 else 0
    
    f.write("-" * 120 + "\n")
    f.write(f"{'TOTAL':<8} {'':<8} {'':<8} {'':<8} {total_all:>9,} {itm_all:>9,} {overall_rate:>7.2f}%\n")
    
    # Per-year summary across all tickers
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("PER-YEAR SUMMARY (All Tickers Combined)\n")
    f.write("=" * 120 + "\n")
    f.write(f"{'Year':<8} {'Total Options':<15} {'ITM YES (Liquidations)':<25} {'ITM Rate':<12}\n")
    f.write("-" * 120 + "\n")
    
    # Collect all years
    all_years = set()
    for r in ticker_results.values():
        all_years.update(r['yearly_data'].keys())
    
    for year in sorted(all_years):
        year_total = 0
        year_itm = 0
        for r in ticker_results.values():
            if year in r['yearly_data']:
                year_total += r['yearly_data'][year]['total']
                year_itm += r['yearly_data'][year]['itm_yes']
        
        rate = (year_itm / year_total * 100) if year_total > 0 else 0
        f.write(f"{year:<8} {year_total:>14,} {year_itm:>24,} {rate:>11.2f}%\n")
    
    # Lists for easy copy-paste
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("DATA AS LISTS (for easy copy-paste)\n")
    f.write("=" * 120 + "\n\n")
    
    f.write("Ticker Names:\n")
    f.write(str(sorted_tickers) + "\n\n")
    
    f.write("Overall ITM YES Counts (Total Liquidations):\n")
    f.write(str([sum(yd['itm_yes'] for yd in r['yearly_data'].values()) for r in [ticker_results[t] for t in sorted_tickers]]) + "\n\n")
    
    f.write("Overall ITM Rates (%):\n")
    f.write(str([round((sum(yd['itm_yes'] for yd in r['yearly_data'].values()) / sum(yd['total'] for yd in r['yearly_data'].values()) * 100) if sum(yd['total'] for yd in r['yearly_data'].values()) > 0 else 0, 2) for r in [ticker_results[t] for t in sorted_tickers]]) + "\n\n")

print(f"✅ Per-year ITM liquidation data saved to: {output_file}")
print(f"   Open the file to inspect all results by year.")

