#!/usr/bin/env python3
"""
Format ITM rates and depth results in a readable format for inspection.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

# Step 1: Calculate IV threshold
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

# Step 2: Determine monthly IV level for each ticker
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

# Step 3: Calculate ITM rates and depth
ticker_results = {}

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    avg_iv = ticker_avg_iv.get(ticker, 0)
    
    total_options = 0
    itm_options = 0
    itm_depths = []
    
    # Process weekly if high monthly IV
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Filter by 4-7% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.04) & 
                                  (df['probability_itm'] <= 0.07) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    total_options += len(filtered)
                    itm_count = (filtered['ITM'] == 'YES').sum()
                    itm_options += itm_count
                    
                    # Calculate depth for ITM options
                    itm_rows = filtered[filtered['ITM'] == 'YES']
                    if len(itm_rows) > 0 and 'underlying_spot_at_expiry' in itm_rows.columns and 'strike' in itm_rows.columns:
                        depths = itm_rows['underlying_spot_at_expiry'] - itm_rows['strike']
                        depths = depths[depths > 0]
                        itm_depths.extend(depths.tolist())
                        
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
                    
                    # Filter by 10-13% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.10) & 
                                  (df['probability_itm'] <= 0.13) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    total_options += len(filtered)
                    itm_count = (filtered['ITM'] == 'YES').sum()
                    itm_options += itm_count
                    
                    # Calculate depth for ITM options
                    itm_rows = filtered[filtered['ITM'] == 'YES']
                    if len(itm_rows) > 0 and 'underlying_spot_at_expiry' in itm_rows.columns and 'strike' in itm_rows.columns:
                        depths = itm_rows['underlying_spot_at_expiry'] - itm_rows['strike']
                        depths = depths[depths > 0]
                        itm_depths.extend(depths.tolist())
                        
                except Exception as e:
                    continue
    
    if total_options > 0:
        itm_rate = (itm_options / total_options) * 100
        
        avg_depth = np.mean(itm_depths) if itm_depths else 0
        median_depth = np.median(itm_depths) if itm_depths else 0
        min_depth = np.min(itm_depths) if itm_depths else 0
        max_depth = np.max(itm_depths) if itm_depths else 0
        
        ticker_results[ticker] = {
            'iv_level': monthly_iv_level,
            'avg_iv': avg_iv,
            'option_type': 'Weekly' if monthly_iv_level == 'high' else 'Monthly',
            'prob_range': '4-7%' if monthly_iv_level == 'high' else '10-13%',
            'total_options': total_options,
            'itm_options': itm_options,
            'itm_rate': itm_rate,
            'avg_depth': avg_depth,
            'median_depth': median_depth,
            'min_depth': min_depth,
            'max_depth': max_depth,
            'num_itm_with_depth': len(itm_depths)
        }

# Create output file
output_file = Path("itm_rates_and_depth_full_list.txt")

with open(output_file, 'w') as f:
    f.write("=" * 120 + "\n")
    f.write("FULL LIST: ITM RATES AND DEPTH FOR ALL STOCKS\n")
    f.write("=" * 120 + "\n")
    f.write(f"\nIV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)\n")
    f.write(f"Logic: Low IV → Monthly (10-13% prob ITM), High IV → Weekly (4-7% prob ITM)\n")
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
        f.write(f"  Total Options:       {r['total_options']:,}\n")
        f.write(f"  ITM Options:         {r['itm_options']:,}\n")
        f.write(f"  ITM Rate:            {r['itm_rate']:.2f}%\n")
        f.write(f"\n")
        f.write(f"  ITM Depth Statistics:\n")
        f.write(f"    Average Depth:     ${r['avg_depth']:.2f}\n")
        f.write(f"    Median Depth:       ${r['median_depth']:.2f}\n")
        f.write(f"    Min Depth:          ${r['min_depth']:.2f}\n")
        f.write(f"    Max Depth:          ${r['max_depth']:.2f}\n")
        f.write(f"    ITM Options with Depth Data: {r['num_itm_with_depth']}\n")
    
    # Summary table
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("SUMMARY TABLE\n")
    f.write("=" * 120 + "\n")
    f.write(f"{'Ticker':<8} {'IV%':<8} {'Type':<8} {'Prob':<8} {'Total':<10} {'ITM':<8} {'ITM%':<8} {'Avg$':<10} {'Med$':<10} {'Min$':<8} {'Max$':<10}\n")
    f.write("-" * 120 + "\n")
    
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        f.write(f"{ticker:<8} {r['avg_iv']*100 if r['avg_iv'] else 0:>6.2f}% {r['option_type']:<8} {r['prob_range']:<8} {r['total_options']:>9,} {r['itm_options']:>7,} {r['itm_rate']:>7.2f}% ${r['avg_depth']:>8.2f} ${r['median_depth']:>8.2f} ${r['min_depth']:>6.2f} ${r['max_depth']:>8.2f}\n")
    
    # Overall summary
    total_all = sum(r['total_options'] for r in ticker_results.values())
    itm_all = sum(r['itm_options'] for r in ticker_results.values())
    overall_rate = (itm_all / total_all * 100) if total_all > 0 else 0
    
    f.write("-" * 120 + "\n")
    f.write(f"{'TOTAL':<8} {'':<8} {'':<8} {'':<8} {total_all:>9,} {itm_all:>7,} {overall_rate:>7.2f}%\n")
    
    # Lists for easy copy-paste
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("DATA AS LISTS (for easy copy-paste)\n")
    f.write("=" * 120 + "\n\n")
    
    f.write("Ticker Names:\n")
    f.write(str(sorted_tickers) + "\n\n")
    
    f.write("ITM Rates (%):\n")
    f.write(str([round(ticker_results[t]['itm_rate'], 2) for t in sorted_tickers]) + "\n\n")
    
    f.write("Average Depth ($):\n")
    f.write(str([round(ticker_results[t]['avg_depth'], 2) for t in sorted_tickers]) + "\n\n")
    
    f.write("Median Depth ($):\n")
    f.write(str([round(ticker_results[t]['median_depth'], 2) for t in sorted_tickers]) + "\n\n")
    
    f.write("Max Depth ($):\n")
    f.write(str([round(ticker_results[t]['max_depth'], 2) for t in sorted_tickers]) + "\n\n")

print(f"✅ Full list saved to: {output_file}")
print(f"   Open the file to inspect all results in detail.")


