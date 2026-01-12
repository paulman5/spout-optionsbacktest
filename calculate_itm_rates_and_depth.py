#!/usr/bin/env python3
"""
Calculate ITM rates and depth for all stocks based on IV levels:
- Low IV → Monthly with 10-13% probability ITM
- High IV → Weekly with 4-7% probability ITM
Also calculate how deep ITM options get called.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

print("=" * 120)
print("CALCULATING ITM RATES AND DEPTH FOR ALL STOCKS")
print("=" * 120)
print("Logic:")
print("  - Low Monthly IV → Monthly options with 10-13% probability ITM")
print("  - High Monthly IV → Weekly options with 4-7% probability ITM")
print("  - Calculate ITM rate and depth (how much above strike when ITM)")
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

# Step 3: Calculate ITM rates and depth
print("Step 3: Calculating ITM rates and depth...")
print()

ticker_results = {}

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    avg_iv = ticker_avg_iv.get(ticker, 0)
    
    total_options = 0
    itm_options = 0
    itm_depths = []  # List of (underlying_spot_at_expiry - strike) for ITM options
    
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
                        depths = depths[depths > 0]  # Only positive depths
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
                        depths = depths[depths > 0]  # Only positive depths
                        itm_depths.extend(depths.tolist())
                        
                except Exception as e:
                    continue
    
    if total_options > 0:
        itm_rate = (itm_options / total_options) * 100
        
        # Calculate depth statistics
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

# Step 4: Print results
print("=" * 120)
print("RESULTS: ITM RATES AND DEPTH BY TICKER")
print("=" * 120)
print()

# Sort by ticker name
sorted_tickers = sorted(ticker_results.keys())

print(f"{'Ticker':<8} {'IV Level':<10} {'Avg IV':<10} {'Type':<8} {'Prob Range':<12} {'Total':<10} {'ITM':<8} {'ITM Rate':<12} {'Avg Depth':<12} {'Median Depth':<14} {'Min Depth':<12} {'Max Depth':<12}")
print("-" * 120)

for ticker in sorted_tickers:
    r = ticker_results[ticker]
    print(f"{ticker:<8} {r['iv_level']:<10} {r['avg_iv']*100 if r['avg_iv'] else 0:>8.2f}% {'':<2} {r['option_type']:<8} {r['prob_range']:<12} {r['total_options']:>9,} {r['itm_options']:>7,} {r['itm_rate']:>11.2f}% {r['avg_depth']:>11.2f} {r['median_depth']:>13.2f} {r['min_depth']:>11.2f} {r['max_depth']:>11.2f}")

print("-" * 120)

# Summary statistics
total_all = sum(r['total_options'] for r in ticker_results.values())
itm_all = sum(r['itm_options'] for r in ticker_results.values())
overall_rate = (itm_all / total_all * 100) if total_all > 0 else 0

print(f"{'TOTAL':<8} {'':<10} {'':<10} {'':<8} {'':<12} {total_all:>9,} {itm_all:>7,} {overall_rate:>11.2f}%")
print()

# Print as lists
print("=" * 120)
print("DATA AS LISTS")
print("=" * 120)
print()

print("Ticker Names (alphabetical):")
print(sorted_tickers)
print()

print("IV Level (high/low):")
print([ticker_results[t]['iv_level'] for t in sorted_tickers])
print()

print("Average IV (%):")
print([round(ticker_results[t]['avg_iv']*100, 2) if ticker_results[t]['avg_iv'] else 0 for t in sorted_tickers])
print()

print("Option Type (Weekly/Monthly):")
print([ticker_results[t]['option_type'] for t in sorted_tickers])
print()

print("Probability ITM Range (%):")
print([ticker_results[t]['prob_range'] for t in sorted_tickers])
print()

print("Total Options:")
print([ticker_results[t]['total_options'] for t in sorted_tickers])
print()

print("ITM Options:")
print([ticker_results[t]['itm_options'] for t in sorted_tickers])
print()

print("ITM Rate (%):")
print([round(ticker_results[t]['itm_rate'], 2) for t in sorted_tickers])
print()

print("Average Depth (how much above strike when ITM):")
print([round(ticker_results[t]['avg_depth'], 2) for t in sorted_tickers])
print()

print("Median Depth:")
print([round(ticker_results[t]['median_depth'], 2) for t in sorted_tickers])
print()

print("Min Depth:")
print([round(ticker_results[t]['min_depth'], 2) for t in sorted_tickers])
print()

print("Max Depth:")
print([round(ticker_results[t]['max_depth'], 2) for t in sorted_tickers])
print()

print("=" * 120)


