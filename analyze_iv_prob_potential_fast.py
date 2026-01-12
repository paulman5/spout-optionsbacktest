#!/usr/bin/env python3
"""
Fast analysis of IV and probability ITM calculation potential for all tickers.
Checks data availability without calculating IV for every row.
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("ANALYZING IV AND PROBABILITY ITM CALCULATION POTENTIAL FOR ALL TICKERS")
print("=" * 100)
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

results = []
total_potential_iv = 0
total_current_iv = 0
total_rows_all = 0

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    
    ticker_stats = {
        'ticker': ticker,
        'holidays_files': 0,
        'monthly_files': 0,
        'holidays_rows': 0,
        'monthly_rows': 0,
        'holidays_iv_current': 0,
        'monthly_iv_current': 0,
        'holidays_iv_potential': 0,
        'monthly_iv_potential': 0,
        'holidays_missing': 0,
        'monthly_missing': 0,
    }
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            try:
                df = pd.read_csv(file)
                rows = len(df)
                
                # Count current IV
                iv_current = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                
                # Ensure mid_price exists
                if 'mid_price' not in df.columns:
                    if 'high_price' in df.columns and 'low_price' in df.columns:
                        df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
                    elif 'close_price' in df.columns:
                        df['mid_price'] = df['close_price']
                    else:
                        df['mid_price'] = np.nan
                
                # Check for required columns
                required = ['mid_price', 'underlying_spot', 'strike', 'days_to_expiry']
                if not all(col in df.columns for col in required):
                    continue
                
                # Find rows that could potentially have IV calculated
                # Need: valid price (>= 0.01), underlying_spot, strike, days_to_expiry
                has_price = (df['mid_price'].notna() & (df['mid_price'] >= 0.01))
                if 'close_price' in df.columns:
                    has_price = has_price | (df['close_price'].notna() & (df['close_price'] >= 0.01))
                
                has_spot = df['underlying_spot'].notna() & (df['underlying_spot'] > 0)
                has_strike = df['strike'].notna() & (df['strike'] > 0)
                has_dte = df['days_to_expiry'].notna() & (df['days_to_expiry'] > 0)
                
                can_calculate = has_price & has_spot & has_strike & has_dte
                missing_iv = can_calculate & (df['implied_volatility'].isna() if 'implied_volatility' in df.columns else can_calculate)
                
                potential_iv = iv_current + missing_iv.sum()
                missing_count = missing_iv.sum()
                
                if subdir == 'holidays':
                    ticker_stats['holidays_files'] += 1
                    ticker_stats['holidays_rows'] += rows
                    ticker_stats['holidays_iv_current'] += iv_current
                    ticker_stats['holidays_iv_potential'] += potential_iv
                    ticker_stats['holidays_missing'] += missing_count
                else:
                    ticker_stats['monthly_files'] += 1
                    ticker_stats['monthly_rows'] += rows
                    ticker_stats['monthly_iv_current'] += iv_current
                    ticker_stats['monthly_iv_potential'] += potential_iv
                    ticker_stats['monthly_missing'] += missing_count
                
                total_rows_all += rows
                total_current_iv += iv_current
                total_potential_iv += potential_iv
                
            except Exception as e:
                continue
    
    # Calculate percentages
    total_rows = ticker_stats['holidays_rows'] + ticker_stats['monthly_rows']
    total_current = ticker_stats['holidays_iv_current'] + ticker_stats['monthly_iv_current']
    total_potential = ticker_stats['holidays_iv_potential'] + ticker_stats['monthly_iv_potential']
    
    if total_rows > 0:
        ticker_stats['current_pct'] = total_current / total_rows * 100
        ticker_stats['potential_pct'] = total_potential / total_rows * 100
        ticker_stats['improvement'] = total_potential - total_current
        ticker_stats['improvement_pct'] = ticker_stats['improvement'] / total_rows * 100
    else:
        ticker_stats['current_pct'] = 0
        ticker_stats['potential_pct'] = 0
        ticker_stats['improvement'] = 0
        ticker_stats['improvement_pct'] = 0
    
    results.append(ticker_stats)
    
    # Print progress
    if len(results) % 10 == 0:
        print(f"Processed {len(results)} tickers...", end='\r')

print(f"\nProcessed {len(results)} tickers")

# Sort by improvement potential (descending)
results.sort(key=lambda x: x['improvement'], reverse=True)

# Print summary table
print("\n" + "=" * 100)
print("SUMMARY BY TICKER - IV/PROB ITM CALCULATION POTENTIAL")
print("=" * 100)
print(f"{'Ticker':<8} {'Rows':<10} {'Current':<12} {'Potential':<12} {'Improvement':<12} {'Current%':<10} {'Potential%':<10}")
print("-" * 100)

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    marker = "⭐" if stat['improvement'] > 1000 else "  "
    print(f"{marker}{stat['ticker']:<6} "
          f"{total_rows:>9,} "
          f"{stat['holidays_iv_current'] + stat['monthly_iv_current']:>10,} "
          f"{stat['holidays_iv_potential'] + stat['monthly_iv_potential']:>10,} "
          f"{stat['improvement']:>10,} "
          f"{stat['current_pct']:>8.1f}% "
          f"{stat['potential_pct']:>9.1f}%")

print("-" * 100)
if total_rows_all > 0:
    print(f"{'TOTAL':<8} "
          f"{total_rows_all:>9,} "
          f"{total_current_iv:>10,} "
          f"{total_potential_iv:>10,} "
          f"{total_potential_iv - total_current_iv:>10,} "
          f"{total_current_iv/total_rows_all*100:>8.1f}% "
          f"{total_potential_iv/total_rows_all*100:>9.1f}%")

# Show top opportunities
print("\n" + "=" * 100)
print("⭐ TOP OPPORTUNITIES FOR IMPROVEMENT (sorted by potential improvement)")
print("=" * 100)

top_opportunities = [r for r in results if r['improvement'] > 100]
top_opportunities.sort(key=lambda x: x['improvement'], reverse=True)

for stat in top_opportunities[:20]:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    print(f"\n{stat['ticker']}:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Current IV: {stat['holidays_iv_current'] + stat['monthly_iv_current']:,} ({stat['current_pct']:.1f}%)")
    print(f"  Potential IV: {stat['holidays_iv_potential'] + stat['monthly_iv_potential']:,} ({stat['potential_pct']:.1f}%)")
    print(f"  Improvement: +{stat['improvement']:,} ({stat['improvement_pct']:.1f}%)")
    print(f"  Holidays: {stat['holidays_iv_current']:,}/{stat['holidays_rows']:,} current, "
          f"{stat['holidays_iv_potential']:,} potential (+{stat['holidays_iv_potential'] - stat['holidays_iv_current']:,})")
    print(f"  Monthly: {stat['monthly_iv_current']:,}/{stat['monthly_rows']:,} current, "
          f"{stat['monthly_iv_potential']:,} potential (+{stat['monthly_iv_potential'] - stat['monthly_iv_current']:,})")

print("\n" + "=" * 100)
print("OVERALL STATISTICS")
print("=" * 100)
print(f"Total rows analyzed: {total_rows_all:,}")
if total_rows_all > 0:
    print(f"Current IV coverage: {total_current_iv:,} ({total_current_iv/total_rows_all*100:.1f}%)")
    print(f"Potential IV coverage: {total_potential_iv:,} ({total_potential_iv/total_rows_all*100:.1f}%)")
    print(f"Potential improvement: +{total_potential_iv - total_current_iv:,} ({((total_potential_iv - total_current_iv)/total_rows_all*100):.1f}%)")
print("=" * 100)

#!/usr/bin/env python3
"""
Fast analysis of IV and probability ITM calculation potential for all tickers.
Checks data availability without calculating IV for every row.
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("ANALYZING IV AND PROBABILITY ITM CALCULATION POTENTIAL FOR ALL TICKERS")
print("=" * 100)
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

results = []
total_potential_iv = 0
total_current_iv = 0
total_rows_all = 0

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    
    ticker_stats = {
        'ticker': ticker,
        'holidays_files': 0,
        'monthly_files': 0,
        'holidays_rows': 0,
        'monthly_rows': 0,
        'holidays_iv_current': 0,
        'monthly_iv_current': 0,
        'holidays_iv_potential': 0,
        'monthly_iv_potential': 0,
        'holidays_missing': 0,
        'monthly_missing': 0,
    }
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            try:
                df = pd.read_csv(file)
                rows = len(df)
                
                # Count current IV
                iv_current = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                
                # Ensure mid_price exists
                if 'mid_price' not in df.columns:
                    if 'high_price' in df.columns and 'low_price' in df.columns:
                        df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
                    elif 'close_price' in df.columns:
                        df['mid_price'] = df['close_price']
                    else:
                        df['mid_price'] = np.nan
                
                # Check for required columns
                required = ['mid_price', 'underlying_spot', 'strike', 'days_to_expiry']
                if not all(col in df.columns for col in required):
                    continue
                
                # Find rows that could potentially have IV calculated
                # Need: valid price (>= 0.01), underlying_spot, strike, days_to_expiry
                has_price = (df['mid_price'].notna() & (df['mid_price'] >= 0.01))
                if 'close_price' in df.columns:
                    has_price = has_price | (df['close_price'].notna() & (df['close_price'] >= 0.01))
                
                has_spot = df['underlying_spot'].notna() & (df['underlying_spot'] > 0)
                has_strike = df['strike'].notna() & (df['strike'] > 0)
                has_dte = df['days_to_expiry'].notna() & (df['days_to_expiry'] > 0)
                
                can_calculate = has_price & has_spot & has_strike & has_dte
                missing_iv = can_calculate & (df['implied_volatility'].isna() if 'implied_volatility' in df.columns else can_calculate)
                
                potential_iv = iv_current + missing_iv.sum()
                missing_count = missing_iv.sum()
                
                if subdir == 'holidays':
                    ticker_stats['holidays_files'] += 1
                    ticker_stats['holidays_rows'] += rows
                    ticker_stats['holidays_iv_current'] += iv_current
                    ticker_stats['holidays_iv_potential'] += potential_iv
                    ticker_stats['holidays_missing'] += missing_count
                else:
                    ticker_stats['monthly_files'] += 1
                    ticker_stats['monthly_rows'] += rows
                    ticker_stats['monthly_iv_current'] += iv_current
                    ticker_stats['monthly_iv_potential'] += potential_iv
                    ticker_stats['monthly_missing'] += missing_count
                
                total_rows_all += rows
                total_current_iv += iv_current
                total_potential_iv += potential_iv
                
            except Exception as e:
                continue
    
    # Calculate percentages
    total_rows = ticker_stats['holidays_rows'] + ticker_stats['monthly_rows']
    total_current = ticker_stats['holidays_iv_current'] + ticker_stats['monthly_iv_current']
    total_potential = ticker_stats['holidays_iv_potential'] + ticker_stats['monthly_iv_potential']
    
    if total_rows > 0:
        ticker_stats['current_pct'] = total_current / total_rows * 100
        ticker_stats['potential_pct'] = total_potential / total_rows * 100
        ticker_stats['improvement'] = total_potential - total_current
        ticker_stats['improvement_pct'] = ticker_stats['improvement'] / total_rows * 100
    else:
        ticker_stats['current_pct'] = 0
        ticker_stats['potential_pct'] = 0
        ticker_stats['improvement'] = 0
        ticker_stats['improvement_pct'] = 0
    
    results.append(ticker_stats)
    
    # Print progress
    if len(results) % 10 == 0:
        print(f"Processed {len(results)} tickers...", end='\r')

print(f"\nProcessed {len(results)} tickers")

# Sort by improvement potential (descending)
results.sort(key=lambda x: x['improvement'], reverse=True)

# Print summary table
print("\n" + "=" * 100)
print("SUMMARY BY TICKER - IV/PROB ITM CALCULATION POTENTIAL")
print("=" * 100)
print(f"{'Ticker':<8} {'Rows':<10} {'Current':<12} {'Potential':<12} {'Improvement':<12} {'Current%':<10} {'Potential%':<10}")
print("-" * 100)

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    marker = "⭐" if stat['improvement'] > 1000 else "  "
    print(f"{marker}{stat['ticker']:<6} "
          f"{total_rows:>9,} "
          f"{stat['holidays_iv_current'] + stat['monthly_iv_current']:>10,} "
          f"{stat['holidays_iv_potential'] + stat['monthly_iv_potential']:>10,} "
          f"{stat['improvement']:>10,} "
          f"{stat['current_pct']:>8.1f}% "
          f"{stat['potential_pct']:>9.1f}%")

print("-" * 100)
if total_rows_all > 0:
    print(f"{'TOTAL':<8} "
          f"{total_rows_all:>9,} "
          f"{total_current_iv:>10,} "
          f"{total_potential_iv:>10,} "
          f"{total_potential_iv - total_current_iv:>10,} "
          f"{total_current_iv/total_rows_all*100:>8.1f}% "
          f"{total_potential_iv/total_rows_all*100:>9.1f}%")

# Show top opportunities
print("\n" + "=" * 100)
print("⭐ TOP OPPORTUNITIES FOR IMPROVEMENT (sorted by potential improvement)")
print("=" * 100)

top_opportunities = [r for r in results if r['improvement'] > 100]
top_opportunities.sort(key=lambda x: x['improvement'], reverse=True)

for stat in top_opportunities[:20]:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    print(f"\n{stat['ticker']}:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Current IV: {stat['holidays_iv_current'] + stat['monthly_iv_current']:,} ({stat['current_pct']:.1f}%)")
    print(f"  Potential IV: {stat['holidays_iv_potential'] + stat['monthly_iv_potential']:,} ({stat['potential_pct']:.1f}%)")
    print(f"  Improvement: +{stat['improvement']:,} ({stat['improvement_pct']:.1f}%)")
    print(f"  Holidays: {stat['holidays_iv_current']:,}/{stat['holidays_rows']:,} current, "
          f"{stat['holidays_iv_potential']:,} potential (+{stat['holidays_iv_potential'] - stat['holidays_iv_current']:,})")
    print(f"  Monthly: {stat['monthly_iv_current']:,}/{stat['monthly_rows']:,} current, "
          f"{stat['monthly_iv_potential']:,} potential (+{stat['monthly_iv_potential'] - stat['monthly_iv_current']:,})")

print("\n" + "=" * 100)
print("OVERALL STATISTICS")
print("=" * 100)
print(f"Total rows analyzed: {total_rows_all:,}")
if total_rows_all > 0:
    print(f"Current IV coverage: {total_current_iv:,} ({total_current_iv/total_rows_all*100:.1f}%)")
    print(f"Potential IV coverage: {total_potential_iv:,} ({total_potential_iv/total_rows_all*100:.1f}%)")
    print(f"Potential improvement: +{total_potential_iv - total_current_iv:,} ({((total_potential_iv - total_current_iv)/total_rows_all*100):.1f}%)")
print("=" * 100)

