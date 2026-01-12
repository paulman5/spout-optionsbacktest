#!/usr/bin/env python3
"""
Check IV and probability ITM coverage for all tickers across weekly (holidays) and monthly files.
Reports adequacy of data points.
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 120)
print("IV AND PROBABILITY ITM COVERAGE ANALYSIS FOR ALL TICKERS")
print("=" * 120)
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

results = []

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    
    ticker_stats = {
        'ticker': ticker,
        'holidays_files': 0,
        'monthly_files': 0,
        'holidays_rows': 0,
        'monthly_rows': 0,
        'holidays_iv_count': 0,
        'monthly_iv_count': 0,
        'holidays_prob_count': 0,
        'monthly_prob_count': 0,
        'holidays_iv_pct': 0.0,
        'monthly_iv_pct': 0.0,
        'holidays_prob_pct': 0.0,
        'monthly_prob_pct': 0.0,
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
                
                # Count IV and prob ITM
                iv_count = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                prob_count = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
                
                if subdir == 'holidays':
                    ticker_stats['holidays_files'] += 1
                    ticker_stats['holidays_rows'] += rows
                    ticker_stats['holidays_iv_count'] += iv_count
                    ticker_stats['holidays_prob_count'] += prob_count
                else:
                    ticker_stats['monthly_files'] += 1
                    ticker_stats['monthly_rows'] += rows
                    ticker_stats['monthly_iv_count'] += iv_count
                    ticker_stats['monthly_prob_count'] += prob_count
                
            except Exception as e:
                continue
    
    # Calculate percentages
    if ticker_stats['holidays_rows'] > 0:
        ticker_stats['holidays_iv_pct'] = ticker_stats['holidays_iv_count'] / ticker_stats['holidays_rows'] * 100
        ticker_stats['holidays_prob_pct'] = ticker_stats['holidays_prob_count'] / ticker_stats['holidays_rows'] * 100
    
    if ticker_stats['monthly_rows'] > 0:
        ticker_stats['monthly_iv_pct'] = ticker_stats['monthly_iv_count'] / ticker_stats['monthly_rows'] * 100
        ticker_stats['monthly_prob_pct'] = ticker_stats['monthly_prob_count'] / ticker_stats['monthly_rows'] * 100
    
    results.append(ticker_stats)

# Sort by total rows
results.sort(key=lambda x: x['holidays_rows'] + x['monthly_rows'], reverse=True)

# Print detailed report
print("=" * 120)
print("DETAILED COVERAGE REPORT BY TICKER")
print("=" * 120)
print(f"{'Ticker':<8} {'Type':<8} {'Files':<8} {'Rows':<12} {'IV Count':<12} {'IV %':<10} {'Prob Count':<12} {'Prob %':<10} {'Status':<15}")
print("-" * 120)

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    # Holidays (Weekly)
    if stat['holidays_rows'] > 0:
        iv_status = "✅ Good" if stat['holidays_iv_pct'] >= 80 else "⚠️ Low" if stat['holidays_iv_pct'] >= 50 else "❌ Poor"
        prob_status = "✅ Good" if stat['holidays_prob_pct'] >= 80 else "⚠️ Low" if stat['holidays_prob_pct'] >= 50 else "❌ Poor"
        status = f"{iv_status[:2]}/{prob_status[:2]}"
        
        print(f"{stat['ticker']:<8} {'Weekly':<8} {stat['holidays_files']:<8} "
              f"{stat['holidays_rows']:>11,} {stat['holidays_iv_count']:>11,} "
              f"{stat['holidays_iv_pct']:>9.1f}% {stat['holidays_prob_count']:>11,} "
              f"{stat['holidays_prob_pct']:>9.1f}% {status:<15}")
    
    # Monthly
    if stat['monthly_rows'] > 0:
        iv_status = "✅ Good" if stat['monthly_iv_pct'] >= 80 else "⚠️ Low" if stat['monthly_iv_pct'] >= 50 else "❌ Poor"
        prob_status = "✅ Good" if stat['monthly_prob_pct'] >= 80 else "⚠️ Low" if stat['monthly_prob_pct'] >= 50 else "❌ Poor"
        status = f"{iv_status[:2]}/{prob_status[:2]}"
        
        print(f"{stat['ticker']:<8} {'Monthly':<8} {stat['monthly_files']:<8} "
              f"{stat['monthly_rows']:>11,} {stat['monthly_iv_count']:>11,} "
              f"{stat['monthly_iv_pct']:>9.1f}% {stat['monthly_prob_count']:>11,} "
              f"{stat['monthly_prob_pct']:>9.1f}% {status:<15}")

print("-" * 120)

# Summary statistics
total_rows_all = sum(r['holidays_rows'] + r['monthly_rows'] for r in results)
total_iv_all = sum(r['holidays_iv_count'] + r['monthly_iv_count'] for r in results)
total_prob_all = sum(r['holidays_prob_count'] + r['monthly_prob_count'] for r in results)

print(f"\n{'TOTAL':<8} {'All':<8} {'':<8} "
      f"{total_rows_all:>11,} {total_iv_all:>11,} "
      f"{total_iv_all/total_rows_all*100:>9.1f}% {total_prob_all:>11,} "
      f"{total_prob_all/total_rows_all*100:>9.1f}%")

# Identify tickers that need improvement
print("\n" + "=" * 120)
print("TICKERS NEEDING IMPROVEMENT (IV or Prob < 80%)")
print("=" * 120)

needs_improvement = []
for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    issues = []
    if stat['holidays_rows'] > 0:
        if stat['holidays_iv_pct'] < 80:
            issues.append(f"Weekly IV: {stat['holidays_iv_pct']:.1f}%")
        if stat['holidays_prob_pct'] < 80:
            issues.append(f"Weekly Prob: {stat['holidays_prob_pct']:.1f}%")
    
    if stat['monthly_rows'] > 0:
        if stat['monthly_iv_pct'] < 80:
            issues.append(f"Monthly IV: {stat['monthly_iv_pct']:.1f}%")
        if stat['monthly_prob_pct'] < 80:
            issues.append(f"Monthly Prob: {stat['monthly_prob_pct']:.1f}%")
    
    if issues:
        needs_improvement.append((stat['ticker'], issues, stat))

if needs_improvement:
    for ticker, issues, stat in needs_improvement:
        print(f"\n{ticker}:")
        for issue in issues:
            print(f"  - {issue}")
        print(f"  Total: {stat['holidays_rows'] + stat['monthly_rows']:,} rows, "
              f"{stat['holidays_iv_count'] + stat['monthly_iv_count']:,} IV, "
              f"{stat['holidays_prob_count'] + stat['monthly_prob_count']:,} Prob")
else:
    print("✅ All tickers have adequate coverage (>= 80%)!")

# Coverage quality summary
print("\n" + "=" * 120)
print("COVERAGE QUALITY SUMMARY")
print("=" * 120)

excellent = []  # >= 90%
good = []       # 80-89%
fair = []       # 50-79%
poor = []       # < 50%

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    total_iv = stat['holidays_iv_count'] + stat['monthly_iv_count']
    total_prob = stat['holidays_prob_count'] + stat['monthly_prob_count']
    iv_pct = total_iv / total_rows * 100 if total_rows > 0 else 0
    prob_pct = total_prob / total_rows * 100 if total_rows > 0 else 0
    
    avg_pct = (iv_pct + prob_pct) / 2
    
    if avg_pct >= 90:
        excellent.append(stat['ticker'])
    elif avg_pct >= 80:
        good.append(stat['ticker'])
    elif avg_pct >= 50:
        fair.append(stat['ticker'])
    else:
        poor.append(stat['ticker'])

print(f"✅ Excellent (>= 90%): {len(excellent)} tickers - {', '.join(excellent)}")
print(f"✅ Good (80-89%): {len(good)} tickers - {', '.join(good)}")
print(f"⚠️  Fair (50-79%): {len(fair)} tickers - {', '.join(fair)}")
print(f"❌ Poor (< 50%): {len(poor)} tickers - {', '.join(poor)}")

print("\n" + "=" * 120)

#!/usr/bin/env python3
"""
Check IV and probability ITM coverage for all tickers across weekly (holidays) and monthly files.
Reports adequacy of data points.
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 120)
print("IV AND PROBABILITY ITM COVERAGE ANALYSIS FOR ALL TICKERS")
print("=" * 120)
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

results = []

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    
    ticker_stats = {
        'ticker': ticker,
        'holidays_files': 0,
        'monthly_files': 0,
        'holidays_rows': 0,
        'monthly_rows': 0,
        'holidays_iv_count': 0,
        'monthly_iv_count': 0,
        'holidays_prob_count': 0,
        'monthly_prob_count': 0,
        'holidays_iv_pct': 0.0,
        'monthly_iv_pct': 0.0,
        'holidays_prob_pct': 0.0,
        'monthly_prob_pct': 0.0,
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
                
                # Count IV and prob ITM
                iv_count = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                prob_count = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
                
                if subdir == 'holidays':
                    ticker_stats['holidays_files'] += 1
                    ticker_stats['holidays_rows'] += rows
                    ticker_stats['holidays_iv_count'] += iv_count
                    ticker_stats['holidays_prob_count'] += prob_count
                else:
                    ticker_stats['monthly_files'] += 1
                    ticker_stats['monthly_rows'] += rows
                    ticker_stats['monthly_iv_count'] += iv_count
                    ticker_stats['monthly_prob_count'] += prob_count
                
            except Exception as e:
                continue
    
    # Calculate percentages
    if ticker_stats['holidays_rows'] > 0:
        ticker_stats['holidays_iv_pct'] = ticker_stats['holidays_iv_count'] / ticker_stats['holidays_rows'] * 100
        ticker_stats['holidays_prob_pct'] = ticker_stats['holidays_prob_count'] / ticker_stats['holidays_rows'] * 100
    
    if ticker_stats['monthly_rows'] > 0:
        ticker_stats['monthly_iv_pct'] = ticker_stats['monthly_iv_count'] / ticker_stats['monthly_rows'] * 100
        ticker_stats['monthly_prob_pct'] = ticker_stats['monthly_prob_count'] / ticker_stats['monthly_rows'] * 100
    
    results.append(ticker_stats)

# Sort by total rows
results.sort(key=lambda x: x['holidays_rows'] + x['monthly_rows'], reverse=True)

# Print detailed report
print("=" * 120)
print("DETAILED COVERAGE REPORT BY TICKER")
print("=" * 120)
print(f"{'Ticker':<8} {'Type':<8} {'Files':<8} {'Rows':<12} {'IV Count':<12} {'IV %':<10} {'Prob Count':<12} {'Prob %':<10} {'Status':<15}")
print("-" * 120)

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    # Holidays (Weekly)
    if stat['holidays_rows'] > 0:
        iv_status = "✅ Good" if stat['holidays_iv_pct'] >= 80 else "⚠️ Low" if stat['holidays_iv_pct'] >= 50 else "❌ Poor"
        prob_status = "✅ Good" if stat['holidays_prob_pct'] >= 80 else "⚠️ Low" if stat['holidays_prob_pct'] >= 50 else "❌ Poor"
        status = f"{iv_status[:2]}/{prob_status[:2]}"
        
        print(f"{stat['ticker']:<8} {'Weekly':<8} {stat['holidays_files']:<8} "
              f"{stat['holidays_rows']:>11,} {stat['holidays_iv_count']:>11,} "
              f"{stat['holidays_iv_pct']:>9.1f}% {stat['holidays_prob_count']:>11,} "
              f"{stat['holidays_prob_pct']:>9.1f}% {status:<15}")
    
    # Monthly
    if stat['monthly_rows'] > 0:
        iv_status = "✅ Good" if stat['monthly_iv_pct'] >= 80 else "⚠️ Low" if stat['monthly_iv_pct'] >= 50 else "❌ Poor"
        prob_status = "✅ Good" if stat['monthly_prob_pct'] >= 80 else "⚠️ Low" if stat['monthly_prob_pct'] >= 50 else "❌ Poor"
        status = f"{iv_status[:2]}/{prob_status[:2]}"
        
        print(f"{stat['ticker']:<8} {'Monthly':<8} {stat['monthly_files']:<8} "
              f"{stat['monthly_rows']:>11,} {stat['monthly_iv_count']:>11,} "
              f"{stat['monthly_iv_pct']:>9.1f}% {stat['monthly_prob_count']:>11,} "
              f"{stat['monthly_prob_pct']:>9.1f}% {status:<15}")

print("-" * 120)

# Summary statistics
total_rows_all = sum(r['holidays_rows'] + r['monthly_rows'] for r in results)
total_iv_all = sum(r['holidays_iv_count'] + r['monthly_iv_count'] for r in results)
total_prob_all = sum(r['holidays_prob_count'] + r['monthly_prob_count'] for r in results)

print(f"\n{'TOTAL':<8} {'All':<8} {'':<8} "
      f"{total_rows_all:>11,} {total_iv_all:>11,} "
      f"{total_iv_all/total_rows_all*100:>9.1f}% {total_prob_all:>11,} "
      f"{total_prob_all/total_rows_all*100:>9.1f}%")

# Identify tickers that need improvement
print("\n" + "=" * 120)
print("TICKERS NEEDING IMPROVEMENT (IV or Prob < 80%)")
print("=" * 120)

needs_improvement = []
for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    issues = []
    if stat['holidays_rows'] > 0:
        if stat['holidays_iv_pct'] < 80:
            issues.append(f"Weekly IV: {stat['holidays_iv_pct']:.1f}%")
        if stat['holidays_prob_pct'] < 80:
            issues.append(f"Weekly Prob: {stat['holidays_prob_pct']:.1f}%")
    
    if stat['monthly_rows'] > 0:
        if stat['monthly_iv_pct'] < 80:
            issues.append(f"Monthly IV: {stat['monthly_iv_pct']:.1f}%")
        if stat['monthly_prob_pct'] < 80:
            issues.append(f"Monthly Prob: {stat['monthly_prob_pct']:.1f}%")
    
    if issues:
        needs_improvement.append((stat['ticker'], issues, stat))

if needs_improvement:
    for ticker, issues, stat in needs_improvement:
        print(f"\n{ticker}:")
        for issue in issues:
            print(f"  - {issue}")
        print(f"  Total: {stat['holidays_rows'] + stat['monthly_rows']:,} rows, "
              f"{stat['holidays_iv_count'] + stat['monthly_iv_count']:,} IV, "
              f"{stat['holidays_prob_count'] + stat['monthly_prob_count']:,} Prob")
else:
    print("✅ All tickers have adequate coverage (>= 80%)!")

# Coverage quality summary
print("\n" + "=" * 120)
print("COVERAGE QUALITY SUMMARY")
print("=" * 120)

excellent = []  # >= 90%
good = []       # 80-89%
fair = []       # 50-79%
poor = []       # < 50%

for stat in results:
    total_rows = stat['holidays_rows'] + stat['monthly_rows']
    if total_rows == 0:
        continue
    
    total_iv = stat['holidays_iv_count'] + stat['monthly_iv_count']
    total_prob = stat['holidays_prob_count'] + stat['monthly_prob_count']
    iv_pct = total_iv / total_rows * 100 if total_rows > 0 else 0
    prob_pct = total_prob / total_rows * 100 if total_rows > 0 else 0
    
    avg_pct = (iv_pct + prob_pct) / 2
    
    if avg_pct >= 90:
        excellent.append(stat['ticker'])
    elif avg_pct >= 80:
        good.append(stat['ticker'])
    elif avg_pct >= 50:
        fair.append(stat['ticker'])
    else:
        poor.append(stat['ticker'])

print(f"✅ Excellent (>= 90%): {len(excellent)} tickers - {', '.join(excellent)}")
print(f"✅ Good (80-89%): {len(good)} tickers - {', '.join(good)}")
print(f"⚠️  Fair (50-79%): {len(fair)} tickers - {', '.join(fair)}")
print(f"❌ Poor (< 50%): {len(poor)} tickers - {', '.join(poor)}")

print("\n" + "=" * 120)

