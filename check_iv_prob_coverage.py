#!/usr/bin/env python3
"""
Check all tickers to see if they have enough data points for 
implied volatility and probability ITM in both monthly and holidays directories.
"""

import pandas as pd
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("CHECKING IV AND PROBABILITY ITM COVERAGE FOR ALL TICKERS")
print("=" * 100)
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

print(f"Found {len(ticker_dirs)} tickers to check\n")

# Statistics
results = []
low_coverage_tickers = []
total_files = 0
total_rows_all = 0
total_iv_all = 0
total_prob_all = 0

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    
    ticker_stats = {
        'ticker': ticker,
        'holidays_files': 0,
        'monthly_files': 0,
        'holidays_rows': 0,
        'monthly_rows': 0,
        'holidays_iv': 0,
        'monthly_iv': 0,
        'holidays_prob': 0,
        'monthly_prob': 0,
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
                
                # Count valid IV and probability ITM
                iv_valid = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                prob_valid = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
                
                iv_pct = (iv_valid / rows * 100) if rows > 0 else 0
                prob_pct = (prob_valid / rows * 100) if rows > 0 else 0
                
                if subdir == 'holidays':
                    ticker_stats['holidays_files'] += 1
                    ticker_stats['holidays_rows'] += rows
                    ticker_stats['holidays_iv'] += iv_valid
                    ticker_stats['holidays_prob'] += prob_valid
                else:
                    ticker_stats['monthly_files'] += 1
                    ticker_stats['monthly_rows'] += rows
                    ticker_stats['monthly_iv'] += iv_valid
                    ticker_stats['monthly_prob'] += prob_valid
                
                total_files += 1
                total_rows_all += rows
                total_iv_all += iv_valid
                total_prob_all += prob_valid
                
            except Exception as e:
                print(f"⚠️  Error reading {file}: {str(e)}")
    
    # Calculate percentages
    if ticker_stats['holidays_rows'] > 0:
        ticker_stats['holidays_iv_pct'] = ticker_stats['holidays_iv'] / ticker_stats['holidays_rows'] * 100
        ticker_stats['holidays_prob_pct'] = ticker_stats['holidays_prob'] / ticker_stats['holidays_rows'] * 100
    
    if ticker_stats['monthly_rows'] > 0:
        ticker_stats['monthly_iv_pct'] = ticker_stats['monthly_iv'] / ticker_stats['monthly_rows'] * 100
        ticker_stats['monthly_prob_pct'] = ticker_stats['monthly_prob'] / ticker_stats['monthly_rows'] * 100
    
    results.append(ticker_stats)
    
    # Flag tickers with low coverage (< 30% in either directory)
    if (ticker_stats['holidays_iv_pct'] < 30 and ticker_stats['holidays_rows'] > 0) or \
       (ticker_stats['monthly_iv_pct'] < 30 and ticker_stats['monthly_rows'] > 0):
        low_coverage_tickers.append(ticker_stats)

# Print summary table
print("=" * 100)
print("SUMMARY BY TICKER")
print("=" * 100)
print(f"{'Ticker':<8} {'Holidays':<30} {'Monthly':<30} {'Overall':<20}")
print(f"{'':8} {'Rows':<8} {'IV%':<8} {'Prob%':<8} {'Rows':<8} {'IV%':<8} {'Prob%':<8} {'IV%':<8} {'Prob%':<8}")
print("-" * 100)

for stat in sorted(results, key=lambda x: x['ticker']):
    h_rows = stat['holidays_rows']
    h_iv_pct = stat['holidays_iv_pct']
    h_prob_pct = stat['holidays_prob_pct']
    m_rows = stat['monthly_rows']
    m_iv_pct = stat['monthly_iv_pct']
    m_prob_pct = stat['monthly_prob_pct']
    
    total_rows = h_rows + m_rows
    total_iv = stat['holidays_iv'] + stat['monthly_iv']
    total_prob = stat['holidays_prob'] + stat['monthly_prob']
    overall_iv_pct = (total_iv / total_rows * 100) if total_rows > 0 else 0
    overall_prob_pct = (total_prob / total_rows * 100) if total_rows > 0 else 0
    
    # Highlight low coverage
    marker = "⚠️ " if overall_iv_pct < 30 or overall_prob_pct < 30 else "  "
    
    print(f"{marker}{stat['ticker']:<6} "
          f"{h_rows:>7,} {h_iv_pct:>6.1f}% {h_prob_pct:>6.1f}%  "
          f"{m_rows:>7,} {m_iv_pct:>6.1f}% {m_prob_pct:>6.1f}%  "
          f"{overall_iv_pct:>6.1f}% {overall_prob_pct:>6.1f}%")

print("-" * 100)
print(f"{'TOTAL':<8} "
      f"{sum(r['holidays_rows'] for r in results):>7,} "
      f"{sum(r['holidays_iv'] for r in results) / sum(r['holidays_rows'] for r in results if r['holidays_rows'] > 0) * 100:>6.1f}% "
      f"{sum(r['holidays_prob'] for r in results) / sum(r['holidays_rows'] for r in results if r['holidays_rows'] > 0) * 100:>6.1f}%  "
      f"{sum(r['monthly_rows'] for r in results):>7,} "
      f"{sum(r['monthly_iv'] for r in results) / sum(r['monthly_rows'] for r in results if r['monthly_rows'] > 0) * 100:>6.1f}% "
      f"{sum(r['monthly_prob'] for r in results) / sum(r['monthly_rows'] for r in results if r['monthly_rows'] > 0) * 100:>6.1f}%  "
      f"{total_iv_all / total_rows_all * 100:>6.1f}% {total_prob_all / total_rows_all * 100:>6.1f}%")

# Print detailed report for low coverage tickers
if low_coverage_tickers:
    print("\n" + "=" * 100)
    print("⚠️  TICKERS WITH LOW COVERAGE (< 30%)")
    print("=" * 100)
    
    for stat in sorted(low_coverage_tickers, key=lambda x: min(x['holidays_iv_pct'] if x['holidays_rows'] > 0 else 100, 
                                                                 x['monthly_iv_pct'] if x['monthly_rows'] > 0 else 100)):
        print(f"\n{stat['ticker']}:")
        if stat['holidays_rows'] > 0:
            print(f"  Holidays: {stat['holidays_iv']:,}/{stat['holidays_rows']:,} IV ({stat['holidays_iv_pct']:.1f}%), "
                  f"{stat['holidays_prob']:,}/{stat['holidays_rows']:,} Prob ({stat['holidays_prob_pct']:.1f}%)")
        if stat['monthly_rows'] > 0:
            print(f"  Monthly:  {stat['monthly_iv']:,}/{stat['monthly_rows']:,} IV ({stat['monthly_iv_pct']:.1f}%), "
                  f"{stat['monthly_prob']:,}/{stat['monthly_rows']:,} Prob ({stat['monthly_prob_pct']:.1f}%)")

# Overall statistics
print("\n" + "=" * 100)
print("OVERALL STATISTICS")
print("=" * 100)
print(f"Total files checked: {total_files:,}")
print(f"Total rows: {total_rows_all:,}")
print(f"Total IV valid: {total_iv_all:,} ({total_iv_all/total_rows_all*100:.1f}%)")
print(f"Total Prob ITM valid: {total_prob_all:,} ({total_prob_all/total_rows_all*100:.1f}%)")
print(f"Tickers with low coverage: {len(low_coverage_tickers)}")
print("=" * 100)


