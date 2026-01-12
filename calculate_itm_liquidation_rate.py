#!/usr/bin/env python3
"""
Calculate the percentage of times per year per ticker that options expire ITM
(borrower gets liquidated/assigned).
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("CALCULATING ITM LIQUIDATION RATE PER TICKER")
print("=" * 100)
print("Percentage of options that expire ITM (borrower gets assigned)")
print()

ticker_stats = defaultdict(lambda: {
    'total_options': 0,
    'itm_options': 0,
    'by_year': defaultdict(lambda: {'total': 0, 'itm': 0})
})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        for file in sorted(data_dir.glob('*_options_pessimistic.csv')):
            try:
                df = pd.read_csv(file)
                
                if 'ITM' not in df.columns:
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
                
                # Count total options and ITM options
                total = len(df)
                itm_count = (df['ITM'] == 'YES').sum()
                
                ticker_stats[ticker]['total_options'] += total
                ticker_stats[ticker]['itm_options'] += itm_count
                
                # Count by year
                for year, group in df.groupby('year'):
                    year_total = len(group)
                    year_itm = (group['ITM'] == 'YES').sum()
                    ticker_stats[ticker]['by_year'][year]['total'] += year_total
                    ticker_stats[ticker]['by_year'][year]['itm'] += year_itm
                
            except Exception as e:
                continue

# Calculate percentages
print("=" * 100)
print("ITM LIQUIDATION RATE BY TICKER")
print("=" * 100)
print(f"{'Ticker':<8} {'Total Options':<15} {'ITM Options':<15} {'ITM Rate':<12} {'Years':<10} {'Avg per Year':<15}")
print("-" * 100)

ticker_itm_rates = {}
ticker_avg_yearly_rates = {}

for ticker in sorted(ticker_stats.keys()):
    stats = ticker_stats[ticker]
    total = stats['total_options']
    itm = stats['itm_options']
    
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
    
    print(f"{ticker:<8} {total:>14,} {itm:>14,} {overall_rate:>11.2f}% {num_years:>9} {avg_yearly_rate:>14.2f}%")

print("-" * 100)

# Overall statistics
total_all = sum(s['total_options'] for s in ticker_stats.values())
itm_all = sum(s['itm_options'] for s in ticker_stats.values())
overall_rate_all = (itm_all / total_all) * 100 if total_all > 0 else 0

print(f"{'TOTAL':<8} {total_all:>14,} {itm_all:>14,} {overall_rate_all:>11.2f}%")

# Print as lists
print("\n" + "=" * 100)
print("ITM LIQUIDATION RATES AS LISTS")
print("=" * 100)

print("\nOverall ITM Rate per Ticker (%):")
print([round(ticker_itm_rates[t], 2) for t in sorted(ticker_itm_rates.keys())])

print("\nAverage Yearly ITM Rate per Ticker (%):")
print([round(ticker_avg_yearly_rates[t], 2) for t in sorted(ticker_avg_yearly_rates.keys())])

print("\nTicker Names (alphabetical):")
print(sorted(ticker_itm_rates.keys()))

print("\n" + "=" * 100)

#!/usr/bin/env python3
"""
Calculate the percentage of times per year per ticker that options expire ITM
(borrower gets liquidated/assigned).
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("CALCULATING ITM LIQUIDATION RATE PER TICKER")
print("=" * 100)
print("Percentage of options that expire ITM (borrower gets assigned)")
print()

ticker_stats = defaultdict(lambda: {
    'total_options': 0,
    'itm_options': 0,
    'by_year': defaultdict(lambda: {'total': 0, 'itm': 0})
})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        for file in sorted(data_dir.glob('*_options_pessimistic.csv')):
            try:
                df = pd.read_csv(file)
                
                if 'ITM' not in df.columns:
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
                
                # Count total options and ITM options
                total = len(df)
                itm_count = (df['ITM'] == 'YES').sum()
                
                ticker_stats[ticker]['total_options'] += total
                ticker_stats[ticker]['itm_options'] += itm_count
                
                # Count by year
                for year, group in df.groupby('year'):
                    year_total = len(group)
                    year_itm = (group['ITM'] == 'YES').sum()
                    ticker_stats[ticker]['by_year'][year]['total'] += year_total
                    ticker_stats[ticker]['by_year'][year]['itm'] += year_itm
                
            except Exception as e:
                continue

# Calculate percentages
print("=" * 100)
print("ITM LIQUIDATION RATE BY TICKER")
print("=" * 100)
print(f"{'Ticker':<8} {'Total Options':<15} {'ITM Options':<15} {'ITM Rate':<12} {'Years':<10} {'Avg per Year':<15}")
print("-" * 100)

ticker_itm_rates = {}
ticker_avg_yearly_rates = {}

for ticker in sorted(ticker_stats.keys()):
    stats = ticker_stats[ticker]
    total = stats['total_options']
    itm = stats['itm_options']
    
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
    
    print(f"{ticker:<8} {total:>14,} {itm:>14,} {overall_rate:>11.2f}% {num_years:>9} {avg_yearly_rate:>14.2f}%")

print("-" * 100)

# Overall statistics
total_all = sum(s['total_options'] for s in ticker_stats.values())
itm_all = sum(s['itm_options'] for s in ticker_stats.values())
overall_rate_all = (itm_all / total_all) * 100 if total_all > 0 else 0

print(f"{'TOTAL':<8} {total_all:>14,} {itm_all:>14,} {overall_rate_all:>11.2f}%")

# Print as lists
print("\n" + "=" * 100)
print("ITM LIQUIDATION RATES AS LISTS")
print("=" * 100)

print("\nOverall ITM Rate per Ticker (%):")
print([round(ticker_itm_rates[t], 2) for t in sorted(ticker_itm_rates.keys())])

print("\nAverage Yearly ITM Rate per Ticker (%):")
print([round(ticker_avg_yearly_rates[t], 2) for t in sorted(ticker_avg_yearly_rates.keys())])

print("\nTicker Names (alphabetical):")
print(sorted(ticker_itm_rates.keys()))

print("\n" + "=" * 100)

