#!/usr/bin/env python3
"""
Calculate average APY graphs for weekly and monthly options.
Logic:
- High IV → use weekly (holidays) with 5-8% probability_itm
- Low IV → use monthly with 10-15% probability_itm
- Filter by probability_itm ranges
- Average APY per year across all tickers
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("CALCULATING AVERAGE APY GRAPHS FOR WEEKLY AND MONTHLY OPTIONS")
print("=" * 100)
print()

# Determine IV threshold (median IV across all tickers)
print("Step 1: Calculating IV threshold...")
all_ivs = []

for ticker_dir in base_dir.iterdir():
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        for file in data_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    all_ivs.extend(ivs.tolist())
            except:
                continue

if all_ivs:
    iv_threshold = np.median(all_ivs)
    print(f"IV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
    print(f"  High IV: >= {iv_threshold:.4f}")
    print(f"  Low IV: < {iv_threshold:.4f}")
else:
    iv_threshold = 0.30  # Default threshold
    print(f"Using default IV threshold: {iv_threshold:.4f}")

print()

# Collect data for each ticker
print("Step 2: Processing tickers and determining monthly IV level...")
ticker_data = defaultdict(lambda: {'monthly_iv_level': None, 'weekly_data': [], 'monthly_data': []})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    # Calculate average IV for MONTHLY files only
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
        ticker_data[ticker]['monthly_iv_level'] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
        print(f"  {ticker}: monthly avg IV = {avg_monthly_iv:.4f} ({avg_monthly_iv*100:.2f}%) → {'HIGH' if avg_monthly_iv >= iv_threshold else 'LOW'}")
    else:
        ticker_data[ticker]['monthly_iv_level'] = 'low'  # Default to low
        print(f"  {ticker}: No monthly IV data → LOW (default)")

print()

# Process files and collect data
print("Step 3: Processing files and filtering by probability_itm...")
total_weekly_rows = 0
total_monthly_rows = 0

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_data[ticker]['monthly_iv_level']
    
    # Process holidays (weekly) - only if monthly IV is HIGH
    # If monthly IV is HIGH → use weekly with 4-7% probability ITM
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    # Extract year from filename or date_only
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        # Try to extract from filename
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by probability_itm: 4-7% for weekly when monthly IV is high
                    if 'probability_itm' in df.columns:
                        filtered = df[(df['probability_itm'] >= 0.04) & (df['probability_itm'] <= 0.07) & (df['probability_itm'].notna())]
                        
                        # Get APY (premium_yield_pct - NOT annualized)
                        if 'premium_yield_pct' in filtered.columns:
                            apy_col = 'premium_yield_pct'
                        elif 'premium' in filtered.columns and 'underlying_spot' in filtered.columns:
                            # Calculate: (premium / underlying_spot) * 100
                            filtered['premium_yield_pct'] = (filtered['premium'] / filtered['underlying_spot']) * 100
                            apy_col = 'premium_yield_pct'
                        else:
                            continue
                        
                        # Only process if we have valid data
                        if len(filtered) == 0:
                            continue
                        
                        total_weekly_rows += len(filtered)
                        
                        # Group by year and calculate average (use premium_yield_pct as-is, not annualized)
                        for year, group in filtered.groupby('year'):
                            valid_apy = group[apy_col].dropna()
                            if len(valid_apy) > 0:
                                avg_apy = valid_apy.mean()
                                if pd.notna(avg_apy) and avg_apy > 0:
                                    ticker_data[ticker]['weekly_data'].append({
                                        'year': year,
                                        'apy': avg_apy
                                    })
                except Exception as e:
                    continue
    
    # Process monthly - only if monthly IV is LOW
    # If monthly IV is LOW → use monthly with 10-15% probability ITM
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
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
                    if 'probability_itm' in df.columns:
                        filtered = df[(df['probability_itm'] >= 0.10) & (df['probability_itm'] <= 0.15) & (df['probability_itm'].notna())]
                        
                        # Get APY (premium_yield_pct - NOT annualized)
                        if 'premium_yield_pct' in filtered.columns:
                            apy_col = 'premium_yield_pct'
                        elif 'premium' in filtered.columns and 'underlying_spot' in filtered.columns:
                            filtered['premium_yield_pct'] = (filtered['premium'] / filtered['underlying_spot']) * 100
                            apy_col = 'premium_yield_pct'
                        else:
                            continue
                        
                        # Only process if we have valid data
                        if len(filtered) == 0:
                            continue
                        
                        total_monthly_rows += len(filtered)
                        
                        # Group by year (use premium_yield_pct as-is, not annualized)
                        for year, group in filtered.groupby('year'):
                            valid_apy = group[apy_col].dropna()
                            if len(valid_apy) > 0:
                                avg_apy = valid_apy.mean()
                                if pd.notna(avg_apy) and avg_apy > 0:
                                    ticker_data[ticker]['monthly_data'].append({
                                        'year': year,
                                        'apy': avg_apy
                                    })
                except Exception as e:
                    continue

print(f"\nTotal rows processed: {total_weekly_rows:,} weekly, {total_monthly_rows:,} monthly")
print()

# Aggregate by year across all tickers
print("Step 4: Aggregating data by year...")

weekly_by_year = defaultdict(list)
monthly_by_year = defaultdict(list)

for ticker, data in ticker_data.items():
    for entry in data['weekly_data']:
        weekly_by_year[entry['year']].append(entry['apy'])
    for entry in data['monthly_data']:
        monthly_by_year[entry['year']].append(entry['apy'])

# Calculate averages
weekly_avg = {year: np.mean(apys) for year, apys in weekly_by_year.items() if apys}
monthly_avg = {year: np.mean(apys) for year, apys in monthly_by_year.items() if apys}

print(f"Weekly data: {len(weekly_avg)} years")
print(f"Monthly data: {len(monthly_avg)} years")
print()

# Create graphs
print("Step 5: Creating graphs...")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Weekly graph
if weekly_avg:
    years_weekly = sorted(weekly_avg.keys())
    apys_weekly = [weekly_avg[y] for y in years_weekly]
    
    ax1.plot(years_weekly, apys_weekly, marker='o', linewidth=2, markersize=8)
    ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Weekly APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Average Weekly APY Across All Tickers\n(High Monthly IV → Weekly with 4-7% Prob ITM)', 
                  fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(years_weekly)
    ax1.set_ylim(0, 2)  # Set Y-axis range to 0-2%
    ax1.tick_params(axis='x', rotation=45)

# Monthly graph
if monthly_avg:
    years_monthly = sorted(monthly_avg.keys())
    apys_monthly = [monthly_avg[y] for y in years_monthly]
    
    ax2.plot(years_monthly, apys_monthly, marker='s', linewidth=2, markersize=8, color='orange')
    ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Average Monthly APY (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Average Monthly APY Across All Tickers\n(Low Monthly IV → Monthly with 10-15% Prob ITM)', 
                  fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(years_monthly)
    ax2.set_ylim(0, 2)  # Set Y-axis range to 0-2%
    ax2.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('average_apy_graphs.png', dpi=300, bbox_inches='tight')
print("✅ Graphs saved to: average_apy_graphs.png")

# Print summary statistics
print("\n" + "=" * 100)
print("SUMMARY STATISTICS")
print("=" * 100)

if weekly_avg:
    print("\nWeekly APY by Year:")
    for year in sorted(weekly_avg.keys()):
        print(f"  {year}: {weekly_avg[year]:.2f}% (from {len(weekly_by_year[year])} tickers)")

if monthly_avg:
    print("\nMonthly APY by Year:")
    for year in sorted(monthly_avg.keys()):
        print(f"  {year}: {monthly_avg[year]:.2f}% (from {len(monthly_by_year[year])} tickers)")

print("\n" + "=" * 100)


Calculate average APY graphs for weekly and monthly options.
Logic:
- High IV → use weekly (holidays) with 5-8% probability_itm
- Low IV → use monthly with 10-15% probability_itm
- Filter by probability_itm ranges
- Average APY per year across all tickers
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("CALCULATING AVERAGE APY GRAPHS FOR WEEKLY AND MONTHLY OPTIONS")
print("=" * 100)
print()

# Determine IV threshold (median IV across all tickers)
print("Step 1: Calculating IV threshold...")
all_ivs = []

for ticker_dir in base_dir.iterdir():
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        for file in data_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    all_ivs.extend(ivs.tolist())
            except:
                continue

if all_ivs:
    iv_threshold = np.median(all_ivs)
    print(f"IV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
    print(f"  High IV: >= {iv_threshold:.4f}")
    print(f"  Low IV: < {iv_threshold:.4f}")
else:
    iv_threshold = 0.30  # Default threshold
    print(f"Using default IV threshold: {iv_threshold:.4f}")

print()

# Collect data for each ticker
print("Step 2: Processing tickers and determining monthly IV level...")
ticker_data = defaultdict(lambda: {'monthly_iv_level': None, 'weekly_data': [], 'monthly_data': []})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    # Calculate average IV for MONTHLY files only
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
        ticker_data[ticker]['monthly_iv_level'] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
        print(f"  {ticker}: monthly avg IV = {avg_monthly_iv:.4f} ({avg_monthly_iv*100:.2f}%) → {'HIGH' if avg_monthly_iv >= iv_threshold else 'LOW'}")
    else:
        ticker_data[ticker]['monthly_iv_level'] = 'low'  # Default to low
        print(f"  {ticker}: No monthly IV data → LOW (default)")

print()

# Process files and collect data
print("Step 3: Processing files and filtering by probability_itm...")
total_weekly_rows = 0
total_monthly_rows = 0

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_data[ticker]['monthly_iv_level']
    
    # Process holidays (weekly) - only if monthly IV is HIGH
    # If monthly IV is HIGH → use weekly with 4-7% probability ITM
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    # Extract year from filename or date_only
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        # Try to extract from filename
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by probability_itm: 4-7% for weekly when monthly IV is high
                    if 'probability_itm' in df.columns:
                        filtered = df[(df['probability_itm'] >= 0.04) & (df['probability_itm'] <= 0.07) & (df['probability_itm'].notna())]
                        
                        # Get APY (premium_yield_pct - NOT annualized)
                        if 'premium_yield_pct' in filtered.columns:
                            apy_col = 'premium_yield_pct'
                        elif 'premium' in filtered.columns and 'underlying_spot' in filtered.columns:
                            # Calculate: (premium / underlying_spot) * 100
                            filtered['premium_yield_pct'] = (filtered['premium'] / filtered['underlying_spot']) * 100
                            apy_col = 'premium_yield_pct'
                        else:
                            continue
                        
                        # Only process if we have valid data
                        if len(filtered) == 0:
                            continue
                        
                        total_weekly_rows += len(filtered)
                        
                        # Group by year and calculate average (use premium_yield_pct as-is, not annualized)
                        for year, group in filtered.groupby('year'):
                            valid_apy = group[apy_col].dropna()
                            if len(valid_apy) > 0:
                                avg_apy = valid_apy.mean()
                                if pd.notna(avg_apy) and avg_apy > 0:
                                    ticker_data[ticker]['weekly_data'].append({
                                        'year': year,
                                        'apy': avg_apy
                                    })
                except Exception as e:
                    continue
    
    # Process monthly - only if monthly IV is LOW
    # If monthly IV is LOW → use monthly with 10-15% probability ITM
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
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
                    if 'probability_itm' in df.columns:
                        filtered = df[(df['probability_itm'] >= 0.10) & (df['probability_itm'] <= 0.15) & (df['probability_itm'].notna())]
                        
                        # Get APY (premium_yield_pct - NOT annualized)
                        if 'premium_yield_pct' in filtered.columns:
                            apy_col = 'premium_yield_pct'
                        elif 'premium' in filtered.columns and 'underlying_spot' in filtered.columns:
                            filtered['premium_yield_pct'] = (filtered['premium'] / filtered['underlying_spot']) * 100
                            apy_col = 'premium_yield_pct'
                        else:
                            continue
                        
                        # Only process if we have valid data
                        if len(filtered) == 0:
                            continue
                        
                        total_monthly_rows += len(filtered)
                        
                        # Group by year (use premium_yield_pct as-is, not annualized)
                        for year, group in filtered.groupby('year'):
                            valid_apy = group[apy_col].dropna()
                            if len(valid_apy) > 0:
                                avg_apy = valid_apy.mean()
                                if pd.notna(avg_apy) and avg_apy > 0:
                                    ticker_data[ticker]['monthly_data'].append({
                                        'year': year,
                                        'apy': avg_apy
                                    })
                except Exception as e:
                    continue

print(f"\nTotal rows processed: {total_weekly_rows:,} weekly, {total_monthly_rows:,} monthly")
print()

# Aggregate by year across all tickers
print("Step 4: Aggregating data by year...")

weekly_by_year = defaultdict(list)
monthly_by_year = defaultdict(list)

for ticker, data in ticker_data.items():
    for entry in data['weekly_data']:
        weekly_by_year[entry['year']].append(entry['apy'])
    for entry in data['monthly_data']:
        monthly_by_year[entry['year']].append(entry['apy'])

# Calculate averages
weekly_avg = {year: np.mean(apys) for year, apys in weekly_by_year.items() if apys}
monthly_avg = {year: np.mean(apys) for year, apys in monthly_by_year.items() if apys}

print(f"Weekly data: {len(weekly_avg)} years")
print(f"Monthly data: {len(monthly_avg)} years")
print()

# Create graphs
print("Step 5: Creating graphs...")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Weekly graph
if weekly_avg:
    years_weekly = sorted(weekly_avg.keys())
    apys_weekly = [weekly_avg[y] for y in years_weekly]
    
    ax1.plot(years_weekly, apys_weekly, marker='o', linewidth=2, markersize=8)
    ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Weekly APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Average Weekly APY Across All Tickers\n(High Monthly IV → Weekly with 4-7% Prob ITM)', 
                  fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(years_weekly)
    ax1.set_ylim(0, 2)  # Set Y-axis range to 0-2%
    ax1.tick_params(axis='x', rotation=45)

# Monthly graph
if monthly_avg:
    years_monthly = sorted(monthly_avg.keys())
    apys_monthly = [monthly_avg[y] for y in years_monthly]
    
    ax2.plot(years_monthly, apys_monthly, marker='s', linewidth=2, markersize=8, color='orange')
    ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Average Monthly APY (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Average Monthly APY Across All Tickers\n(Low Monthly IV → Monthly with 10-15% Prob ITM)', 
                  fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(years_monthly)
    ax2.set_ylim(0, 2)  # Set Y-axis range to 0-2%
    ax2.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig('average_apy_graphs.png', dpi=300, bbox_inches='tight')
print("✅ Graphs saved to: average_apy_graphs.png")

# Print summary statistics
print("\n" + "=" * 100)
print("SUMMARY STATISTICS")
print("=" * 100)

if weekly_avg:
    print("\nWeekly APY by Year:")
    for year in sorted(weekly_avg.keys()):
        print(f"  {year}: {weekly_avg[year]:.2f}% (from {len(weekly_by_year[year])} tickers)")

if monthly_avg:
    print("\nMonthly APY by Year:")
    for year in sorted(monthly_avg.keys()):
        print(f"  {year}: {monthly_avg[year]:.2f}% (from {len(monthly_by_year[year])} tickers)")

print("\n" + "=" * 100)
