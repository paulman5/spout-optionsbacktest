#!/usr/bin/env python3
"""
Fix SPY strike prices: divide by 2 for all dates before 2017-01-25
(SPY had a 2-for-1 stock split on January 25, 2017)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

SPLIT_DATE = pd.to_datetime('2017-01-25')
SPLIT_RATIO = 2

data_dir = Path('python-boilerplate/data/SPY/holidays')
files = sorted(data_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("FIXING SPY STRIKE PRICES FOR PRE-SPLIT DATES")
print("=" * 80)
print(f"Split date: {SPLIT_DATE.date()}")
print(f"Split ratio: 1:{SPLIT_RATIO} (divide strikes by {SPLIT_RATIO})")
print()

total_rows_fixed = 0
files_processed = 0

for file in files:
    year = file.stem.split('_')[0]
    print(f"Processing {year}...")
    
    try:
        df = pd.read_csv(file)
        original_rows = len(df)
        
        # Convert date_only to datetime
        df['date_only'] = pd.to_datetime(df['date_only'])
        
        # Find rows before split date
        before_split = df['date_only'] < SPLIT_DATE
        rows_to_fix = before_split.sum()
        
        if rows_to_fix > 0:
            print(f"  Rows before split date: {rows_to_fix:,} / {original_rows:,}")
            print(f"  Current strike range: {df['strike'].min()} to {df['strike'].max()}")
            
            # Divide strikes by 2 for pre-split dates
            df.loc[before_split, 'strike'] = df.loc[before_split, 'strike'] / SPLIT_RATIO
            
            print(f"  New strike range: {df['strike'].min()} to {df['strike'].max()}")
            
            # Save the file
            df.to_csv(file, index=False)
            print(f"  ✅ Fixed and saved")
            total_rows_fixed += rows_to_fix
            files_processed += 1
        else:
            print(f"  ✅ No rows before split date - no changes needed")
            
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Files processed: {files_processed}")
print(f"Total rows fixed: {total_rows_fixed:,}")

