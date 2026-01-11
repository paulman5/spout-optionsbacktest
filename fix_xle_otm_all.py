#!/usr/bin/env python3
"""
Fix OTM percentage for all XLE holiday files.
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

data_dir = Path('python-boilerplate/data/XLE/holidays')
files = sorted(data_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR ALL XLE HOLIDAY FILES")
print("=" * 80)
print(f"Formula: ((strike - underlying_spot) / underlying_spot) * 100")
print()

total_rows_fixed = 0
files_processed = 0
files_failed = []

for file in files:
    year = file.stem.split('_')[0]
    print(f"Processing {year}...")
    
    try:
        df = pd.read_csv(file)
        original_rows = len(df)
        
        # Check if required columns exist
        if 'strike' not in df.columns or 'underlying_spot' not in df.columns:
            print(f"  ❌ Missing required columns (strike or underlying_spot)")
            files_failed.append((year, "Missing columns"))
            continue
        
        print(f"  Rows: {original_rows:,}")
        print(f"  Current OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
        
        # Calculate correct OTM percentage
        df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
        
        # Round to 2 decimal places
        df['otm_pct'] = df['otm_pct'].round(2)
        
        print(f"  New OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
        
        # Save the file
        df.to_csv(file, index=False)
        print(f"  ✅ Fixed and saved")
        total_rows_fixed += original_rows
        files_processed += 1
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        files_failed.append((year, str(e)))
        import traceback
        traceback.print_exc()
    
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Successfully fixed: {files_processed} files")
print(f"Total rows fixed: {total_rows_fixed:,}")
if files_failed:
    print(f"❌ Failed: {len(files_failed)} files")
    for year, error in files_failed:
        print(f"   {year}: {error}")

