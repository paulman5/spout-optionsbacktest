#!/usr/bin/env python3
"""
Fix OTM percentage for all PLTR monthly files.
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

base_dir = Path("python-boilerplate/data/PLTR/monthly")
files = sorted(base_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR ALL PLTR MONTHLY FILES")
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
        
        # Check how many rows have underlying_spot
        has_spot = df['underlying_spot'].notna().sum()
        if has_spot == 0:
            print(f"  ⚠️  No underlying_spot values found, skipping...")
            continue
        
        print(f"  Rows: {original_rows:,} (with spot: {has_spot:,})")
        
        # Show current OTM range
        if 'otm_pct' in df.columns:
            current_min = df['otm_pct'].min() if df['otm_pct'].notna().any() else None
            current_max = df['otm_pct'].max() if df['otm_pct'].notna().any() else None
            if current_min is not None:
                print(f"  Current OTM range: {current_min:.2f}% to {current_max:.2f}%")
        
        # Calculate correct OTM percentage
        # Formula: ((strike - underlying_spot) / underlying_spot) * 100
        df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
        
        # Round to 2 decimal places
        df['otm_pct'] = df['otm_pct'].round(2)
        
        # Show new OTM range
        new_min = df['otm_pct'].min() if df['otm_pct'].notna().any() else None
        new_max = df['otm_pct'].max() if df['otm_pct'].notna().any() else None
        if new_min is not None:
            print(f"  New OTM range: {new_min:.2f}% to {new_max:.2f}%")
        
        # Show some statistics
        updated_rows = df[df['underlying_spot'].notna() & df['otm_pct'].notna()]
        if len(updated_rows) > 0:
            itm_count = (updated_rows['otm_pct'] < 0).sum()
            otm_count = (updated_rows['otm_pct'] >= 0).sum()
            print(f"  ITM (OTM% < 0): {itm_count:,}")
            print(f"  OTM (OTM% >= 0): {otm_count:,}")
        
        # Save the file
        df.to_csv(file, index=False)
        total_rows_fixed += len(updated_rows)
        files_processed += 1
        print(f"  ✅ Fixed and saved")
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        files_failed.append((year, str(e)))
        continue

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Files processed: {files_processed}")
print(f"Rows fixed: {total_rows_fixed:,}")
if files_failed:
    print(f"\nFiles that failed:")
    for year, error in files_failed:
        print(f"  {year}: {error}")
else:
    print("\n✅ All files processed successfully!")
