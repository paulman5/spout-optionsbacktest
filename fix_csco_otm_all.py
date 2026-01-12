#!/usr/bin/env python3
"""
Fix OTM percentage for all CSCO files (holidays and monthly).
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

base_dir = Path("python-boilerplate/data/CSCO")
directories = ['holidays', 'monthly']

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR ALL CSCO FILES")
print("=" * 80)
print(f"Formula: ((strike - underlying_spot) / underlying_spot) * 100")
print()

total_rows_fixed = 0
files_processed = 0
files_failed = []

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        print(f"⚠️  Directory not found: {data_dir}")
        continue
    
    files = sorted(data_dir.glob('*_options_pessimistic.csv'))
    print(f"\n{'='*80}")
    print(f"Processing {subdir.upper()} directory ({len(files)} files)")
    print(f"{'='*80}")
    
    for file in files:
        year = file.stem.split('_')[0]
        print(f"\nProcessing {subdir}/{year}...")
        
        try:
            df = pd.read_csv(file)
            original_rows = len(df)
            
            # Check if required columns exist
            if 'strike' not in df.columns or 'underlying_spot' not in df.columns:
                print(f"  ❌ Missing required columns (strike or underlying_spot)")
                files_failed.append((f"{subdir}/{year}", "Missing columns"))
                continue
            
            # Check how many rows have underlying_spot
            has_spot = df['underlying_spot'].notna().sum()
            if has_spot == 0:
                print(f"  ⚠️  No underlying_spot values found, skipping...")
                continue
            
            print(f"  Rows: {original_rows:,} (with spot: {has_spot:,})")
            print(f"  Current OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Calculate correct OTM percentage
            # Formula: ((strike - underlying_spot) / underlying_spot) * 100
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
            
            # Round to 2 decimal places
            df['otm_pct'] = df['otm_pct'].round(2)
            
            print(f"  New OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Show some statistics
            updated_rows = df[df['underlying_spot'].notna() & df['otm_pct'].notna()]
            if len(updated_rows) > 0:
                itm_count = (updated_rows['otm_pct'] < 0).sum()
                otm_count = (updated_rows['otm_pct'] >= 0).sum()
                print(f"  ITM (OTM% < 0): {itm_count:,}, OTM (OTM% >= 0): {otm_count:,}")
            
            # Save the file
            df.to_csv(file, index=False)
            print(f"  ✅ Fixed and saved")
            total_rows_fixed += original_rows
            files_processed += 1
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            files_failed.append((f"{subdir}/{year}", str(e)))
            import traceback
            traceback.print_exc()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Successfully fixed: {files_processed} files")
print(f"Total rows fixed: {total_rows_fixed:,}")
if files_failed:
    print(f"❌ Failed: {len(files_failed)} files")
    for file_path, error in files_failed:
        print(f"   {file_path}: {error}")

#!/usr/bin/env python3
"""
Fix OTM percentage for all CSCO files (holidays and monthly).
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

base_dir = Path("python-boilerplate/data/CSCO")
directories = ['holidays', 'monthly']

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR ALL CSCO FILES")
print("=" * 80)
print(f"Formula: ((strike - underlying_spot) / underlying_spot) * 100")
print()

total_rows_fixed = 0
files_processed = 0
files_failed = []

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        print(f"⚠️  Directory not found: {data_dir}")
        continue
    
    files = sorted(data_dir.glob('*_options_pessimistic.csv'))
    print(f"\n{'='*80}")
    print(f"Processing {subdir.upper()} directory ({len(files)} files)")
    print(f"{'='*80}")
    
    for file in files:
        year = file.stem.split('_')[0]
        print(f"\nProcessing {subdir}/{year}...")
        
        try:
            df = pd.read_csv(file)
            original_rows = len(df)
            
            # Check if required columns exist
            if 'strike' not in df.columns or 'underlying_spot' not in df.columns:
                print(f"  ❌ Missing required columns (strike or underlying_spot)")
                files_failed.append((f"{subdir}/{year}", "Missing columns"))
                continue
            
            # Check how many rows have underlying_spot
            has_spot = df['underlying_spot'].notna().sum()
            if has_spot == 0:
                print(f"  ⚠️  No underlying_spot values found, skipping...")
                continue
            
            print(f"  Rows: {original_rows:,} (with spot: {has_spot:,})")
            print(f"  Current OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Calculate correct OTM percentage
            # Formula: ((strike - underlying_spot) / underlying_spot) * 100
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
            
            # Round to 2 decimal places
            df['otm_pct'] = df['otm_pct'].round(2)
            
            print(f"  New OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Show some statistics
            updated_rows = df[df['underlying_spot'].notna() & df['otm_pct'].notna()]
            if len(updated_rows) > 0:
                itm_count = (updated_rows['otm_pct'] < 0).sum()
                otm_count = (updated_rows['otm_pct'] >= 0).sum()
                print(f"  ITM (OTM% < 0): {itm_count:,}, OTM (OTM% >= 0): {otm_count:,}")
            
            # Save the file
            df.to_csv(file, index=False)
            print(f"  ✅ Fixed and saved")
            total_rows_fixed += original_rows
            files_processed += 1
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            files_failed.append((f"{subdir}/{year}", str(e)))
            import traceback
            traceback.print_exc()

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Successfully fixed: {files_processed} files")
print(f"Total rows fixed: {total_rows_fixed:,}")
if files_failed:
    print(f"❌ Failed: {len(files_failed)} files")
    for file_path, error in files_failed:
        print(f"   {file_path}: {error}")


