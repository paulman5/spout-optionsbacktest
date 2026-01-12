#!/usr/bin/env python3
"""
Fix ITM boolean for all tickers based on underlying_spot_at_expiry.
ITM = YES if underlying_spot_at_expiry >= strike
ITM = NO if underlying_spot_at_expiry < strike
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("FIXING ITM BOOLEAN BASED ON EXPIRATION PRICE")
print("=" * 100)
print("ITM = YES if underlying_spot_at_expiry >= strike")
print("ITM = NO if underlying_spot_at_expiry < strike")
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

total_files_processed = 0
total_rows_updated = 0

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    ticker_files = 0
    ticker_rows_updated = 0
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            try:
                df = pd.read_csv(file)
                rows = len(df)
                
                # Check required columns
                if 'strike' not in df.columns:
                    continue
                
                if 'underlying_spot_at_expiry' not in df.columns:
                    continue
                
                # Ensure ITM column exists
                if 'ITM' not in df.columns:
                    df['ITM'] = 'NO'
                
                # Calculate ITM based on underlying_spot_at_expiry
                # ITM = YES if underlying_spot_at_expiry >= strike
                # ITM = NO if underlying_spot_at_expiry < strike
                
                # Only update rows where we have both values
                has_expiry = df['underlying_spot_at_expiry'].notna()
                has_strike = df['strike'].notna()
                can_calculate = has_expiry & has_strike
                
                if can_calculate.sum() == 0:
                    continue
                
                # Calculate new ITM values
                new_itm = (df.loc[can_calculate, 'underlying_spot_at_expiry'] >= df.loc[can_calculate, 'strike'])
                new_itm_values = new_itm.map({True: 'YES', False: 'NO'})
                
                # Compare with existing values
                old_itm = df.loc[can_calculate, 'ITM']
                rows_changed = (new_itm_values != old_itm).sum()
                
                if rows_changed > 0:
                    # Update ITM column
                    df.loc[can_calculate, 'ITM'] = new_itm_values
                    
                    # Save the file
                    df.to_csv(file, index=False)
                    ticker_files += 1
                    ticker_rows_updated += rows_changed
                    total_files_processed += 1
                    total_rows_updated += rows_changed
                
            except Exception as e:
                print(f"  ❌ Error processing {ticker}/{subdir}/{file.name}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    if ticker_rows_updated > 0:
        print(f"✅ {ticker}: {ticker_files} files, {ticker_rows_updated:,} rows updated")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files processed: {total_files_processed:,}")
print(f"Rows updated: {total_rows_updated:,}")
print("=" * 100)

#!/usr/bin/env python3
"""
Fix ITM boolean for all tickers based on underlying_spot_at_expiry.
ITM = YES if underlying_spot_at_expiry >= strike
ITM = NO if underlying_spot_at_expiry < strike
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("FIXING ITM BOOLEAN BASED ON EXPIRATION PRICE")
print("=" * 100)
print("ITM = YES if underlying_spot_at_expiry >= strike")
print("ITM = NO if underlying_spot_at_expiry < strike")
print()

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

total_files_processed = 0
total_rows_updated = 0

for ticker_dir in ticker_dirs:
    ticker = ticker_dir.name
    ticker_files = 0
    ticker_rows_updated = 0
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            try:
                df = pd.read_csv(file)
                rows = len(df)
                
                # Check required columns
                if 'strike' not in df.columns:
                    continue
                
                if 'underlying_spot_at_expiry' not in df.columns:
                    continue
                
                # Ensure ITM column exists
                if 'ITM' not in df.columns:
                    df['ITM'] = 'NO'
                
                # Calculate ITM based on underlying_spot_at_expiry
                # ITM = YES if underlying_spot_at_expiry >= strike
                # ITM = NO if underlying_spot_at_expiry < strike
                
                # Only update rows where we have both values
                has_expiry = df['underlying_spot_at_expiry'].notna()
                has_strike = df['strike'].notna()
                can_calculate = has_expiry & has_strike
                
                if can_calculate.sum() == 0:
                    continue
                
                # Calculate new ITM values
                new_itm = (df.loc[can_calculate, 'underlying_spot_at_expiry'] >= df.loc[can_calculate, 'strike'])
                new_itm_values = new_itm.map({True: 'YES', False: 'NO'})
                
                # Compare with existing values
                old_itm = df.loc[can_calculate, 'ITM']
                rows_changed = (new_itm_values != old_itm).sum()
                
                if rows_changed > 0:
                    # Update ITM column
                    df.loc[can_calculate, 'ITM'] = new_itm_values
                    
                    # Save the file
                    df.to_csv(file, index=False)
                    ticker_files += 1
                    ticker_rows_updated += rows_changed
                    total_files_processed += 1
                    total_rows_updated += rows_changed
                
            except Exception as e:
                print(f"  ❌ Error processing {ticker}/{subdir}/{file.name}: {str(e)}")
                import traceback
                traceback.print_exc()
                continue
    
    if ticker_rows_updated > 0:
        print(f"✅ {ticker}: {ticker_files} files, {ticker_rows_updated:,} rows updated")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files processed: {total_files_processed:,}")
print(f"Rows updated: {total_rows_updated:,}")
print("=" * 100)


