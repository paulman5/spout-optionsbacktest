#!/usr/bin/env python3
"""
Fix ITM boolean for all tickers in all files.
For call options: ITM = YES if underlying_close >= strike
For put options: ITM = YES if underlying_close <= strike
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("FIXING ITM BOOLEAN FOR ALL TICKERS")
print("=" * 100)
print("Call options: ITM = YES if underlying_close >= strike")
print("Put options: ITM = YES if underlying_close <= strike")
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
                
                # Determine which underlying price column to use
                underlying_price_col = None
                if 'underlying_close' in df.columns:
                    underlying_price_col = 'underlying_close'
                elif 'underlying_spot' in df.columns:
                    underlying_price_col = 'underlying_spot'
                else:
                    continue
                
                # Check if option_type column exists
                if 'option_type' not in df.columns:
                    # Try to infer from ticker or assume all are calls
                    df['option_type'] = 'C'  # Default to call
                
                # Ensure ITM column exists
                if 'ITM' not in df.columns:
                    df['ITM'] = 'NO'
                
                # Calculate ITM based on option type
                rows_updated = 0
                
                # For call options: ITM = YES if underlying_close >= strike
                call_mask = df['option_type'] == 'C'
                if call_mask.any():
                    call_itm = (df.loc[call_mask, underlying_price_col] >= df.loc[call_mask, 'strike'])
                    old_call_itm = df.loc[call_mask, 'ITM'] == 'YES'
                    df.loc[call_mask, 'ITM'] = call_itm.map({True: 'YES', False: 'NO'})
                    rows_updated += (call_itm != old_call_itm).sum()
                
                # For put options: ITM = YES if underlying_close <= strike
                put_mask = df['option_type'] == 'P'
                if put_mask.any():
                    put_itm = (df.loc[put_mask, underlying_price_col] <= df.loc[put_mask, 'strike'])
                    old_put_itm = df.loc[put_mask, 'ITM'] == 'YES'
                    df.loc[put_mask, 'ITM'] = put_itm.map({True: 'YES', False: 'NO'})
                    rows_updated += (put_itm != old_put_itm).sum()
                
                # Only save if we made changes
                if rows_updated > 0:
                    df.to_csv(file, index=False)
                    ticker_files += 1
                    ticker_rows_updated += rows_updated
                    total_files_processed += 1
                    total_rows_updated += rows_updated
                
            except Exception as e:
                print(f"  ❌ Error processing {ticker}/{subdir}/{file.name}: {str(e)}")
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
Fix ITM boolean for all tickers in all files.
For call options: ITM = YES if underlying_close >= strike
For put options: ITM = YES if underlying_close <= strike
"""

import pandas as pd
import numpy as np
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("FIXING ITM BOOLEAN FOR ALL TICKERS")
print("=" * 100)
print("Call options: ITM = YES if underlying_close >= strike")
print("Put options: ITM = YES if underlying_close <= strike")
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
                
                # Determine which underlying price column to use
                underlying_price_col = None
                if 'underlying_close' in df.columns:
                    underlying_price_col = 'underlying_close'
                elif 'underlying_spot' in df.columns:
                    underlying_price_col = 'underlying_spot'
                else:
                    continue
                
                # Check if option_type column exists
                if 'option_type' not in df.columns:
                    # Try to infer from ticker or assume all are calls
                    df['option_type'] = 'C'  # Default to call
                
                # Ensure ITM column exists
                if 'ITM' not in df.columns:
                    df['ITM'] = 'NO'
                
                # Calculate ITM based on option type
                rows_updated = 0
                
                # For call options: ITM = YES if underlying_close >= strike
                call_mask = df['option_type'] == 'C'
                if call_mask.any():
                    call_itm = (df.loc[call_mask, underlying_price_col] >= df.loc[call_mask, 'strike'])
                    old_call_itm = df.loc[call_mask, 'ITM'] == 'YES'
                    df.loc[call_mask, 'ITM'] = call_itm.map({True: 'YES', False: 'NO'})
                    rows_updated += (call_itm != old_call_itm).sum()
                
                # For put options: ITM = YES if underlying_close <= strike
                put_mask = df['option_type'] == 'P'
                if put_mask.any():
                    put_itm = (df.loc[put_mask, underlying_price_col] <= df.loc[put_mask, 'strike'])
                    old_put_itm = df.loc[put_mask, 'ITM'] == 'YES'
                    df.loc[put_mask, 'ITM'] = put_itm.map({True: 'YES', False: 'NO'})
                    rows_updated += (put_itm != old_put_itm).sum()
                
                # Only save if we made changes
                if rows_updated > 0:
                    df.to_csv(file, index=False)
                    ticker_files += 1
                    ticker_rows_updated += rows_updated
                    total_files_processed += 1
                    total_rows_updated += rows_updated
                
            except Exception as e:
                print(f"  ❌ Error processing {ticker}/{subdir}/{file.name}: {str(e)}")
                continue
    
    if ticker_rows_updated > 0:
        print(f"✅ {ticker}: {ticker_files} files, {ticker_rows_updated:,} rows updated")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files processed: {total_files_processed:,}")
print(f"Rows updated: {total_rows_updated:,}")
print("=" * 100)


