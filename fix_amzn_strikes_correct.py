#!/usr/bin/env python3
"""
Fix AMZN strike prices correctly:
1. Extract strike from ticker (e.g., AMZN190111C01350000 -> 135000 / 1000 = 1350)
2. For dates before 2022-06-06, divide by 20 (1350 / 20 = 67.5)
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

SPLIT_DATE = pd.to_datetime('2022-06-06')
SPLIT_RATIO = 20

def extract_strike_from_ticker(ticker):
    """Extract strike from ticker format: O:AMZN190111C01350000"""
    ticker_clean = ticker.replace('O:', '')
    match = re.search(r'[CP](\d+)', ticker_clean)
    if match:
        strike_raw = int(match.group(1))
        return strike_raw / 1000.0
    return None

data_dir = Path('python-boilerplate/data/AMZN/holidays')
files = sorted(data_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("FIXING AMZN STRIKE PRICES CORRECTLY")
print("=" * 80)
print(f"Split date: {SPLIT_DATE.date()}")
print(f"Process: Extract from ticker (div 1000) -> then div {SPLIT_RATIO} for pre-split dates")
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
        
        # Extract strike from ticker for all rows
        print(f"  Extracting strikes from tickers...")
        df['strike_from_ticker'] = df['ticker'].apply(extract_strike_from_ticker)
        
        # Check if any are None
        if df['strike_from_ticker'].isna().any():
            print(f"  ⚠️  Warning: Some strikes could not be extracted")
        
        # Find rows before split date
        before_split = df['date_only'] < SPLIT_DATE
        rows_to_fix = before_split.sum()
        
        if rows_to_fix > 0:
            print(f"  Rows before split date: {rows_to_fix:,} / {original_rows:,}")
            print(f"  Original strike range: {df['strike'].min()} to {df['strike'].max()}")
            
            # For pre-split dates: extract from ticker, divide by 1000, then divide by 20
            df.loc[before_split, 'strike'] = df.loc[before_split, 'strike_from_ticker'] / SPLIT_RATIO
            
            # For post-split dates: just extract from ticker, divide by 1000
            after_split = df['date_only'] >= SPLIT_DATE
            if after_split.sum() > 0:
                df.loc[after_split, 'strike'] = df.loc[after_split, 'strike_from_ticker']
            
            print(f"  New strike range: {df['strike'].min()} to {df['strike'].max()}")
            
            # Remove temporary column
            df = df.drop(columns=['strike_from_ticker'])
            
            # Save the file
            df.to_csv(file, index=False)
            print(f"  ✅ Fixed and saved")
            total_rows_fixed += rows_to_fix
            files_processed += 1
        else:
            # All rows are after split, just extract from ticker
            print(f"  All rows after split date - extracting from ticker only")
            df['strike'] = df['strike_from_ticker']
            df = df.drop(columns=['strike_from_ticker'])
            df.to_csv(file, index=False)
            print(f"  ✅ Fixed and saved")
            files_processed += 1
            
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Files processed: {files_processed}")
print(f"Total rows with pre-split dates fixed: {total_rows_fixed:,}")

