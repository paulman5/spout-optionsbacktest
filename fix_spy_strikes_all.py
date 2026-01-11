#!/usr/bin/env python3
"""
Reset all SPY strike prices: extract from ticker and divide by 1000
"""

import pandas as pd
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike from ticker format: O:SPY200110C00300000"""
    ticker_clean = ticker.replace('O:', '')
    match = re.search(r'[CP](\d+)', ticker_clean)
    if match:
        strike_raw = int(match.group(1))
        return strike_raw / 1000.0
    return None

data_dir = Path('python-boilerplate/data/SPY/holidays')
files = sorted(data_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("RESETTING SPY STRIKE PRICES FOR ALL HOLIDAY FILES")
print("=" * 80)
print("Extract from ticker and divide by 1000")
print()

total_rows_fixed = 0
files_processed = 0

for file in files:
    year = file.stem.split('_')[0]
    print(f"Processing {year}...")
    
    try:
        df = pd.read_csv(file)
        original_rows = len(df)
        
        print(f"  Rows: {original_rows:,}")
        print(f"  Current strike range: {df['strike'].min()} to {df['strike'].max()}")
        
        # Extract strike from ticker
        print(f"  Extracting strikes from tickers...")
        df['strike_new'] = df['ticker'].apply(extract_strike_from_ticker)
        
        # Check if any are None
        if df['strike_new'].isna().any():
            print(f"  ⚠️  Warning: Some strikes could not be extracted")
        
        # Update strike column
        df['strike'] = df['strike_new']
        df = df.drop(columns=['strike_new'])
        
        print(f"  New strike range: {df['strike'].min()} to {df['strike'].max()}")
        
        # Save the file
        df.to_csv(file, index=False)
        print(f"  ✅ Fixed and saved")
        total_rows_fixed += original_rows
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
print(f"Total rows fixed: {total_rows_fixed:,}")

