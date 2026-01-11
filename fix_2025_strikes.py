#!/usr/bin/env python3
"""
Fix strike prices for all tickers in 2025 holiday files.
Extract strike from ticker and divide by 1000 (not by 4).
"""

import pandas as pd
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker format: O:AAPL250110C00230000"""
    ticker_clean = ticker.replace('O:', '')
    match = re.search(r'[CP](\d+)', ticker_clean)
    if match:
        strike_raw = int(match.group(1))
        return strike_raw / 1000.0
    return None

def fix_ticker_2025(ticker_name):
    """Fix strike prices for a single ticker's 2025 file."""
    file_path = Path(f'python-boilerplate/data/{ticker_name}/holidays/2025_options_pessimistic.csv')
    
    if not file_path.exists():
        return False, f"File not found"
    
    try:
        print(f'  Reading {ticker_name}...')
        df = pd.read_csv(file_path)
        
        print(f'    Rows: {len(df):,}')
        print(f'    Current strike range: {df["strike"].min()} to {df["strike"].max()}')
        
        # Extract strikes from tickers
        df['strike_new'] = df['ticker'].apply(extract_strike_from_ticker)
        
        # Check if any are None
        if df['strike_new'].isna().any():
            print(f'    ⚠️  Warning: Some strikes could not be extracted')
        
        # Update strike column
        df['strike'] = df['strike_new']
        df = df.drop(columns=['strike_new'])
        
        print(f'    New strike range: {df["strike"].min()} to {df["strike"].max()}')
        
        # Save
        df.to_csv(file_path, index=False)
        print(f'    ✅ Fixed and saved')
        return True, f"Fixed {len(df):,} rows"
        
    except Exception as e:
        return False, f"Error: {str(e)}"

# Get all tickers with 2025 data
data_dir = Path('python-boilerplate/data')
tickers_with_2025 = []

for ticker_dir in sorted(data_dir.iterdir()):
    if ticker_dir.is_dir():
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            file_2025 = holidays_dir / '2025_options_pessimistic.csv'
            if file_2025.exists():
                tickers_with_2025.append(ticker_dir.name)

print("=" * 80)
print(f"FIXING 2025 STRIKE PRICES FOR ALL TICKERS")
print("=" * 80)
print(f"Found {len(tickers_with_2025)} tickers with 2025 data\n")

fixed_count = 0
failed_count = 0

for ticker in sorted(tickers_with_2025):
    print(f"Processing {ticker}...")
    success, message = fix_ticker_2025(ticker)
    if success:
        fixed_count += 1
    else:
        failed_count += 1
        print(f"  ❌ {message}")
    print()

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✅ Successfully fixed: {fixed_count} tickers")
if failed_count > 0:
    print(f"❌ Failed: {failed_count} tickers")
print(f"Total processed: {len(tickers_with_2025)} tickers")

