#!/usr/bin/env python3
"""
Fix OTM percentage for all tickers in 2025 holiday files.
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

def fix_ticker_2025_otm(ticker_name):
    """Fix OTM percentage for a single ticker's 2025 file."""
    file_path = Path(f'python-boilerplate/data/{ticker_name}/holidays/2025_options_pessimistic.csv')
    
    if not file_path.exists():
        return False, f"File not found"
    
    try:
        print(f'  Reading {ticker_name}...')
        df = pd.read_csv(file_path)
        
        print(f'    Rows: {len(df):,}')
        
        # Check if required columns exist
        if 'strike' not in df.columns or 'underlying_spot' not in df.columns:
            return False, "Missing required columns (strike or underlying_spot)"
        
        # Calculate correct OTM percentage
        # Formula: ((strike - underlying_spot) / underlying_spot) * 100
        df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
        
        # Round to 2 decimal places
        df['otm_pct'] = df['otm_pct'].round(2)
        
        print(f'    OTM range: {df["otm_pct"].min():.2f}% to {df["otm_pct"].max():.2f}%')
        
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
print(f"FIXING 2025 OTM PERCENTAGE FOR ALL TICKERS")
print("=" * 80)
print(f"Formula: ((strike - underlying_spot) / underlying_spot) * 100")
print(f"Found {len(tickers_with_2025)} tickers with 2025 data\n")

fixed_count = 0
failed_count = 0

for ticker in sorted(tickers_with_2025):
    print(f"Processing {ticker}...")
    success, message = fix_ticker_2025_otm(ticker)
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

