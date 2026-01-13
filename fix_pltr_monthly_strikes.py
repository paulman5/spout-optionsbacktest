#!/usr/bin/env python3
"""
Fix strike prices for all PLTR monthly files.
Extract strike from ticker and divide by 1000.
Format: O:PLTR250221C00016000 -> 00016000 -> 16000 / 1000 = 16.0
"""

import pandas as pd
import re
from pathlib import Path

base_dir = Path("python-boilerplate/data/PLTR/monthly")
files = sorted(base_dir.glob('*_options_pessimistic.csv'))

print("=" * 80)
print("FIXING STRIKE PRICES FOR ALL PLTR MONTHLY FILES")
print("=" * 80)
print(f"Formula: Extract from ticker and divide by 1000")
print()

def extract_strike_from_ticker(ticker: str) -> float:
    """Extract strike from ticker and divide by 1000."""
    try:
        if not isinstance(ticker, str) or not ticker.startswith('O:'):
            return None
        
        ticker = ticker[2:]  # Remove 'O:'
        
        # Find where the strike part starts (after expiration and option type)
        # Format: SYMBOL + YYMMDD + C/P + STRIKE (8 digits)
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        strike_str = match.group(4)  # e.g., "00016000"
        strike = float(strike_str) / 1000.0  # e.g., 16.0
        return strike
    except (ValueError, IndexError, AttributeError, TypeError):
        return None

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
        if 'ticker' not in df.columns:
            print(f"  ❌ Missing ticker column")
            files_failed.append((year, "Missing ticker column"))
            continue
        
        print(f"  Rows: {original_rows:,}")
        
        # Show current strike range
        if 'strike' in df.columns:
            current_min = df['strike'].min() if df['strike'].notna().any() else None
            current_max = df['strike'].max() if df['strike'].notna().any() else None
            if current_min is not None:
                print(f"  Current strike range: {current_min:.2f} to {current_max:.2f}")
        
        # Extract strike from ticker
        df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
        
        # Count how many were successfully extracted
        extracted_count = df['strike'].notna().sum()
        print(f"  Successfully extracted strikes: {extracted_count:,} / {original_rows:,}")
        
        # Show new strike range
        new_min = df['strike'].min() if df['strike'].notna().any() else None
        new_max = df['strike'].max() if df['strike'].notna().any() else None
        if new_min is not None:
            print(f"  New strike range: {new_min:.2f} to {new_max:.2f}")
        
        # Recalculate OTM percentage with new strikes
        if 'underlying_spot' in df.columns:
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
            df['otm_pct'] = df['otm_pct'].round(2)
            
            # Update ITM column
            df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Save the file
        df.to_csv(file, index=False)
        total_rows_fixed += extracted_count
        files_processed += 1
        print(f"  ✅ Fixed and saved")
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
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
