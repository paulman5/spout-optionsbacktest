#!/usr/bin/env python3
"""
Recalculate all strike prices from scratch: ticker -> /1000 -> split adjustment -> round(2).
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime

def parse_strike_from_ticker(ticker):
    """Extract strike from ticker and divide by 1000."""
    if not ticker.startswith('O:'):
        return None
    
    ticker = ticker[2:]  # Remove 'O:'
    
    # Find first digit to separate symbol from numbers
    symbol_end = 0
    for i, char in enumerate(ticker):
        if char.isdigit():
            symbol_end = i
            break
    
    if symbol_end == 0:
        return None
    
    remaining = ticker[symbol_end:]
    if len(remaining) < 15:  # Need at least 6 (date) + 1 (type) + 8 (strike)
        return None
    
    try:
        # Skip 6-digit date and 1-char type, get strike
        strike_str = remaining[7:]
        return float(strike_str) / 1000.0
    except (ValueError, IndexError):
        return None

def get_split_multiplier(date_str):
    """Get the split multiplier based on the date."""
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # TSLA split history:
        # - August 31, 2020: 5-for-1 split
        # - August 25, 2022: 3-for-1 split
        
        if date_obj < datetime(2020, 8, 31):
            # Before 2020 split: apply both splits (5 * 3 = 15)
            return 15.0
        elif date_obj < datetime(2022, 8, 25):
            # Between 2020 and 2022 splits: apply only the 2022 split (3)
            return 3.0
        else:
            # After 2022 split: no adjustment needed
            return 1.0
            
    except (ValueError, TypeError):
        return 1.0

def recalculate_strikes_in_csv(csv_path):
    """Recalculate strikes from scratch in a single CSV file."""
    print(f"🔄 Recalculating {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        
        if 'ticker' not in df.columns or 'date_only' not in df.columns:
            print(f"   ⚠️  Skipping - missing required columns")
            return False
        
        # Calculate strikes from scratch
        print(f"   📊 Parsing strikes from tickers...")
        df['raw_strike'] = df['ticker'].apply(parse_strike_from_ticker)
        
        # Get split multipliers
        df['split_multiplier'] = df['date_only'].apply(get_split_multiplier)
        
        # Calculate final adjusted strikes
        df['new_strike'] = (df['raw_strike'] / df['split_multiplier']).round(2)
        
        # Check if there are differences
        different = df['strike'] != df['new_strike']
        diff_count = different.sum()
        
        if diff_count > 0:
            print(f"   📊 Found {diff_count} rows with incorrect strikes")
            
            # Show some examples
            diff_examples = df[different].head(3)
            print("   📝 Example corrections:")
            for _, row in diff_examples.iterrows():
                print(f"      {row['ticker']}: {row['strike']} → {row['new_strike']} (raw: {row['raw_strike']} ÷ {row['split_multiplier']})")
            
            # Update strikes
            df['strike'] = df['new_strike']
            
            # Clean up temporary columns
            df.drop(['raw_strike', 'split_multiplier', 'new_strike'], axis=1, inplace=True)
            
            # Save back
            df.to_csv(csv_path, index=False)
            print(f"   ✅ Recalculated and saved {csv_path}")
            return True
        else:
            print(f"   ✅ All strikes already correct")
            df.drop(['raw_strike', 'split_multiplier', 'new_strike'], axis=1, inplace=True)
            return False
            
    except Exception as e:
        print(f"   ❌ Error processing {csv_path}: {e}")
        return False

def find_all_csv_files(base_path):
    """Find all CSV files that contain options data."""
    csv_files = []
    
    # Look for CSV files with options data patterns
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith('.csv') and (
                'options' in file.lower() or 
                'pessimistic' in file.lower() or
                'day_aggs' in file.lower()
            ):
                csv_path = Path(root) / file
                csv_files.append(csv_path)
    
    return csv_files

def main():
    print("🔄 Recalculating all strike prices from scratch...")
    print("=" * 70)
    print("📋 Process: ticker → ÷1000 → split adjustment → round(2)")
    print("📅 Split Adjustment Rules:")
    print("   • Before 2020-08-31: Divide by 15 (5:1 × 3:1 splits)")
    print("   • 2020-08-31 to 2022-08-25: Divide by 3 (3:1 split only)")
    print("   • After 2022-08-25: No adjustment (1:1)")
    print("=" * 70)
    
    # Find all CSV files
    base_path = Path(".")
    csv_files = find_all_csv_files(base_path)
    
    print(f"📁 Found {len(csv_files)} CSV files to check")
    
    total_recalculated = 0
    for csv_file in sorted(csv_files):
        if recalculate_strikes_in_csv(csv_file):
            total_recalculated += 1
    
    print("=" * 70)
    print(f"✅ Completed! Recalculated strikes in {total_recalculated} files")

if __name__ == "__main__":
    main()
