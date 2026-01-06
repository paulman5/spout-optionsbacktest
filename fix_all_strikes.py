#!/usr/bin/env python3
"""
Fix strike prices in all CSV files to ensure consistent division by 1000.
"""

import pandas as pd
import os
from pathlib import Path
import re

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

def fix_csv_file(csv_path):
    """Fix strikes in a single CSV file."""
    print(f"🔧 Processing {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        
        if 'ticker' not in df.columns or 'strike' not in df.columns:
            print(f"   ⚠️  Skipping - missing ticker or strike columns")
            return False
        
        # Calculate correct strikes
        df['correct_strike'] = df['ticker'].apply(parse_strike_from_ticker)
        
        # Check if there are differences
        different = df['strike'] != df['correct_strike']
        diff_count = different.sum()
        
        if diff_count > 0:
            print(f"   📊 Found {diff_count} incorrect strikes out of {len(df)} rows")
            
            # Show some examples
            diff_examples = df[different].head(3)
            print("   📝 Example corrections:")
            for _, row in diff_examples.iterrows():
                print(f"      {row['ticker']}: {row['strike']} → {row['correct_strike']}")
            
            # Update strikes
            df['strike'] = df['correct_strike']
            df.drop('correct_strike', axis=1, inplace=True)
            
            # Save back
            df.to_csv(csv_path, index=False)
            print(f"   ✅ Fixed and saved {csv_path}")
            return True
        else:
            print(f"   ✅ All strikes already correct")
            df.drop('correct_strike', axis=1, inplace=True)
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
    print("🔧 Fixing strike prices in all CSV files...")
    print("=" * 60)
    
    # Find all CSV files
    base_path = Path(".")
    csv_files = find_all_csv_files(base_path)
    
    print(f"📁 Found {len(csv_files)} CSV files to check")
    
    total_fixed = 0
    for csv_file in sorted(csv_files):
        if fix_csv_file(csv_file):
            total_fixed += 1
    
    print("=" * 60)
    print(f"✅ Completed! Fixed strikes in {total_fixed} files")

if __name__ == "__main__":
    main()
