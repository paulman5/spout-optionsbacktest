#!/usr/bin/env python3
"""
Fix premium yield calculations in all CSV files.
Correct formula: ((option_price / 100) / underlying_spot) * 100
"""

import pandas as pd
import os
from pathlib import Path

def fix_premium_yields_in_csv(csv_path):
    """Fix premium yield calculations in a single CSV file."""
    print(f"🔄 Fixing premium yields in {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        
        # Check if required columns exist
        required_cols = ['close_price', 'low_price', 'underlying_spot', 'premium_yield_pct', 'premium_yield_pct_low']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"   ⚠️  Skipping - missing columns: {missing_cols}")
            return False
        
        # Store original values for comparison
        df['original_premium_yield'] = df['premium_yield_pct']
        df['original_premium_yield_low'] = df['premium_yield_pct_low']
        
        # Recalculate premium yields using correct formula
        # Formula: ((option_price / 100) / underlying_spot) * 100
        df['premium_yield_pct'] = ((df['close_price'] / 100) / df['underlying_spot'] * 100).round(2)
        df['premium_yield_pct_low'] = ((df['low_price'] / 100) / df['underlying_spot'] * 100).round(2)
        
        # Check if there are differences
        diff_close = df['original_premium_yield'] != df['premium_yield_pct']
        diff_low = df['original_premium_yield_low'] != df['premium_yield_pct_low']
        total_diff = (diff_close | diff_low).sum()
        
        if total_diff > 0:
            print(f"   📊 Found {total_diff} rows with incorrect premium yields")
            
            # Show some examples
            diff_examples = df[diff_close | diff_low].head(3)
            print("   📝 Example corrections:")
            for _, row in diff_examples.iterrows():
                if row['original_premium_yield'] != row['premium_yield_pct']:
                    print(f"      {row['ticker']}: {row['original_premium_yield']}% → {row['premium_yield_pct']}% (close)")
                if row['original_premium_yield_low'] != row['premium_yield_pct_low']:
                    print(f"      {row['ticker']}: {row['original_premium_yield_low']}% → {row['premium_yield_pct_low']}% (low)")
            
            # Clean up temporary columns
            df.drop(['original_premium_yield', 'original_premium_yield_low'], axis=1, inplace=True)
            
            # Save back
            df.to_csv(csv_path, index=False)
            print(f"   ✅ Fixed and saved {csv_path}")
            return True
        else:
            print(f"   ✅ All premium yields already correct")
            df.drop(['original_premium_yield', 'original_premium_yield_low'], axis=1, inplace=True)
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
    print("🔄 Fixing premium yield calculations in all CSV files...")
    print("=" * 70)
    print("📋 Correct Formula: ((option_price / 100) / underlying_spot) * 100")
    print("   - option_price: in cents (close_price, low_price)")
    print("   - underlying_spot: in dollars")
    print("   - Result: premium yield as percentage")
    print("=" * 70)
    
    # Find all CSV files
    base_path = Path(".")
    csv_files = find_all_csv_files(base_path)
    
    print(f"📁 Found {len(csv_files)} CSV files to check")
    
    total_fixed = 0
    for csv_file in sorted(csv_files):
        if fix_premium_yields_in_csv(csv_file):
            total_fixed += 1
    
    print("=" * 70)
    print(f"✅ Completed! Fixed premium yields in {total_fixed} files")

if __name__ == "__main__":
    main()
