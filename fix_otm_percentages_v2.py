#!/usr/bin/env python3
"""
Fix OTM percentage calculations in all CSV files.
Correct formula: ((strike - underlying_spot) / underlying_spot) * 100
This gives negative values for ITM options and positive for OTM options.
"""

import pandas as pd
import os
from pathlib import Path

def fix_otm_percentages_in_csv(csv_path):
    """Fix OTM percentage calculations in a single CSV file."""
    print(f"🔄 Fixing OTM percentages in {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        
        # Check if required columns exist
        required_cols = ['strike', 'underlying_spot', 'otm_pct', 'ITM']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"   ⚠️  Skipping - missing columns: {missing_cols}")
            return False
        
        # Store original values for comparison
        df['original_otm_pct'] = df['otm_pct']
        
        # Recalculate OTM percentages using correct formula
        # Formula: ((strike - underlying_spot) / underlying_spot) * 100
        # This gives negative values for ITM options, positive for OTM options
        df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
        
        # Recalculate ITM based on new OTM values
        # ITM: YES if strike < spot (negative otm_pct), NO otherwise
        df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Check if there are differences
        diff_otm = df['original_otm_pct'] != df['otm_pct']
        total_diff = diff_otm.sum()
        
        if total_diff > 0:
            print(f"   📊 Found {total_diff} rows with incorrect OTM percentages")
            
            # Show some examples
            diff_examples = df[diff_otm].head(3)
            print("   📝 Example corrections:")
            for _, row in diff_examples.iterrows():
                print(f"      {row['ticker']}: {row['original_otm_pct']}% → {row['otm_pct']}% (ITM: {row['ITM']})")
            
            # Clean up temporary columns
            df.drop(['original_otm_pct'], axis=1, inplace=True)
            
            # Save back
            df.to_csv(csv_path, index=False)
            print(f"   ✅ Fixed and saved {csv_path}")
            return True
        else:
            print(f"   ✅ All OTM percentages already correct")
            df.drop(['original_otm_pct'], axis=1, inplace=True)
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
    print("🔄 Fixing OTM percentage calculations in all CSV files...")
    print("=" * 70)
    print("📋 Correct Formula: ((strike - underlying_spot) / underlying_spot) * 100")
    print("   - strike: in dollars (split-adjusted)")
    print("   - underlying_spot: in dollars (split-adjusted)")
    print("   - Result: OTM percentage")
    print("     • Negative values = ITM (strike < spot)")
    print("     • Positive values = OTM (strike > spot)")
    print("     • Zero = At The Money (strike = spot)")
    print("=" * 70)
    
    # Find all CSV files
    base_path = Path(".")
    csv_files = find_all_csv_files(base_path)
    
    print(f"📁 Found {len(csv_files)} CSV files to check")
    
    total_fixed = 0
    for csv_file in sorted(csv_files):
        if fix_otm_percentages_in_csv(csv_file):
            total_fixed += 1
    
    print("=" * 70)
    print(f"✅ Completed! Fixed OTM percentages in {total_fixed} files")

if __name__ == "__main__":
    main()
