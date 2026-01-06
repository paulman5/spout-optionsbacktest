#!/usr/bin/env python3
"""
Fix XLF split application - only apply split before September 19, 2016
"""

import pandas as pd
from pathlib import Path

def fix_xlf_split_application(file_path, split_date, split_ratio):
    """Fix split application - only apply to data before split date"""
    print(f"Fixing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Convert date columns to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Separate data before and after split
    before_split = df[df['date_only'] < split_date].copy()
    after_split = df[df['date_only'] >= split_date].copy()
    
    print(f"   Rows before {split_date}: {len(before_split)}")
    print(f"   Rows on/after {split_date}: {len(after_split)}")
    
    if len(before_split) > 0:
        # Apply split to strike prices before split date
        before_split['strike'] = before_split['strike'] / split_ratio
        
        # Recalculate OTM percentage
        before_split['otm_pct'] = ((before_split['strike'] - before_split['underlying_spot']) / before_split['underlying_spot'] * 100)
        before_split['otm_pct'] = before_split['otm_pct'].round(2)
        
        # Recalculate ITM status
        before_split['ITM'] = (before_split['strike'] <= before_split['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yields
        before_split['premium_yield_pct'] = (before_split['premium'] / before_split['strike'] * 100).round(4)
        before_split['premium_yield_pct_low'] = (before_split['premium_low'] / before_split['strike'] * 100).round(4)
        
        print(f"   Applied {split_ratio} split to {len(before_split)} rows")
    
    # For data after split date, restore original strike prices by multiplying back
    if len(after_split) > 0:
        # Check if strikes are currently divided by split ratio
        # We need to restore original strikes for data after split date
        print(f"   Restoring original strike prices for {len(after_split)} rows")
        
        # The current strikes are already divided, so multiply back
        after_split['strike'] = after_split['strike'] * split_ratio
        
        # Recalculate OTM percentage
        after_split['otm_pct'] = ((after_split['strike'] - after_split['underlying_spot']) / after_split['underlying_spot'] * 100)
        after_split['otm_pct'] = after_split['otm_pct'].round(2)
        
        # Recalculate ITM status
        after_split['ITM'] = (after_split['strike'] <= after_split['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yields
        after_split['premium_yield_pct'] = (after_split['premium'] / after_split['strike'] * 100).round(4)
        after_split['premium_yield_pct_low'] = (after_split['premium_low'] / after_split['strike'] * 100).round(4)
    
    # Combine data
    df_updated = pd.concat([before_split, after_split], ignore_index=True)
    
    # Sort by date
    df_updated = df_updated.sort_values('date_only')
    
    # Convert date back to string format
    df_updated['date_only'] = df_updated['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save updated file
    df_updated.to_csv(file_path, index=False)
    
    print(f"   ✅ Fixed file saved: {len(df_updated)} total rows")
    
    return len(df_updated)

def main():
    """Main function to fix XLF split application"""
    print("🔄 Fixing XLF split application - only apply before September 19, 2016...")
    
    split_date = pd.to_datetime('2016-09-19')
    split_ratio = 1.231
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/XLF")
    years = [2016]
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = fix_xlf_split_application(file_path, split_date, split_ratio)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ XLF split application fixed!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
