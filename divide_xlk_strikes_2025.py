#!/usr/bin/env python3
"""
Divide all XLK strike prices by 2 before December 5, 2025
"""

import pandas as pd
from pathlib import Path

def divide_xlk_strikes_before_date(file_path, split_date, split_ratio):
    """Divide strike prices by 2 before specified date"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Convert date columns to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter options before split date
    before_split = df[df['date_only'] < split_date].copy()
    after_split = df[df['date_only'] >= split_date].copy()
    
    print(f"   Rows before {split_date}: {len(before_split)}")
    print(f"   Rows on/after {split_date}: {len(after_split)}")
    
    if len(before_split) > 0:
        # Divide strike prices before split date
        before_split['strike'] = before_split['strike'] / split_ratio
        
        # Recalculate OTM percentage
        before_split['otm_pct'] = ((before_split['strike'] - before_split['underlying_spot']) / before_split['underlying_spot'] * 100)
        before_split['otm_pct'] = before_split['otm_pct'].round(2)
        
        # Recalculate ITM status
        before_split['ITM'] = (before_split['strike'] <= before_split['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yields
        before_split['premium_yield_pct'] = (before_split['premium'] / before_split['strike'] * 100).round(4)
        before_split['premium_yield_pct_low'] = (before_split['premium_low'] / before_split['strike'] * 100).round(4)
        
        print(f"   Divided by {split_ratio} for {len(before_split)} rows")
    
    # Combine data
    df_updated = pd.concat([before_split, after_split], ignore_index=True)
    
    # Round all strike prices to 2 decimal places
    df_updated['strike'] = df_updated['strike'].round(2)
    
    # Recalculate all metrics with rounded strikes
    df_updated['otm_pct'] = ((df_updated['strike'] - df_updated['underlying_spot']) / df_updated['underlying_spot'] * 100)
    df_updated['otm_pct'] = df_updated['otm_pct'].round(2)
    
    df_updated['ITM'] = (df_updated['strike'] <= df_updated['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    df_updated['premium_yield_pct'] = (df_updated['premium'] / df_updated['strike'] * 100).round(4)
    df_updated['premium_yield_pct_low'] = (df_updated['premium_low'] / df_updated['strike'] * 100).round(4)
    
    print(f"   Rounded all strike prices to 2 decimal places")
    print(f"   Recalculated all metrics with rounded strikes")
    
    # Sort by date, then by strike within each date
    df_updated = df_updated.sort_values(['date_only', 'strike'], ascending=[True, True])
    
    # Convert date back to string format
    df_updated['date_only'] = df_updated['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save updated file
    df_updated.to_csv(file_path, index=False)
    
    print(f"   ✅ Updated file saved: {len(df_updated)} total rows")
    
    return len(df_updated)

def main():
    """Main function to divide XLK strike prices by 2"""
    print("🔄 Dividing all XLK strike prices by 2 before December 5, 2025...")
    
    split_date = pd.to_datetime('2025-12-05')
    split_ratio = 2
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/XLK")
    years = [2025]  # Only 2025 is affected by this date
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = divide_xlk_strikes_before_date(file_path, split_date, split_ratio)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ XLK strike prices divided by 2!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
