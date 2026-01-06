#!/usr/bin/env python3
"""
Apply 3-for-1 stock split to TSLA options between August 25, 2022 and August 31, 2020, then round all strikes to 2 decimals
"""

import pandas as pd
from pathlib import Path

def apply_tsla_split_and_round(file_path, split_start_date, split_end_date, split_ratio):
    """Apply stock split to TSLA options between specified dates"""
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
    
    # Separate data by date ranges
    before_split = df[df['date_only'] < split_start_date].copy()
    during_split = df[(df['date_only'] >= split_start_date) & (df['date_only'] <= split_end_date)].copy()
    after_split = df[df['date_only'] > split_end_date].copy()
    
    print(f"   Rows before {split_start_date}: {len(before_split)}")
    print(f"   Rows during split period ({split_start_date} to {split_end_date}): {len(during_split)}")
    print(f"   Rows after {split_end_date}: {len(after_split)}")
    
    # Apply split to strikes during split period
    if len(during_split) > 0:
        during_split['strike'] = during_split['strike'] / split_ratio
        
        # Recalculate OTM percentage
        during_split['otm_pct'] = ((during_split['strike'] - during_split['underlying_spot']) / during_split['underlying_spot'] * 100)
        during_split['otm_pct'] = during_split['otm_pct'].round(2)
        
        # Recalculate ITM status
        during_split['ITM'] = (during_split['strike'] <= during_split['underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yields
        during_split['premium_yield_pct'] = (during_split['premium'] / during_split['strike'] * 100).round(4)
        during_split['premium_yield_pct_low'] = (during_split['premium_low'] / during_split['strike'] * 100).round(4)
        
        print(f"   Applied {split_ratio} split to {len(during_split)} rows")
    
    # Combine all data
    df_updated = pd.concat([before_split, during_split, after_split], ignore_index=True)
    
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
    
    # Sort by date
    df_updated = df_updated.sort_values('date_only')
    
    # Convert date back to string format
    df_updated['date_only'] = df_updated['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save updated file
    df_updated.to_csv(file_path, index=False)
    
    print(f"   ✅ Updated file saved: {len(df_updated)} total rows")
    
    return len(df_updated)

def main():
    """Main function to apply TSLA split and rounding"""
    print("🔄 Applying 3-for-1 split to TSLA options (Aug 25, 2022 to Aug 31, 2020) and rounding all strikes...")
    
    split_start_date = pd.to_datetime('2020-08-25')
    split_end_date = pd.to_datetime('2020-08-31')
    split_ratio = 3
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA")
    years = [2020]  # Only 2020 is affected by this date range
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = apply_tsla_split_and_round(file_path, split_start_date, split_end_date, split_ratio)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ TSLA split and rounding applied!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
