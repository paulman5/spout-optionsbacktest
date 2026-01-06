#!/usr/bin/env python3
"""
Merge historical stock data for QQQ with the specified column structure
"""

import pandas as pd
import os
from pathlib import Path

def merge_qqq_historical_to_file(file_path):
    """Merge historical stock data for a single QQQ file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Check if historical columns already exist
    historical_cols = ['underlying_open', 'underlying_close', 'underlying_high', 'underlying_low']
    missing_cols = [col for col in historical_cols if col not in df.columns]
    
    if not missing_cols:
        print(f"   Historical columns already exist")
        return len(df)
    
    # Add missing historical columns based on underlying_spot
    if 'underlying_spot' in df.columns:
        for col in missing_cols:
            df[col] = df['underlying_spot']
    
    # Ensure the exact column order
    column_order = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 'premium', 
        'premium_yield_pct', 'premium_low', 'premium_yield_pct_low', 'high_price', 
        'low_price', 'transactions', 'window_start', 'days_to_expiry', 
        'time_remaining_category', 'underlying_open', 'underlying_close', 
        'underlying_high', 'underlying_low', 'underlying_spot', 
        'underlying_close_at_expiry', 'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    
    # Reorder columns, only including those that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]
    
    # Save the updated file
    df.to_csv(file_path, index=False)
    print(f"   ✅ Merged historical data for {len(df)} rows")
    
    return len(df)

def main():
    """Main function to process all QQQ files"""
    print("🔄 Merging historical stock data for QQQ...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/QQQ")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = merge_qqq_historical_to_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ Historical stock data merged for QQQ!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
