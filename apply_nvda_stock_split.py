#!/usr/bin/env python3
"""
Apply 40-for-1 stock split to NVDA data before July 20, 2021
"""

import pandas as pd
import os
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol"""
    # Pattern: O:NVDAYYMMDDC/PXXXXXXXX
    match = re.search(r'O:NVDA\d{6}[CP](\d{8})', ticker)
    if match:
        strike_str = match.group(1)
        return int(strike_str) / 1000  # Divide by 1000 to get actual strike
    return None

def apply_stock_split_to_file(file_path):
    """Apply 40-for-1 stock split to a single file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for data before July 20, 2021
    split_date = pd.to_datetime('2021-07-20')
    before_split_mask = df['date_only'] < split_date
    before_split_count = before_split_mask.sum()
    
    if before_split_count == 0:
        print(f"   No data before July 20, 2021 found")
        return 0
    
    # Apply 40-for-1 split to data before July 20, 2021
    # Divide strike and all price columns by 40
    
    # Columns to divide by 40
    price_columns = [
        'strike', 'open_price', 'close_price', 'premium', 'premium_low',
        'high_price', 'low_price', 'underlying_open', 'underlying_close',
        'underlying_high', 'underlying_low', 'underlying_spot'
    ]
    
    # Extract strike from ticker first (in case it's not already extracted)
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Apply split to price columns
    for col in price_columns:
        if col in df.columns:
            df.loc[before_split_mask, col] = df.loc[before_split_mask, col] / 40
    
    # Recalculate OTM percentage for pre-split data
    if 'underlying_spot' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'otm_pct'] = (
            (df.loc[before_split_mask, 'strike'] - df.loc[before_split_mask, 'underlying_spot']) / 
            df.loc[before_split_mask, 'underlying_spot'] * 100
        )
    
    # Recalculate ITM status for pre-split data
    if 'underlying_spot' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'ITM'] = df.loc[before_split_mask, 'strike'] <= df.loc[before_split_mask, 'underlying_spot']
        df.loc[before_split_mask, 'ITM'] = df.loc[before_split_mask, 'ITM'].map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yield for pre-split data
    if 'premium' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'premium_yield_pct'] = (
            df.loc[before_split_mask, 'premium'] / df.loc[before_split_mask, 'strike'] * 100
        )
        df.loc[before_split_mask, 'premium_yield_pct_low'] = (
            df.loc[before_split_mask, 'premium_low'] / df.loc[before_split_mask, 'strike'] * 100
        )
    
    # Ensure decimal precision
    if 'otm_pct' in df.columns:
        df['otm_pct'] = df['otm_pct'].round(2)
    if 'premium_yield_pct' in df.columns:
        df['premium_yield_pct'] = df['premium_yield_pct'].round(4)
    if 'premium_yield_pct_low' in df.columns:
        df['premium_yield_pct_low'] = df['premium_yield_pct_low'].round(4)
    
    # Round other numeric columns to 2 decimals
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        if col not in ['premium_yield_pct', 'premium_yield_pct_low', 'window_start']:
            df[col] = df[col].round(2)
    
    # Save the updated file
    df.to_csv(file_path, index=False)
    print(f"   ✅ Applied 40-for-1 stock split to {before_split_count} rows before July 20, 2021")
    
    return before_split_count

def main():
    """Main function to process all NVDA files"""
    print("🔄 Applying 40-for-1 stock split to NVDA data before July 20, 2021...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/NVDA")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = apply_stock_split_to_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ 40-for-1 stock split applied to NVDA data!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
