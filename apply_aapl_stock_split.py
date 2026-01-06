#!/usr/bin/env python3
"""
Apply 4-for-1 stock split to AAPL data before August 31, 2020.
"""

import pandas as pd
import os
from datetime import datetime

def apply_aapl_stock_split(file_path):
    """Apply 4-for-1 stock split to data before August 31, 2020."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Apply 4-for-1 split to data before August 31, 2020
    split_date = pd.to_datetime('2020-08-31')
    mask = df['date_only'] < split_date
    
    # Divide strike prices by 4 for pre-split data
    df.loc[mask, 'strike'] = df.loc[mask, 'strike'] / 4
    
    # Also divide price columns by 4 for pre-split data
    price_columns = ['open_price', 'close_price', 'high_price', 'low_price', 'premium', 'premium_low']
    for col in price_columns:
        if col in df.columns:
            df.loc[mask, col] = df.loc[mask, col] / 4
    
    # Recalculate OTM percentage with new strike
    if 'underlying_spot' in df.columns:
        df.loc[mask, 'otm_pct'] = ((df.loc[mask, 'strike'] - df.loc[mask, 'underlying_spot']) / df.loc[mask, 'underlying_spot']) * 100
        df.loc[mask, 'otm_pct'] = df.loc[mask, 'otm_pct'].round(2)  # Keep 2 decimals for OTM%
        
        # Recalculate ITM status
        df.loc[mask, 'ITM'] = df.loc[mask, 'strike'].le(df.loc[mask, 'underlying_spot']).map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yields with new strike
        df.loc[mask, 'premium_yield_pct'] = (df.loc[mask, 'close_price'] / df.loc[mask, 'strike']) * 100
        df.loc[mask, 'premium_yield_pct'] = df.loc[mask, 'premium_yield_pct'].round(4)  # Keep 4 decimals
        
        if 'premium_yield_pct_low' in df.columns:
            df.loc[mask, 'premium_yield_pct_low'] = (df.loc[mask, 'low_price'] / df.loc[mask, 'strike']) * 100
            df.loc[mask, 'premium_yield_pct_low'] = df.loc[mask, 'premium_yield_pct_low'].round(4)  # Keep 4 decimals
    
    # Round all other numeric columns to 2 decimals (except window_start)
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    columns_to_round = [col for col in numeric_columns if col not in ['otm_pct', 'premium_yield_pct', 'premium_yield_pct_low', 'window_start']]
    
    for col in columns_to_round:
        df[col] = df[col].round(2)
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the fixed data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Applied 4-for-1 stock split to {len(df[mask])} rows before August 31, 2020")

def main():
    """Apply 4-for-1 stock split to AAPL data for all years."""
    aapl_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL"
    years = [2016, 2017, 2018, 2019, 2020]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(aapl_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(aapl_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                apply_aapl_stock_split(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ 4-for-1 stock split applied to AAPL data!")

if __name__ == "__main__":
    main()
