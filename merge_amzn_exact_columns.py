#!/usr/bin/env python3
"""
Merge historical stock data with AMZN options data for 2016-2025.
Match the exact column structure from TSLA example.
"""

import pandas as pd
import os
import numpy as np

def load_historical_data():
    """Load historical AMZN stock data."""
    print("Loading historical AMZN stock data...")
    
    # Try to find the historical data file
    hist_file = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/HistoricalData_AMZN.csv"
    
    if not os.path.exists(hist_file):
        print(f"❌ Historical data file not found: {hist_file}")
        return None
    
    df = pd.read_csv(hist_file)
    
    # Convert date column to datetime (handle MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    
    # Clean price columns (remove $ symbols and convert to float)
    price_columns = ['Close/Last', 'Open', 'High', 'Low']
    for col in price_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('$', '').astype(float)
    
    # Rename columns for consistency
    df = df.rename(columns={
        'Close/Last': 'underlying_close',
        'Volume': 'underlying_volume',
        'Open': 'underlying_open',
        'High': 'underlying_high',
        'Low': 'underlying_low'
    })
    
    print(f"✅ Loaded {len(df)} rows of historical data")
    return df

def merge_historical_with_options(options_file, historical_df, output_file):
    """Merge historical stock data with options data with exact column structure."""
    print(f"Processing {options_file}...")
    
    # Load options data
    options_df = pd.read_csv(options_file)
    
    if options_df.empty:
        print(f"  Options file is empty, skipping...")
        return
    
    # Convert date_only to datetime
    options_df['date_only'] = pd.to_datetime(options_df['date_only'])
    
    # Merge with historical data on date
    merged_df = options_df.merge(historical_df, left_on='date_only', right_on='date', how='left')
    
    # Calculate OTM percentage
    if 'underlying_close' in merged_df.columns and 'strike' in merged_df.columns:
        merged_df['otm_pct'] = ((merged_df['strike'] - merged_df['underlying_close']) / merged_df['underlying_close']) * 100
    
    # Calculate ITM (In The Money) - YES if strike <= underlying_close, NO otherwise
    if 'underlying_close' in merged_df.columns and 'strike' in merged_df.columns:
        merged_df['ITM'] = np.where(merged_df['strike'] <= merged_df['underlying_close'], 'YES', 'NO')
    
    # Calculate premium and premium yield
    merged_df['premium'] = merged_df['close_price']
    merged_df['premium_yield_pct'] = (merged_df['close_price'] / merged_df['strike']) * 100
    
    # For low premium calculations (using low_price)
    merged_df['premium_low'] = merged_df['low_price']
    merged_df['premium_yield_pct_low'] = (merged_df['low_price'] / merged_df['strike']) * 100
    
    # Add underlying spot (same as underlying_close)
    merged_df['underlying_spot'] = merged_df['underlying_close']
    
    # Add expiry columns (same as current values for now)
    merged_df['underlying_close_at_expiry'] = merged_df['underlying_close']
    merged_df['underlying_high_at_expiry'] = merged_df['underlying_high']
    merged_df['underlying_spot_at_expiry'] = merged_df['underlying_close']
    
    # Select and reorder columns to match TSLA structure exactly
    columns_order = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 
        'premium', 'premium_yield_pct', 'premium_low', 'premium_yield_pct_low',
        'high_price', 'low_price', 'transactions', 'window_start', 'days_to_expiry',
        'time_remaining_category', 'underlying_open', 'underlying_close', 'underlying_high',
        'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
        'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    
    # Only include columns that exist
    final_columns = [col for col in columns_order if col in merged_df.columns]
    merged_df = merged_df[final_columns]
    
    # Sort by date and strike
    merged_df = merged_df.sort_values(['date_only', 'strike'])
    
    # Save merged data
    merged_df.to_csv(output_file, index=False)
    print(f"  ✅ Merged {len(merged_df)} rows")

def main():
    """Merge historical data with AMZN options data for 2016-2025."""
    # Load historical data
    historical_df = load_historical_data()
    
    if historical_df is None:
        print("❌ Cannot proceed without historical data")
        return
    
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            {
                'options': os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv"),
                'output': os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv")
            },
            {
                'options': os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv"),
                'output': os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv")
            }
        ]
        
        for file_info in files_to_process:
            if os.path.exists(file_info['options']):
                merge_historical_with_options(file_info['options'], historical_df, file_info['output'])
            else:
                print(f"⚠️ Options file not found: {file_info['options']}")
    
    print("\n✅ Historical data merged with AMZN options data for 2016-2025!")

if __name__ == "__main__":
    main()
