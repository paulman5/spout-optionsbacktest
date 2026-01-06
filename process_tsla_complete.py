#!/usr/bin/env python3
"""
Process TSLA data to match the exact column structure with all historical columns.
"""

import pandas as pd
import os
import numpy as np
import re

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol.
    Example: O:TSLA190111C00230000 -> 23000000
    """
    # Remove prefix (O:TSLA) and option type (C/P)
    # The format is: O:TSLA + YYMMDD + C/P + 8-digit strike
    match = re.match(r'O:TSLA\d{6}[CP](\d{8})', ticker)
    if match:
        return int(match.group(1))
    return None

def process_tsla_data(file_path):
    """Process TSLA data to match exact column structure."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Extract strike from ticker and divide by 1000
    df['strike_from_ticker'] = df['ticker'].apply(extract_strike_from_ticker)
    df['strike'] = df['strike_from_ticker'] / 1000
    
    # Drop the temporary column
    df = df.drop('strike_from_ticker', axis=1)
    
    # Calculate OTM percentage
    if 'underlying_close' in df.columns:
        df['otm_pct'] = ((df['strike'] - df['underlying_close']) / df['underlying_close']) * 100
        df['otm_pct'] = df['otm_pct'].round(2)  # 2 decimals for OTM%
    
    # Calculate ITM status
    if 'underlying_close' in df.columns:
        df['ITM'] = df['strike'].le(df['underlying_close']).map({True: 'YES', False: 'NO'})
    
    # Calculate premium and premium yield
    df['premium'] = df['close_price']
    df['premium_yield_pct'] = (df['close_price'] / df['strike']) * 100
    df['premium_yield_pct'] = df['premium_yield_pct'].round(4)  # 4 decimals for premium yield
    
    # For low premium calculations
    if 'premium_yield_pct_low' in df.columns:
        df['premium_yield_pct_low'] = (df['low_price'] / df['strike']) * 100
        df['premium_yield_pct_low'] = df['premium_yield_pct_low'].round(4)  # 4 decimals
    
    # Round all other numeric columns to 2 decimals (except window_start)
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    columns_to_round = [col for col in numeric_columns if col not in ['otm_pct', 'premium_yield_pct', 'premium_yield_pct_low', 'window_start']]
    
    for col in columns_to_round:
        df[col] = df[col].round(2)
    
    # Define the exact column order with all historical columns
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
    final_columns = [col for col in columns_order if col in df.columns]
    df = df[final_columns]
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the processed data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Processed {len(df)} rows")

def main():
    """Process TSLA data for all years."""
    tsla_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA"
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(tsla_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(tsla_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                process_tsla_data(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ TSLA data processed for all years!")

if __name__ == "__main__":
    main()
