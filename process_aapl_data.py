#!/usr/bin/env python3
"""
Process AAPL data to match the exact column structure and formatting.
"""

import pandas as pd
import os
import numpy as np
import re

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol.
    Example: O:AAPL210319C00110000 -> 11000000
    """
    # Remove prefix (O:AAPL) and option type (C/P)
    # The format is: O:AAPL + YYMMDD + C/P + 8-digit strike
    match = re.match(r'O:AAPL\d{6}[CP](\d{8})', ticker)
    if match:
        return int(match.group(1))
    return None

def process_aapl_data(file_path):
    """Process AAPL data to match exact column structure."""
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
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the processed data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Processed {len(df)} rows")

def main():
    """Process AAPL data for all years."""
    aapl_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL"
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(aapl_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(aapl_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                process_aapl_data(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ AAPL data processed for all years!")

if __name__ == "__main__":
    main()
