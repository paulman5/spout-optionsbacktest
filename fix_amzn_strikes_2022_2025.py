#!/usr/bin/env python3
"""
Fix strike prices for AMZN data 2022-2025 by extracting from ticker symbols and dividing by 1000.
"""

import pandas as pd
import os
import re

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol.
    Example: O:AMZN240719C00130000 -> 13000000
    """
    # Remove prefix (O:AMZN) and option type (C/P)
    # The format is: O:AMZN + YYMMDD + C/P + 8-digit strike
    match = re.match(r'O:AMZN\d{6}[CP](\d{8})', ticker)
    if match:
        return int(match.group(1))
    return None

def fix_strike_prices(file_path):
    """Fix strike prices by extracting from ticker and dividing by 1000."""
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
    
    # Recalculate OTM percentage with new strike
    if 'underlying_close' in df.columns:
        df['otm_pct'] = ((df['strike'] - df['underlying_close']) / df['underlying_close']) * 100
        df['otm_pct'] = df['otm_pct'].round(2)  # Keep 2 decimals for OTM%
    
    # Recalculate ITM status
    if 'underlying_close' in df.columns:
        df['ITM'] = df['strike'].le(df['underlying_close']).map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yields with new strike
    df['premium_yield_pct'] = (df['close_price'] / df['strike']) * 100
    df['premium_yield_pct'] = df['premium_yield_pct'].round(4)  # Keep 4 decimals
    
    if 'premium_yield_pct_low' in df.columns:
        df['premium_yield_pct_low'] = (df['low_price'] / df['strike']) * 100
        df['premium_yield_pct_low'] = df['premium_yield_pct_low'].round(4)  # Keep 4 decimals
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the fixed data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Fixed strike prices for {len(df)} rows")

def main():
    """Fix strike prices for AMZN data 2022-2025."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    years = [2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                fix_strike_prices(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ Strike prices fixed for AMZN data 2022-2025!")

if __name__ == "__main__":
    main()
