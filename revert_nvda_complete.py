#!/usr/bin/env python3
"""
Completely revert NVDA data to original state
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

def revert_nvda_to_original(file_path):
    """Revert NVDA file to original state"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Extract strike from ticker (original values)
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Recalculate all metrics based on original strike prices
    if 'underlying_spot' in df.columns and 'strike' in df.columns:
        # Recalculate OTM percentage
        df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
        
        # Recalculate ITM status
        df['ITM'] = df['strike'] <= df['underlying_spot']
        df['ITM'] = df['ITM'].map({True: 'YES', False: 'NO'})
        
        # Recalculate premium yield
        if 'premium' in df.columns:
            df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100)
        if 'premium_low' in df.columns:
            df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100)
    
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
    print(f"   ✅ Reverted to original state for {len(df)} rows")
    
    return len(df)

def main():
    """Main function to process all NVDA files"""
    print("🔄 Completely reverting NVDA data to original state...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/NVDA")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = revert_nvda_to_original(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ NVDA data completely reverted to original state!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
