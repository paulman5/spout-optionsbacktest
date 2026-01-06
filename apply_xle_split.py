#!/usr/bin/env python3
"""
Divide XLE strike prices by 2 for all options before December 5, 2025
"""

import pandas as pd
import os
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol"""
    # Pattern: O:XLEYYMMDDC/PXXXXXXXX
    match = re.search(r'O:XLE\d{6}[CP](\d{8})', ticker)
    if match:
        strike_str = match.group(1)
        return int(strike_str) / 1000  # Divide by 1000 to get actual strike
    return None

def apply_xle_split_to_file(file_path):
    """Apply strike split to a single XLE file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for all options before December 5, 2025
    split_date = pd.to_datetime('2025-12-05')
    before_split_mask = df['date_only'] < split_date
    before_split_count = before_split_mask.sum()
    
    if before_split_count == 0:
        print(f"   No options before December 5, 2025 found")
        return 0
    
    # Extract strike from ticker first (to get original values)
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Divide strike prices by 2 for all options before December 5, 2025
    df.loc[before_split_mask, 'strike'] = df.loc[before_split_mask, 'strike'] / 2
    
    # Recalculate OTM percentage for the affected options
    if 'underlying_spot' in df.columns:
        df.loc[before_split_mask, 'otm_pct'] = (
            (df.loc[before_split_mask, 'strike'] - df.loc[before_split_mask, 'underlying_spot']) / 
            df.loc[before_split_mask, 'underlying_spot'] * 100
        )
        
        # Round to 2 decimals
        df.loc[before_split_mask, 'otm_pct'] = df.loc[before_split_mask, 'otm_pct'].round(2)
    
    # Recalculate ITM status for the affected options
    if 'underlying_spot' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'ITM'] = df.loc[before_split_mask, 'strike'] <= df.loc[before_split_mask, 'underlying_spot']
        df.loc[before_split_mask, 'ITM'] = df.loc[before_split_mask, 'ITM'].map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yield for the affected options
    if 'premium' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'premium_yield_pct'] = (
            df.loc[before_split_mask, 'premium'] / df.loc[before_split_mask, 'strike'] * 100
        )
        df.loc[before_split_mask, 'premium_yield_pct'] = df.loc[before_split_mask, 'premium_yield_pct'].round(4)
    
    if 'premium_low' in df.columns and 'strike' in df.columns:
        df.loc[before_split_mask, 'premium_yield_pct_low'] = (
            df.loc[before_split_mask, 'premium_low'] / df.loc[before_split_mask, 'strike'] * 100
        )
        df.loc[before_split_mask, 'premium_yield_pct_low'] = df.loc[before_split_mask, 'premium_yield_pct_low'].round(4)
    
    # Save the updated file
    df.to_csv(file_path, index=False)
    print(f"   ✅ Divided strike prices by 2 for {before_split_count} options before December 5, 2025")
    
    return before_split_count

def main():
    """Main function to process all XLE files"""
    print("🔄 Dividing XLE strike prices by 2 for all options before December 5, 2025...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/XLE")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = apply_xle_split_to_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ Strike prices divided by 2 for XLE options!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
