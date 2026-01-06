#!/usr/bin/env python3
"""
Divide NVDA strike prices by 10 for call options ON and AFTER July 20, 2021 and BEFORE June 10, 2024
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

def apply_second_split_to_file(file_path):
    """Apply second strike split to a single file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for call options ON and AFTER July 20, 2021 and BEFORE June 10, 2024
    start_date = pd.to_datetime('2021-07-20')
    end_date = pd.to_datetime('2024-06-10')
    split_mask = (df['date_only'] >= start_date) & (df['date_only'] < end_date) & (df['option_type'] == 'C')
    split_count = split_mask.sum()
    
    if split_count == 0:
        print(f"   No call options in the specified date range found")
        return 0
    
    # Extract strike from ticker first (to get original values)
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Divide strike prices by 10 for the specified date range
    df.loc[split_mask, 'strike'] = df.loc[split_mask, 'strike'] / 10
    
    # Recalculate OTM percentage for the affected options
    if 'underlying_spot' in df.columns:
        df.loc[split_mask, 'otm_pct'] = (
            (df.loc[split_mask, 'strike'] - df.loc[split_mask, 'underlying_spot']) / 
            df.loc[split_mask, 'underlying_spot'] * 100
        )
        
        # Round to 2 decimals
        df.loc[split_mask, 'otm_pct'] = df.loc[split_mask, 'otm_pct'].round(2)
    
    # Save the updated file
    df.to_csv(file_path, index=False)
    print(f"   ✅ Divided strike prices by 10 for {split_count} call options in the specified date range")
    
    return split_count

def main():
    """Main function to process all NVDA files"""
    print("🔄 Dividing NVDA strike prices by 10 for call options ON and AFTER July 20, 2021 and BEFORE June 10, 2024...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/NVDA")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = apply_second_split_to_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ Strike prices divided by 10 for NVDA call options in the specified date range!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
