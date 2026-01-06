#!/usr/bin/env python3
"""
Recalculate OTM percentage for NVDA call options before July 20, 2021
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

def recalculate_otm_to_file(file_path):
    """Recalculate OTM percentage for a single file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for call options before July 20, 2021
    split_date = pd.to_datetime('2021-07-20')
    before_split_mask = (df['date_only'] < split_date) & (df['option_type'] == 'C')
    before_split_count = before_split_mask.sum()
    
    if before_split_count == 0:
        print(f"   No call options before July 20, 2021 found")
        return 0
    
    # Recalculate OTM percentage for call options before July 20, 2021
    # OTM% = (strike / underlying_spot) * 100
    if 'underlying_spot' in df.columns:
        df.loc[before_split_mask, 'otm_pct'] = (
            df.loc[before_split_mask, 'strike'] / df.loc[before_split_mask, 'underlying_spot'] * 100
        )
        
        # Round to 2 decimals
        df.loc[before_split_mask, 'otm_pct'] = df.loc[before_split_mask, 'otm_pct'].round(2)
    
    # Save the updated file
    df.to_csv(file_path, index=False)
    print(f"   ✅ Recalculated OTM% for {before_split_count} call options before July 20, 2021")
    
    return before_split_count

def main():
    """Main function to process all NVDA files"""
    print("🔄 Recalculating OTM% for NVDA call options before July 20, 2021...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/NVDA")
    years = range(2016, 2026)  # 2016-2025
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = recalculate_otm_to_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ OTM% recalculated for NVDA call options!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
