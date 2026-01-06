#!/usr/bin/env python3
"""
Sort TSLA strike prices from low to high for each date
"""

import pandas as pd
from pathlib import Path

def sort_strikes_by_date(file_path):
    """Sort strike prices from low to high for each date"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Convert date columns to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Sort by date, then by strike within each date
    df_sorted = df.sort_values(['date_only', 'strike'], ascending=[True, True])
    
    print(f"   Sorted by date and strike (low to high)")
    
    # Save updated file
    df_sorted.to_csv(file_path, index=False)
    
    print(f"   ✅ Updated file saved: {len(df_sorted)} total rows")
    
    return len(df_sorted)

def main():
    """Main function to sort TSLA strikes by date"""
    print("🔄 Sorting TSLA strike prices from low to high for each date...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA")
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = sort_strikes_by_date(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ TSLA strikes sorted by date!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
