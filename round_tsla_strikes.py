#!/usr/bin/env python3
"""
Round TSLA strike prices to 2 decimal places
"""

import pandas as pd
from pathlib import Path

def round_strike_prices(file_path):
    """Round strike prices to 2 decimal places"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Round strike prices to 2 decimal places
    df['strike'] = df['strike'].round(2)
    
    # Recalculate OTM percentage with rounded strikes
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
    df['otm_pct'] = df['otm_pct'].round(2)
    
    # Recalculate ITM status with rounded strikes
    df['ITM'] = (df['strike'] <= df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yields with rounded strikes
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Rounded strike prices to 2 decimal places")
    print(f"   Recalculated OTM%, ITM, and premium yields")
    
    # Save updated file
    df.to_csv(file_path, index=False)
    
    print(f"   ✅ Updated file saved: {len(df)} total rows")
    
    return len(df)

def main():
    """Main function to round TSLA strike prices"""
    print("🔄 Rounding TSLA strike prices to 2 decimal places...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA")
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = round_strike_prices(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ TSLA strike prices rounded!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
