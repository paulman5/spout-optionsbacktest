#!/usr/bin/env python3
"""
Fix AMZN 2022 data - multiply open, close, low prices by 20 for all rows
"""

import pandas as pd
from pathlib import Path

def fix_amzn_2022_prices(file_path):
    """Multiply open, close, low prices by 20 for all 2022 data"""
    print(f"Fixing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Multiply open, close, low prices by 20 for ALL rows
    df['open_price'] = df['open_price'] * 20
    df['close_price'] = df['close_price'] * 20
    df['low_price'] = df['low_price'] * 20
    df['high_price'] = df['high_price'] * 20
    
    # Recalculate premium yields (premiums now need to be divided by 20 since they were original)
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Multiplied open, close, low, high prices by 20 for all {len(df)} rows")
    print(f"   Recalculated premium yields with updated prices")
    
    # Sort by date, then by strike within each date
    df['date_only'] = pd.to_datetime(df['date_only'])
    df_updated = df.sort_values(['date_only', 'strike'], ascending=[True, True])
    
    # Convert date back to string format
    df_updated['date_only'] = df_updated['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save updated file
    df_updated.to_csv(file_path, index=False)
    
    print(f"   ✅ Fixed file saved: {len(df_updated)} total rows")
    
    return len(df_updated)

def main():
    """Main function to fix AMZN 2022 data"""
    print("🔄 Fixing AMZN 2022 data - multiply open, close, low prices by 20...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN")
    years = [2022]  # Fix 2022 data only
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = fix_amzn_2022_prices(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ AMZN 2022 data fixed!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
