#!/usr/bin/env python3
"""
Divide AMZN strike prices by 20 for years 2016-2021
"""

import pandas as pd
from pathlib import Path

def divide_amzn_strikes_2016_2021(file_path, split_ratio):
    """Divide all strike prices by specified ratio"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Extract correct strike prices from ticker symbols first
    import re
    def extract_strike_from_ticker(ticker):
        match = re.search(r'O:AMZN\d{6}[CP](\d{8})', ticker)
        if match:
            strike_str = match.group(1)
            return int(strike_str) / 1000.0
        return None
    
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Divide all strike prices by split ratio
    df['strike'] = df['strike'] / split_ratio
    
    # Recalculate OTM percentage
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
    df['otm_pct'] = df['otm_pct'].round(2)
    
    # Recalculate ITM status
    df['ITM'] = (df['strike'] <= df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yields
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Divided all strikes by {split_ratio}")
    
    # Round all strike prices to 2 decimal places
    df['strike'] = df['strike'].round(2)
    
    # Recalculate all metrics with rounded strikes
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
    df['otm_pct'] = df['otm_pct'].round(2)
    
    df['ITM'] = (df['strike'] <= df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Rounded all strike prices to 2 decimal places")
    print(f"   Recalculated all metrics with rounded strikes")
    
    # Sort by date, then by strike within each date
    df['date_only'] = pd.to_datetime(df['date_only'])
    df_updated = df.sort_values(['date_only', 'strike'], ascending=[True, True])
    
    # Convert date back to string format
    df_updated['date_only'] = df_updated['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save updated file
    df_updated.to_csv(file_path, index=False)
    
    print(f"   ✅ Updated file saved: {len(df_updated)} total rows")
    
    return len(df_updated)

def main():
    """Main function to divide AMZN strike prices for 2016-2021"""
    print("🔄 Dividing AMZN strike prices by 20 for years 2016-2021...")
    
    split_ratio = 20
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN")
    years = [2016, 2017, 2018, 2019, 2020, 2021]  # Years 2016-2021
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = divide_amzn_strikes_2016_2021(file_path, split_ratio)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ AMZN strike prices divided by 20 for 2016-2021!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
