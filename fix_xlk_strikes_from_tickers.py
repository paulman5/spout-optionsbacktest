#!/usr/bin/env python3
"""
Fix XLK strike prices by extracting from ticker symbols
"""

import pandas as pd
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol"""
    # Pattern: O:XLKYYMMDDC/PXXXXXXXX
    match = re.search(r'O:XLK\d{6}[CP](\d{8})', ticker)
    if match:
        strike_str = match.group(1)
        # Convert to actual strike price by dividing by 1000
        return int(strike_str) / 1000.0
    return None

def fix_xlk_strikes_from_tickers(file_path):
    """Fix XLK strike prices by extracting from ticker symbols"""
    print(f"Fixing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    original_count = len(df)
    print(f"   Original rows: {original_count}")
    
    # Extract correct strike prices from ticker symbols
    df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Recalculate OTM percentage
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
    df['otm_pct'] = df['otm_pct'].round(2)
    
    # Recalculate ITM status
    df['ITM'] = (df['strike'] <= df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yields
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Extracted strike prices from ticker symbols")
    print(f"   Recalculated OTM%, ITM, and premium yields")
    
    # Round all strike prices to 2 decimal places
    df['strike'] = df['strike'].round(2)
    
    # Recalculate all metrics with rounded strikes
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100)
    df['otm_pct'] = df['otm_pct'].round(2)
    
    df['ITM'] = (df['strike'] <= df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    df['premium_yield_pct'] = (df['premium'] / df['strike'] * 100).round(4)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['strike'] * 100).round(4)
    
    print(f"   Rounded strike prices to 2 decimal places")
    
    # Sort by date, then by strike within each date
    df = df.sort_values(['date_only', 'strike'], ascending=[True, True])
    
    # Save updated file
    df.to_csv(file_path, index=False)
    
    print(f"   ✅ Fixed file saved: {len(df)} total rows")
    
    return len(df)

def main():
    """Main function to fix XLK strike prices from tickers"""
    print("🔄 Fixing XLK strike prices by extracting from ticker symbols...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/XLK")
    years = [2021, 2022, 2023, 2024, 2025]
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = fix_xlk_strikes_from_tickers(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ XLK strike prices fixed from ticker symbols!")
    print(f"📊 Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
