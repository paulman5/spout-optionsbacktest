#!/usr/bin/env python3
"""
Fix AMZN prices that were incorrectly divided by a large factor.
Multiply prices by 1000 for dates after June 6, 2022.
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

def extract_strike_from_ticker(ticker_str):
    """Extract strike price from ticker symbol."""
    # Pattern: O:TICKERYYMMDDCXXXXXX where XXXXXX is strike in cents
    match = re.search(r'C(\d+)$', ticker_str)
    if match:
        strike_cents = int(match.group(1))
        strike_dollars = strike_cents / 1000.0
        return strike_dollars
    return None

def fix_amzn_prices_final_factor(file_path):
    """Fix AMZN prices with correct multiplication factor."""
    try:
        df = pd.read_csv(file_path)
        
        if not df.empty and 'ticker' in df.columns:
            sample_ticker = df['ticker'].iloc[0]
            if 'AMZN' in sample_ticker:
                print(f"    🔧 Fixing AMZN prices in {file_path.name}")
                
                # Convert date_only to datetime
                df['date_only'] = pd.to_datetime(df['date_only'])
                
                # Extract strikes from ticker symbols (start fresh)
                df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
                
                # ONLY apply 20-for-1 split to strikes before June 6, 2022
                mask_split = (df['date_only'] >= '1998-06-02') & (df['date_only'] < '2022-06-06')
                df.loc[mask_split, 'strike'] = df.loc[mask_split, 'strike'] / 20
                
                # For dates AFTER June 6, 2022, multiply ALL prices by 1000 to fix the incorrect division
                mask_fix_prices = df['date_only'] >= '2022-06-06'
                price_columns = ['open_price', 'close_price', 'high_price', 'low_price', 'premium', 'premium_low']
                for col in price_columns:
                    if col in df.columns:
                        df.loc[mask_fix_prices, col] = df.loc[mask_fix_prices, col] * 1000
                
                # Show split application
                if mask_split.any():
                    print(f"    📝 Applied 20-for-1 split to strikes before June 6, 2022")
                if mask_fix_prices.any():
                    print(f"    📝 Multiplied prices by 1000 for dates after June 6, 2022")
                
                # Recalculate OTM and premium yields after splits
                df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
                df['premium_yield_pct'] = ((df['close_price'] / df['strike']) * 100).round(2)
                df['premium_yield_pct_low'] = ((df['open_price'] / df['strike']) * 100).round(2)
                
                # Round all numeric columns to 2 decimal places
                columns_to_round = [
                    'strike', 'open_price', 'close_price', 'otm_pct', 
                    'premium', 'premium_yield_pct', 'premium_low', 
                    'premium_yield_pct_low', 'high_price', 'low_price',
                    'underlying_open', 'underlying_close', 'underlying_high', 
                    'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
                    'underlying_high_at_expiry', 'underlying_spot_at_expiry'
                ]
                
                for col in columns_to_round:
                    if col in df.columns:
                        df[col] = df[col].round(2)
                
                # Sort by date (ascending) and strike (ascending)
                df = df.sort_values(['date_only', 'strike'], ascending=[True, True])
                df['date_only'] = df['date_only'].dt.strftime('%Y-%m-%d')
                
                # Save data with corrected prices
                df.to_csv(file_path, index=False)
                print(f"    ✅ Fixed and sorted {len(df)} records in {file_path.name}")
                return True
            else:
                print(f"    ℹ️  Skipping {file_path.name} (not AMZN data)")
                return True
        else:
            print(f"    ⚠️  Empty or invalid file: {file_path.name}")
            return False
            
    except Exception as e:
        print(f"    ❌ Error processing {file_path}: {e}")
        return False

def process_amzn_files():
    """Process all AMZN CSV files with price fixes."""
    print("🔧 Fixing AMZN prices (multiply prices by 1000 for dates after June 6, 2022)...")
    print("🎯 Split History:")
    print("   June 2, 1998: 2-for-1")
    print("   January 5, 1999: 3-for-1") 
    print("   September 2, 1999: 2-for-1")
    print("   June 6, 2022: 20-for-1 (ONLY BEFORE June 6, 2022)")
    
    amzn_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN")
    
    if not amzn_dir.exists():
        print(f"  ❌ AMZN directory not found: {amzn_dir}")
        return
    
    total_files = 0
    successful_files = 0
    
    # Process monthly files
    monthly_dir = amzn_dir / 'monthly'
    if monthly_dir.exists():
        print(f"\n  📁 Processing monthly files...")
        for csv_file in monthly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = fix_amzn_prices_final_factor(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = amzn_dir / 'weekly'
    if weekly_dir.exists():
        print(f"\n  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = fix_amzn_prices_final_factor(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"\n🎉 === AMZN FINAL FACTOR FIX SUMMARY ===")
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully processed: {successful_files}")
    print(f"❌ Failed: {total_files - successful_files}")
    print("\n✅ All AMZN prices fixed with correct factor and sorted!")

def main():
    """Main processing function."""
    process_amzn_files()

if __name__ == "__main__":
    main()
