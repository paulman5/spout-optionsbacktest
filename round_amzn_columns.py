#!/usr/bin/env python3
"""
Round all numeric columns to 2 decimal places maximum for AMZN data.
"""

import pandas as pd
from pathlib import Path

def round_amzn_columns(file_path):
    """Round all numeric columns to 2 decimal places maximum."""
    try:
        df = pd.read_csv(file_path)
        
        if not df.empty and 'ticker' in df.columns:
            sample_ticker = df['ticker'].iloc[0]
            if 'AMZN' in sample_ticker:
                print(f"    🔧 Rounding AMZN numeric columns in {file_path.name}")
                
                # Columns to round to 2 decimal places
                columns_to_round = [
                    'strike', 'open_price', 'close_price', 'otm_pct', 
                    'premium', 'premium_yield_pct', 'premium_low', 
                    'premium_yield_pct_low', 'high_price', 'low_price',
                    'underlying_open', 'underlying_close', 'underlying_high', 
                    'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
                    'underlying_high_at_expiry', 'underlying_spot_at_expiry'
                ]
                
                # Round each column to 2 decimal places
                for col in columns_to_round:
                    if col in df.columns:
                        df[col] = df[col].round(2)
                        print(f"    📝 Rounded {col} to 2 decimal places")
                
                # Save data with rounded columns
                df.to_csv(file_path, index=False)
                print(f"    ✅ Rounded {len(df)} records in {file_path.name}")
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
    """Process all AMZN CSV files with rounding."""
    print("🔧 Rounding AMZN numeric columns to 2 decimal places...")
    print("🎯 Columns to round: strike, open_price, close_price, otm_pct, premium, premium_yield_pct, premium_low, premium_yield_pct_low, high_price, low_price, underlying_open, underlying_close, underlying_high, underlying_low, underlying_spot, underlying_close_at_expiry, underlying_high_at_expiry, underlying_spot_at_expiry")
    
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
            success = round_amzn_columns(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = amzn_dir / 'weekly'
    if weekly_dir.exists():
        print(f"\n  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = round_amzn_columns(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"\n🎉 === AMZN ROUNDING SUMMARY ===")
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully processed: {successful_files}")
    print(f"❌ Failed: {total_files - successful_files}")
    print("\n✅ All AMZN numeric columns rounded to 2 decimal places!")

def main():
    """Main processing function."""
    process_amzn_files()

if __name__ == "__main__":
    main()
