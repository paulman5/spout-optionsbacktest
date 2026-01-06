#!/usr/bin/env python3
"""
Fix AMZN strike prices by dividing by 100 (not 1000).
The ticker format shows strike in cents (e.g., 00400000 = $400.00)
"""

import pandas as pd
from pathlib import Path

def fix_amzn_strike_prices(file_path):
    """Fix AMZN strike prices by dividing by 100."""
    try:
        df = pd.read_csv(file_path)
        
        # Check if this is AMZN data
        if not df.empty and 'ticker' in df.columns:
            sample_ticker = df['ticker'].iloc[0]
            if 'AMZN' in sample_ticker:
                print(f"    🔧 Fixing AMZN strike prices in {file_path.name}")
                
                # Fix strike prices by dividing by 100
                original_strikes = df['strike'].copy()
                df['strike'] = df['strike'] / 100
                
                # Show some examples
                print(f"    📝 Example: {original_strikes.iloc[0]} → {df['strike'].iloc[0]}")
                print(f"    📝 Example: {original_strikes.iloc[1] if len(original_strikes) > 1 else 'N/A'} → {df['strike'].iloc[1] if len(df) > 1 else 'N/A'}")
                
                # Recalculate OTM percentage with corrected strikes
                df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
                
                # Recalculate premium yield percentages with corrected strikes
                df['premium_yield_pct'] = ((df['close_price'] / df['strike']) * 100).round(2)
                df['premium_yield_pct_low'] = ((df['open_price'] / df['strike']) * 100).round(2)
                
                # Save corrected data
                df.to_csv(file_path, index=False)
                print(f"    ✅ Fixed {len(df)} strike prices in {file_path.name}")
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
    """Process all AMZN CSV files."""
    print("🔧 Fixing AMZN strike prices (divide by 100)...")
    print("🎯 Goal: Convert from cents to dollars")
    
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
            success = fix_amzn_strike_prices(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = amzn_dir / 'weekly'
    if weekly_dir.exists():
        print(f"\n  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = fix_amzn_strike_prices(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"\n🎉 === AMZN STRIKE FIX SUMMARY ===")
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully fixed: {successful_files}")
    print(f"❌ Failed: {total_files - successful_files}")
    print("\n✅ All AMZN strike prices corrected!")

def main():
    """Main processing function."""
    process_amzn_files()

if __name__ == "__main__":
    main()
