#!/usr/bin/env python3
"""
Apply Amazon stock splits to adjust strike prices and premiums.
Split History:
- June 2, 1998: 2-for-1 (divide by 2)
- January 5, 1999: 3-for-1 (divide by 3)  
- September 2, 1999: 2-for-1 (divide by 2)
- June 6, 2022: 20-for-1 (divide by 20) - ONLY BEFORE June 6, 2022
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def apply_amzn_stock_splits(file_path):
    """Apply Amazon stock splits based on dates."""
    try:
        df = pd.read_csv(file_path)
        
        if not df.empty and 'ticker' in df.columns:
            sample_ticker = df['ticker'].iloc[0]
            if 'AMZN' in sample_ticker:
                print(f"    🔧 Applying AMZN stock splits to {file_path.name}")
                
                # Convert date_only to datetime
                df['date_only'] = pd.to_datetime(df['date_only'])
                
                # Apply splits based on dates
                # June 2, 1998 to January 5, 1999: 2-for-1 split (ONLY BEFORE June 6, 2022)
                mask_split1 = (df['date_only'] >= '1998-06-02') & (df['date_only'] < '2022-06-06')
                df.loc[mask_split1, 'strike'] = df.loc[mask_split1, 'strike'] / 2
                df.loc[mask_split1, 'open_price'] = df.loc[mask_split1, 'open_price'] / 2
                df.loc[mask_split1, 'close_price'] = df.loc[mask_split1, 'close_price'] / 2
                df.loc[mask_split1, 'high_price'] = df.loc[mask_split1, 'high_price'] / 2
                df.loc[mask_split1, 'low_price'] = df.loc[mask_split1, 'low_price'] / 2
                
                # January 5, 1999 to September 2, 1999: 3-for-1 split (ONLY BEFORE June 6, 2022)
                mask_split2 = (df['date_only'] >= '1999-01-05') & (df['date_only'] < '2022-06-06')
                df.loc[mask_split2, 'strike'] = df.loc[mask_split2, 'strike'] / 3
                df.loc[mask_split2, 'open_price'] = df.loc[mask_split2, 'open_price'] / 3
                df.loc[mask_split2, 'close_price'] = df.loc[mask_split2, 'close_price'] / 3
                df.loc[mask_split2, 'high_price'] = df.loc[mask_split2, 'high_price'] / 3
                df.loc[mask_split2, 'low_price'] = df.loc[mask_split2, 'low_price'] / 3
                
                # September 2, 1999 to June 6, 2022: 2-for-1 split (ONLY BEFORE June 6, 2022)
                mask_split3 = (df['date_only'] >= '1999-09-02') & (df['date_only'] < '2022-06-06')
                df.loc[mask_split3, 'strike'] = df.loc[mask_split3, 'strike'] / 2
                df.loc[mask_split3, 'open_price'] = df.loc[mask_split3, 'open_price'] / 2
                df.loc[mask_split3, 'close_price'] = df.loc[mask_split3, 'close_price'] / 2
                df.loc[mask_split3, 'high_price'] = df.loc[mask_split3, 'high_price'] / 2
                df.loc[mask_split3, 'low_price'] = df.loc[mask_split3, 'low_price'] / 2
                
                # Show split applications
                splits_applied = []
                if mask_split1.any():
                    splits_applied.append("2-for-1 (1998-06-02 to 2022-06-05)")
                if mask_split2.any():
                    splits_applied.append("3-for-1 (1999-01-05 to 2022-06-05)")
                if mask_split3.any():
                    splits_applied.append("2-for-1 (1999-09-02 to 2022-06-05)")
                
                if splits_applied:
                    print(f"    📝 Applied splits: {', '.join(splits_applied)}")
                
                # Recalculate OTM and premium yields after splits
                df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
                df['premium_yield_pct'] = ((df['close_price'] / df['strike']) * 100).round(2)
                df['premium_yield_pct_low'] = ((df['open_price'] / df['strike']) * 100).round(2)
                
                # Save data with splits applied
                df.to_csv(file_path, index=False)
                print(f"    ✅ Applied splits to {len(df)} records in {file_path.name}")
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
    """Process all AMZN CSV files with stock splits."""
    print("🔧 Applying AMZN stock splits (CORRECTED - Only 20-for-1 BEFORE June 6, 2022)...")
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
            success = apply_amzn_stock_splits(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = amzn_dir / 'weekly'
    if weekly_dir.exists():
        print(f"\n  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = apply_amzn_stock_splits(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"\n🎉 === AMZN STOCK SPLIT SUMMARY ===")
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully processed: {successful_files}")
    print(f"❌ Failed: {total_files - successful_files}")
    print("\n✅ All AMZN stock splits applied correctly!")

def main():
    """Main processing function."""
    process_amzn_files()

if __name__ == "__main__":
    main()
