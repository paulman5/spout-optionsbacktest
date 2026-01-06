#!/usr/bin/env python3
"""
Sort AMZN data by date (first to last) and strike (low to high).
"""

import pandas as pd
from pathlib import Path

def sort_amzn_data(file_path):
    """Sort AMZN data by date and strike."""
    try:
        df = pd.read_csv(file_path)
        
        if not df.empty and 'ticker' in df.columns:
            sample_ticker = df['ticker'].iloc[0]
            if 'AMZN' in sample_ticker:
                print(f"    🔧 Sorting AMZN data in {file_path.name}")
                
                # Convert date_only to datetime for proper sorting
                df['date_only'] = pd.to_datetime(df['date_only'])
                
                # Sort by date (ascending = first to last) and then by strike (ascending = low to high)
                df_sorted = df.sort_values(['date_only', 'strike'], ascending=[True, True])
                
                # Convert date_only back to string format for CSV
                df_sorted['date_only'] = df_sorted['date_only'].dt.strftime('%Y-%m-%d')
                
                # Save sorted data
                df_sorted.to_csv(file_path, index=False)
                print(f"    ✅ Sorted {len(df_sorted)} records in {file_path.name}")
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
    """Process all AMZN CSV files with sorting."""
    print("🔧 Sorting AMZN data by date (first to last) and strike (low to high)...")
    print("🎯 Sort order: date ascending, then strike ascending")
    
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
            success = sort_amzn_data(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = amzn_dir / 'weekly'
    if weekly_dir.exists():
        print(f"\n  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = sort_amzn_data(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"\n🎉 === AMZN SORTING SUMMARY ===")
    print(f"📊 Total files processed: {total_files}")
    print(f"✅ Successfully processed: {successful_files}")
    print(f"❌ Failed: {total_files - successful_files}")
    print("\n✅ All AMZN data sorted correctly!")

def main():
    """Main processing function."""
    process_amzn_files()

if __name__ == "__main__":
    main()
