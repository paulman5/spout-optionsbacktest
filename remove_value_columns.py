#!/usr/bin/env python3
"""
Remove time_value, intrinsic_value, and extrinsic_value columns from all ticker CSV files.
"""

import pandas as pd
import os
from pathlib import Path

# List of all tickers we processed
TICKERS = [
    'AAPL', 'AMZN', 'MSFT', 'GOOG', 'NVDA', 'SPY', 'META', 
    'HOOD', 'IWM', 'JPM', 'XLE', 'XLF', 'XLK', 'QQQ'
]

def remove_columns_from_file(file_path):
    """Remove specified columns from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        
        # Columns to remove
        columns_to_remove = ['time_value', 'intrinsic_value', 'extrinsic_value']
        
        # Check if columns exist and remove them
        existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        
        if existing_columns_to_remove:
            print(f"    🗑️  Removing columns: {existing_columns_to_remove}")
            df = df.drop(columns=existing_columns_to_remove)
            
            # Save the updated data
            df.to_csv(file_path, index=False)
            print(f"    ✅ Updated {file_path.name} (removed {len(existing_columns_to_remove)} columns)")
        else:
            print(f"    ℹ️  No columns to remove in {file_path.name}")
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error processing {file_path}: {e}")
        return False

def process_ticker(ticker):
    """Process all CSV files for a ticker."""
    print(f"\n=== Processing {ticker} ===")
    
    ticker_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}")
    
    if not ticker_dir.exists():
        print(f"  ⚠️  Directory {ticker_dir} not found, skipping...")
        return
    
    total_files = 0
    successful_files = 0
    
    # Process monthly files
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        print(f"  📁 Processing monthly files...")
        for csv_file in monthly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = remove_columns_from_file(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    # Process weekly files
    weekly_dir = ticker_dir / 'weekly'
    if weekly_dir.exists():
        print(f"  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            success = remove_columns_from_file(csv_file)
            total_files += 1
            if success:
                successful_files += 1
    
    print(f"  📊 {ticker} Summary: {successful_files}/{total_files} files processed successfully")
    
    return successful_files, total_files

def main():
    """Main processing function."""
    print("🚀 Removing time_value, intrinsic_value, and extrinsic_value columns from all ticker data...")
    print("🎯 Goal: Clean up column structure across all tickers")
    
    grand_total_files = 0
    grand_successful_files = 0
    
    for ticker in TICKERS:
        successful_files, total_files = process_ticker(ticker)
        grand_successful_files += successful_files
        grand_total_files += total_files
    
    print(f"\n🎉 === FINAL SUMMARY ===")
    print(f"📊 Total files processed: {grand_total_files}")
    print(f"✅ Successfully updated: {grand_successful_files}")
    print(f"❌ Failed: {grand_total_files - grand_successful_files}")
    print("\n✅ All ticker data columns cleaned successfully!")

if __name__ == "__main__":
    main()
