#!/usr/bin/env python3
"""
Remove all PUT options from all ticker data folders, keeping only CALL options.
"""

import pandas as pd
import os
from pathlib import Path

# List of all tickers we processed
TICKERS = [
    'AAPL', 'AMZN', 'MSFT', 'GOOG', 'NVDA', 'SPY', 'META', 
    'HOOD', 'IWM', 'JPM', 'XLE', 'XLF', 'XLK', 'QQQ'
]

def remove_puts_from_file(file_path):
    """Remove PUT options from a CSV file, keeping only CALLs."""
    try:
        df = pd.read_csv(file_path)
        
        # Count original rows
        original_count = len(df)
        
        # Filter to keep only CALL options
        df_calls_only = df[df['option_type'] == 'C'].copy()
        
        # Count removed rows
        removed_count = original_count - len(df_calls_only)
        
        # Save the filtered data
        df_calls_only.to_csv(file_path, index=False)
        
        return original_count, len(df_calls_only), removed_count
        
    except Exception as e:
        print(f"    ❌ Error processing {file_path}: {e}")
        return 0, 0, 0

def process_ticker(ticker):
    """Process all CSV files for a ticker."""
    print(f"\n=== Processing {ticker} ===")
    
    ticker_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}")
    
    if not ticker_dir.exists():
        print(f"  ⚠️  Directory {ticker_dir} not found, skipping...")
        return 0, 0, 0
    
    total_original = 0
    total_remaining = 0
    total_removed = 0
    
    # Process monthly files
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        print(f"  📁 Processing monthly files...")
        for csv_file in monthly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            orig, remain, removed = remove_puts_from_file(csv_file)
            total_original += orig
            total_remaining += remain
            total_removed += removed
            print(f"      🗑️  Removed {removed:,} PUT options, kept {remain:,} CALL options")
    else:
        print(f"  ⚠️  Monthly directory not found for {ticker}")
    
    # Process weekly files
    weekly_dir = ticker_dir / 'weekly'
    if weekly_dir.exists():
        print(f"  📁 Processing weekly files...")
        for csv_file in weekly_dir.glob('*.csv'):
            print(f"    📄 {csv_file.name}")
            orig, remain, removed = remove_puts_from_file(csv_file)
            total_original += orig
            total_remaining += remain
            total_removed += removed
            print(f"      🗑️  Removed {removed:,} PUT options, kept {remain:,} CALL options")
    else:
        print(f"  ⚠️  Weekly directory not found for {ticker}")
    
    if total_original > 0:
        print(f"  📊 {ticker} Summary: {total_original:,} total → {total_remaining:,} remaining (removed {total_removed:,} PUTs)")
    
    return total_original, total_remaining, total_removed

def main():
    """Main processing function."""
    print("🚀 Starting PUT removal process for all tickers...")
    print("🎯 Goal: Keep only CALL options for backtesting")
    
    grand_total_original = 0
    grand_total_remaining = 0
    grand_total_removed = 0
    
    for ticker in TICKERS:
        total_original, total_remaining, total_removed = process_ticker(ticker)
        grand_total_original += total_original
        grand_total_remaining += total_remaining
        grand_total_removed += total_removed
    
    print(f"\n🎉 === FINAL SUMMARY ===")
    print(f"📊 Total rows processed: {grand_total_original:,}")
    print(f"✅ Total CALL options remaining: {grand_total_remaining:,}")
    print(f"🗑️  Total PUT options removed: {grand_total_removed:,}")
    print(f"📈 PUT removal rate: {(grand_total_removed/grand_total_original)*100:.1f}%")
    print("\n✅ All ticker data cleaned and ready for next processing step!")

if __name__ == "__main__":
    main()
