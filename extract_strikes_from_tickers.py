#!/usr/bin/env python3
"""
Extract strike prices from ticker symbols for ALL stocks.
Format: O:TICKERYYMMDDCXXXXXX where XXXXXX is strike in cents
Strike = XXXXXX / 1000 (convert cents to dollars)
"""

import pandas as pd
import re
from pathlib import Path

def extract_strike_from_ticker(ticker_str):
    """Extract strike price from ticker symbol."""
    # Pattern: O:TICKERYYMMDDCXXXXXX where XXXXXX is strike in cents
    match = re.search(r'C(\d+)$', ticker_str)
    if match:
        strike_cents = int(match.group(1))
        strike_dollars = strike_cents / 1000.0
        return strike_dollars
    return None

def fix_strike_prices_from_tickers(file_path):
    """Extract strike prices from ticker symbols for all stocks."""
    try:
        df = pd.read_csv(file_path)
        
        if not df.empty and 'ticker' in df.columns:
            print(f"    🔧 Processing {file_path.name}")
            
            # Extract strikes from ticker symbols
            df['strike'] = df['ticker'].apply(extract_strike_from_ticker)
            
            # Show some examples
            original_strikes = df['strike'].copy()
            print(f"    📝 Example ticker: {df['ticker'].iloc[0]}")
            print(f"    📝 Extracted strike: {df['strike'].iloc[0]}")
            if len(df) > 1:
                print(f"    📝 Example ticker: {df['ticker'].iloc[1]}")
                print(f"    📝 Extracted strike: {df['strike'].iloc[1]}")
            
            # Recalculate OTM percentage with correct strikes
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
            
            # Recalculate premium yield percentages with correct strikes
            df['premium_yield_pct'] = ((df['close_price'] / df['strike']) * 100).round(2)
            df['premium_yield_pct_low'] = ((df['open_price'] / df['strike']) * 100).round(2)
            
            # Save corrected data
            df.to_csv(file_path, index=False)
            print(f"    ✅ Fixed {len(df)} strike prices in {file_path.name}")
            return True
        else:
            print(f"    ⚠️  Empty or invalid file: {file_path.name}")
            return False
            
    except Exception as e:
        print(f"    ❌ Error processing {file_path}: {e}")
        return False

def process_all_tickers():
    """Process all ticker CSV files."""
    print("🔧 Extracting strike prices from ticker symbols for ALL stocks...")
    print("🎯 Goal: Convert cents from ticker to dollar strikes")
    
    # List of all tickers we processed
    TICKERS = [
        'AAPL', 'AMZN', 'MSFT', 'GOOG', 'NVDA', 'SPY', 'META', 
        'HOOD', 'IWM', 'JPM', 'XLE', 'XLF', 'XLK', 'QQQ'
    ]
    
    grand_total_files = 0
    grand_successful_files = 0
    
    for ticker in TICKERS:
        print(f"\n=== Processing {ticker} ===")
        ticker_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}")
        
        if not ticker_dir.exists():
            print(f"  ⚠️  Directory {ticker_dir} not found, skipping...")
            continue
        
        total_files = 0
        successful_files = 0
        
        # Process monthly files
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            print(f"  📁 Processing monthly files...")
            for csv_file in monthly_dir.glob('*.csv'):
                success = fix_strike_prices_from_tickers(csv_file)
                total_files += 1
                if success:
                    successful_files += 1
        
        # Process weekly files
        weekly_dir = ticker_dir / 'weekly'
        if weekly_dir.exists():
            print(f"  📁 Processing weekly files...")
            for csv_file in weekly_dir.glob('*.csv'):
                success = fix_strike_prices_from_tickers(csv_file)
                total_files += 1
                if success:
                    successful_files += 1
        
        print(f"  📊 {ticker} Summary: {successful_files}/{total_files} files processed successfully")
        grand_total_files += total_files
        grand_successful_files += successful_files
    
    print(f"\n🎉 === FINAL SUMMARY ===")
    print(f"📊 Total files processed: {grand_total_files}")
    print(f"✅ Successfully fixed: {grand_successful_files}")
    print(f"❌ Failed: {grand_total_files - grand_successful_files}")
    print("\n✅ All ticker strike prices extracted from symbols!")

def main():
    """Main processing function."""
    process_all_tickers()

if __name__ == "__main__":
    main()
