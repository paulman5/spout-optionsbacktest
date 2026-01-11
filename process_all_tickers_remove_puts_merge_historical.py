#!/usr/bin/env python3
"""
Process all tickers' holidays CSV files:
1. Remove all PUT options (keep only CALL options)
2. Merge with historical stock data
3. Round all numeric columns to 2 decimal places
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add the monthly.py path to import the load function
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting" / "weekly-monthly"))
from monthly import load_historical_stock_prices, add_underlying_prices_from_csv


def round_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Round all numeric columns to 2 decimal places."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype in [np.float64, np.float32, 'float64', 'float32']:
            df[col] = df[col].round(2)
    return df


def process_holidays_file(holidays_file: Path, historical_data_file: Path, ticker_name: str):
    """
    Process a single holidays CSV file:
    1. Remove PUT options
    2. Merge with historical stock data
    3. Round to 2 decimals
    
    Args:
        holidays_file: Path to the holidays CSV file
        historical_data_file: Path to the HistoricalData CSV file
        ticker_name: Ticker symbol
    """
    print(f"\n{'='*80}")
    print(f"Processing: {holidays_file.name}")
    print(f"{'='*80}")
    
    # Load holidays data
    print(f"📂 Loading holidays data...")
    try:
        df = pd.read_csv(holidays_file)
        print(f"   Loaded {len(df):,} rows")
    except Exception as e:
        print(f"   ❌ Error loading file: {e}")
        return False
    
    if len(df) == 0:
        print(f"   ⚠️  File is empty, skipping...")
        return False
    
    # Remove PUT options
    if 'option_type' in df.columns:
        initial_count = len(df)
        df = df[df['option_type'] == 'C'].copy()
        removed_count = initial_count - len(df)
        print(f"   Removed {removed_count:,} PUT options, {len(df):,} CALL options remaining")
    else:
        print(f"   ⚠️  No 'option_type' column found, skipping PUT removal")
    
    if len(df) == 0:
        print(f"   ⚠️  No CALL options remaining, skipping...")
        return False
    
    # Check if historical data file exists
    if not historical_data_file.exists():
        print(f"   ⚠️  Historical data file not found: {historical_data_file}")
        print(f"   💾 Saving file without historical data merge...")
        df = round_numeric_columns(df)
        try:
            df.to_csv(holidays_file, index=False)
            print(f"   ✅ Saved {holidays_file}")
            return True
        except Exception as e:
            print(f"   ❌ Error saving file: {e}")
            return False
    
    # Load and merge historical stock prices
    print(f"📈 Loading historical stock prices...")
    try:
        stock_prices = load_historical_stock_prices(str(historical_data_file))
        print(f"   Loaded {len(stock_prices):,} days of stock price data")
        print(f"   Date range: {stock_prices['date'].min()} to {stock_prices['date'].max()}")
        
        # Create a mapping from date to stock prices
        stock_price_map = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
        
        # Convert date_only to date for matching
        df['date_only_date'] = pd.to_datetime(df['date_only']).dt.date
        
        # Update underlying prices for trading dates using map (safer than merge)
        def get_stock_price(row, price_type):
            date = row['date_only_date']
            if date in stock_price_map:
                return stock_price_map[date].get(price_type)
            return None
        
        df['underlying_open'] = df.apply(lambda row: get_stock_price(row, 'open'), axis=1)
        df['underlying_close'] = df.apply(lambda row: get_stock_price(row, 'close'), axis=1)
        df['underlying_high'] = df.apply(lambda row: get_stock_price(row, 'high'), axis=1)
        df['underlying_low'] = df.apply(lambda row: get_stock_price(row, 'low'), axis=1)
        
        # Set underlying_spot (pessimistic = high)
        df['underlying_spot'] = df['underlying_high']
        
        # Update expiration date prices
        if 'expiration_date' in df.columns:
            df['expiration_date_date'] = pd.to_datetime(df['expiration_date']).dt.date
            
            def get_exp_price(row, price_type):
                date = row['expiration_date_date']
                if date in stock_price_map:
                    return stock_price_map[date].get(price_type)
                return None
            
            df['underlying_close_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
            df['underlying_high_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
            df['underlying_spot_at_expiry'] = df['underlying_high_at_expiry']  # pessimistic
            
            # Drop temporary column
            df = df.drop(columns=['expiration_date_date'], errors='ignore')
        
        # Drop temporary column
        df = df.drop(columns=['date_only_date'], errors='ignore')
        
        # Check how many rows have underlying prices filled
        has_spot = df['underlying_spot'].notna().sum()
        print(f"   ✅ Merged successfully: {has_spot:,} / {len(df):,} rows have underlying prices")
        
    except Exception as e:
        print(f"   ⚠️  Error merging historical data: {e}")
        import traceback
        traceback.print_exc()
        print(f"   💾 Saving file without historical data merge...")
    
    # Round all numeric columns to 2 decimal places
    print(f"🔢 Rounding numeric columns to 2 decimal places...")
    df = round_numeric_columns(df)
    
    # Save updated file
    print(f"💾 Saving updated file...")
    try:
        df.to_csv(holidays_file, index=False)
        print(f"   ✅ Saved {holidays_file}")
        return True
    except Exception as e:
        print(f"   ❌ Error saving file: {e}")
        return False


def main():
    """Main function to process all tickers."""
    base_path = Path(__file__).parent / "python-boilerplate" / "data"
    
    if not base_path.exists():
        print(f"❌ Data directory not found: {base_path}")
        return
    
    # Find all tickers with holidays directories
    ticker_dirs = []
    for ticker_dir in sorted(base_path.iterdir()):
        if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
            continue
        
        holidays_dir = ticker_dir / "holidays"
        if holidays_dir.exists():
            # Find historical data file
            historical_file = ticker_dir / f"HistoricalData_{ticker_dir.name}.csv"
            if not historical_file.exists():
                # Try to find any HistoricalData file
                historical_files = list(ticker_dir.glob("HistoricalData*.csv"))
                if historical_files:
                    historical_file = historical_files[0]
            
            ticker_dirs.append((ticker_dir.name, ticker_dir, historical_file))
    
    if not ticker_dirs:
        print(f"❌ No tickers with holidays directories found")
        return
    
    print("=" * 80)
    print(f"PROCESSING ALL TICKERS: REMOVE PUTS & MERGE HISTORICAL DATA")
    print("=" * 80)
    print(f"Found {len(ticker_dirs)} tickers with holidays directories")
    print("=" * 80)
    
    # Process each ticker
    total_files = 0
    success_count = 0
    failed_count = 0
    
    for ticker_name, ticker_dir, historical_file in ticker_dirs:
        holidays_dir = ticker_dir / "holidays"
        csv_files = sorted(holidays_dir.glob("*_options_pessimistic.csv"))
        
        if not csv_files:
            continue
        
        print(f"\n📊 {ticker_name}: {len(csv_files)} files")
        if historical_file.exists():
            print(f"   Historical data: {historical_file.name}")
        else:
            print(f"   ⚠️  No historical data file found")
        
        ticker_success = 0
        ticker_failed = 0
        
        for holidays_file in csv_files:
            total_files += 1
            if process_holidays_file(holidays_file, historical_file, ticker_name):
                success_count += 1
                ticker_success += 1
            else:
                failed_count += 1
                ticker_failed += 1
        
        if ticker_failed == 0:
            print(f"   ✅ All {ticker_success} files processed successfully")
        else:
            print(f"   ⚠️  {ticker_success} successful, {ticker_failed} failed")
    
    print("\n" + "=" * 80)
    print("✅ PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"   Total files processed: {total_files}")
    print(f"   Successfully updated: {success_count}")
    if failed_count > 0:
        print(f"   Failed: {failed_count}")
    print("=" * 80)


if __name__ == "__main__":
    main()

