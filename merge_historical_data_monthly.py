#!/usr/bin/env python3
"""
Merge historical stock data with all monthly options CSV files for all tickers.
Updates underlying prices from date_only and expiration_date.
"""

import pandas as pd
from pathlib import Path
import glob
import sys

def load_historical_stock_prices(csv_path: str) -> pd.DataFrame:
    """
    Load historical stock prices from a CSV file.
    
    Expected CSV format:
    - Date column in MM/DD/YYYY format
    - Close/Last, Open, High, Low columns with $ prefix
    - Volume column
    """
    df = pd.read_csv(csv_path)
    
    # Parse date column (assuming MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date
    
    # Extract numeric values from price columns (remove $ and commas)
    def clean_price(value):
        if pd.isna(value):
            return None
        str_val = str(value).replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except ValueError:
            return None
    
    # Clean price columns
    df['close'] = df['Close/Last'].apply(clean_price)
    df['open'] = df['Open'].apply(clean_price)
    df['high'] = df['High'].apply(clean_price)
    df['low'] = df['Low'].apply(clean_price)
    
    # Select and return only the columns we need
    result = df[['date', 'open', 'high', 'low', 'close']].copy()
    
    # Sort by date (ascending - oldest first)
    result = result.sort_values('date').reset_index(drop=True)
    
    return result

def merge_historical_data_for_file(csv_file: Path, stock_csv_path: Path, use_pessimistic: bool = True):
    """
    Merge historical stock data with an options CSV file.
    """
    try:
        # Load options data
        df = pd.read_csv(csv_file)
        
        if len(df) <= 1:  # Only header or empty
            return False
        
        # Load historical stock prices
        stock_prices = load_historical_stock_prices(str(stock_csv_path))
        
        if stock_prices.empty:
            print(f"   ⚠️  No stock price data found in {stock_csv_path.name}")
            return False
        
        # Convert date_only to date for joining
        df['date_only_date'] = pd.to_datetime(df['date_only']).dt.date
        
        # FIRST: Clean up ALL duplicate expiration columns (with _x, _y suffixes from previous merges)
        # Drop ALL variations of expiration price columns
        exp_cols_to_drop = [col for col in df.columns if (
            col.startswith('underlying_close_at_expiry') or 
            col.startswith('underlying_high_at_expiry') or 
            col == 'underlying_spot_at_expiry'
        )]
        df = df.drop(columns=exp_cols_to_drop, errors='ignore')
        
        # SECOND: Drop entry date underlying price columns (we'll recreate them)
        entry_cols_to_drop = [col for col in df.columns if (
            col == 'underlying_open' or 
            col == 'underlying_close' or 
            col == 'underlying_high' or 
            col == 'underlying_low' or 
            col == 'underlying_spot'
        )]
        df = df.drop(columns=entry_cols_to_drop, errors='ignore')
        
        # Create a lookup dictionary from stock_prices for date_only
        stock_price_dict_date = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
        
        # Map underlying prices from date_only using the lookup dictionary
        def get_date_price(row, price_type):
            date_val = row['date_only_date']
            if date_val in stock_price_dict_date:
                return stock_price_dict_date[date_val][price_type]
            return None
        
        df['underlying_open'] = df.apply(lambda row: get_date_price(row, 'open'), axis=1)
        df['underlying_close'] = df.apply(lambda row: get_date_price(row, 'close'), axis=1)
        df['underlying_high'] = df.apply(lambda row: get_date_price(row, 'high'), axis=1)
        df['underlying_low'] = df.apply(lambda row: get_date_price(row, 'low'), axis=1)
        
        # Set underlying_spot (pessimistic = high, otherwise close)
        if use_pessimistic:
            df['underlying_spot'] = df['underlying_high']
        else:
            df['underlying_spot'] = df['underlying_close']
        
        # Drop temporary column
        df = df.drop(columns=['date_only_date'], errors='ignore')
        
        # Add expiration day prices using map (to avoid merge duplicate column issues)
        if 'expiration_date' in df.columns:
            df['expiration_date_date'] = pd.to_datetime(df['expiration_date']).dt.date
            
            # Create a lookup dictionary from stock_prices
            stock_price_dict = stock_prices.set_index('date')[['close', 'high']].to_dict('index')
            
            # Map expiration prices using the lookup dictionary
            # (Note: expiration columns were already dropped at the start)
            def get_exp_price(row, price_type):
                exp_date = row['expiration_date_date']
                if exp_date in stock_price_dict:
                    return stock_price_dict[exp_date][price_type]
                return None
            
            df['underlying_close_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
            df['underlying_high_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
            
            # Set underlying_spot_at_expiry (pessimistic = high, otherwise close)
            if use_pessimistic:
                df['underlying_spot_at_expiry'] = df['underlying_high_at_expiry']
            else:
                df['underlying_spot_at_expiry'] = df['underlying_close_at_expiry']
            
            # Drop temporary column
            df = df.drop(columns=['expiration_date_date'], errors='ignore')
        
        # Save the updated file
        df.to_csv(csv_file, index=False)
        
        # Count how many rows have underlying prices
        has_spot = df['underlying_spot'].notna().sum() if 'underlying_spot' in df.columns else 0
        
        return True, has_spot, len(df)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, 0

def process_ticker(ticker_symbol: str, data_dir: Path):
    """
    Process all monthly files for a ticker.
    """
    ticker_dir = data_dir / ticker_symbol
    monthly_dir = ticker_dir / 'monthly'
    
    if not monthly_dir.exists():
        print(f"   ⚠️  No monthly directory found for {ticker_symbol}")
        return 0, 0
    
    # Find historical stock data file
    stock_file = ticker_dir / f"HistoricalData_{ticker_symbol}.csv"
    
    # Handle special case for TSLA
    if ticker_symbol == 'TSLA' and not stock_file.exists():
        tsla_files = list(ticker_dir.glob("HistoricalData*.csv"))
        if tsla_files:
            stock_file = tsla_files[0]
    
    if not stock_file.exists():
        print(f"   ⚠️  Historical stock data not found for {ticker_symbol}")
        return 0, 0
    
    # Find all monthly CSV files
    monthly_files = list(monthly_dir.glob('*.csv'))
    
    if not monthly_files:
        return 0, 0
    
    print(f"   Processing {len(monthly_files)} monthly files...")
    
    # Check if files are pessimistic (for use_pessimistic flag)
    use_pessimistic = 'pessimistic' in monthly_files[0].name.lower()
    
    successful = 0
    total_rows = 0
    total_rows_with_data = 0
    
    for csv_file in sorted(monthly_files):
        try:
            success, rows_with_data, total = merge_historical_data_for_file(
                csv_file, stock_file, use_pessimistic=use_pessimistic
            )
            
            if success:
                successful += 1
                total_rows += total
                total_rows_with_data += rows_with_data
                print(f"     ✓ {csv_file.name}: {rows_with_data:,}/{total:,} rows with underlying prices")
            else:
                print(f"     ⚠️  {csv_file.name}: Failed to merge")
                
        except Exception as e:
            print(f"     ❌ {csv_file.name}: {e}")
            continue
    
    return successful, total_rows

def main():
    """
    Main function to process all tickers.
    """
    data_dir = Path('python-boilerplate/data')
    
    # Get all ticker directories
    ticker_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    if not ticker_dirs:
        print("❌ No ticker directories found!")
        return
    
    print("=" * 80)
    print("MERGING HISTORICAL STOCK DATA WITH MONTHLY OPTIONS FILES")
    print("=" * 80)
    print(f"Found {len(ticker_dirs)} ticker directories")
    print()
    
    total_tickers = 0
    total_files = 0
    total_rows = 0
    
    for ticker_dir in ticker_dirs:
        ticker_symbol = ticker_dir.name
        print(f"Processing {ticker_symbol}...")
        
        successful_files, rows = process_ticker(ticker_symbol, data_dir)
        
        if successful_files > 0:
            total_tickers += 1
            total_files += successful_files
            total_rows += rows
            print(f"   ✅ {ticker_symbol}: {successful_files} files, {rows:,} rows")
        else:
            print(f"   ⚠️  {ticker_symbol}: No files processed")
        
        print()
    
    print("=" * 80)
    print("✅ COMPLETED")
    print("=" * 80)
    print(f"Tickers processed: {total_tickers}")
    print(f"Total files updated: {total_files}")
    print(f"Total rows: {total_rows:,}")
    print("=" * 80)

if __name__ == "__main__":
    main()


Merge historical stock data with all monthly options CSV files for all tickers.
Updates underlying prices from date_only and expiration_date.
"""

import pandas as pd
from pathlib import Path
import glob
import sys

def load_historical_stock_prices(csv_path: str) -> pd.DataFrame:
    """
    Load historical stock prices from a CSV file.
    
    Expected CSV format:
    - Date column in MM/DD/YYYY format
    - Close/Last, Open, High, Low columns with $ prefix
    - Volume column
    """
    df = pd.read_csv(csv_path)
    
    # Parse date column (assuming MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date
    
    # Extract numeric values from price columns (remove $ and commas)
    def clean_price(value):
        if pd.isna(value):
            return None
        str_val = str(value).replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except ValueError:
            return None
    
    # Clean price columns
    df['close'] = df['Close/Last'].apply(clean_price)
    df['open'] = df['Open'].apply(clean_price)
    df['high'] = df['High'].apply(clean_price)
    df['low'] = df['Low'].apply(clean_price)
    
    # Select and return only the columns we need
    result = df[['date', 'open', 'high', 'low', 'close']].copy()
    
    # Sort by date (ascending - oldest first)
    result = result.sort_values('date').reset_index(drop=True)
    
    return result

def merge_historical_data_for_file(csv_file: Path, stock_csv_path: Path, use_pessimistic: bool = True):
    """
    Merge historical stock data with an options CSV file.
    """
    try:
        # Load options data
        df = pd.read_csv(csv_file)
        
        if len(df) <= 1:  # Only header or empty
            return False
        
        # Load historical stock prices
        stock_prices = load_historical_stock_prices(str(stock_csv_path))
        
        if stock_prices.empty:
            print(f"   ⚠️  No stock price data found in {stock_csv_path.name}")
            return False
        
        # Convert date_only to date for joining
        df['date_only_date'] = pd.to_datetime(df['date_only']).dt.date
        
        # FIRST: Clean up ALL duplicate expiration columns (with _x, _y suffixes from previous merges)
        # Drop ALL variations of expiration price columns
        exp_cols_to_drop = [col for col in df.columns if (
            col.startswith('underlying_close_at_expiry') or 
            col.startswith('underlying_high_at_expiry') or 
            col == 'underlying_spot_at_expiry'
        )]
        df = df.drop(columns=exp_cols_to_drop, errors='ignore')
        
        # SECOND: Drop entry date underlying price columns (we'll recreate them)
        entry_cols_to_drop = [col for col in df.columns if (
            col == 'underlying_open' or 
            col == 'underlying_close' or 
            col == 'underlying_high' or 
            col == 'underlying_low' or 
            col == 'underlying_spot'
        )]
        df = df.drop(columns=entry_cols_to_drop, errors='ignore')
        
        # Create a lookup dictionary from stock_prices for date_only
        stock_price_dict_date = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
        
        # Map underlying prices from date_only using the lookup dictionary
        def get_date_price(row, price_type):
            date_val = row['date_only_date']
            if date_val in stock_price_dict_date:
                return stock_price_dict_date[date_val][price_type]
            return None
        
        df['underlying_open'] = df.apply(lambda row: get_date_price(row, 'open'), axis=1)
        df['underlying_close'] = df.apply(lambda row: get_date_price(row, 'close'), axis=1)
        df['underlying_high'] = df.apply(lambda row: get_date_price(row, 'high'), axis=1)
        df['underlying_low'] = df.apply(lambda row: get_date_price(row, 'low'), axis=1)
        
        # Set underlying_spot (pessimistic = high, otherwise close)
        if use_pessimistic:
            df['underlying_spot'] = df['underlying_high']
        else:
            df['underlying_spot'] = df['underlying_close']
        
        # Drop temporary column
        df = df.drop(columns=['date_only_date'], errors='ignore')
        
        # Add expiration day prices using map (to avoid merge duplicate column issues)
        if 'expiration_date' in df.columns:
            df['expiration_date_date'] = pd.to_datetime(df['expiration_date']).dt.date
            
            # Create a lookup dictionary from stock_prices
            stock_price_dict = stock_prices.set_index('date')[['close', 'high']].to_dict('index')
            
            # Map expiration prices using the lookup dictionary
            # (Note: expiration columns were already dropped at the start)
            def get_exp_price(row, price_type):
                exp_date = row['expiration_date_date']
                if exp_date in stock_price_dict:
                    return stock_price_dict[exp_date][price_type]
                return None
            
            df['underlying_close_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
            df['underlying_high_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
            
            # Set underlying_spot_at_expiry (pessimistic = high, otherwise close)
            if use_pessimistic:
                df['underlying_spot_at_expiry'] = df['underlying_high_at_expiry']
            else:
                df['underlying_spot_at_expiry'] = df['underlying_close_at_expiry']
            
            # Drop temporary column
            df = df.drop(columns=['expiration_date_date'], errors='ignore')
        
        # Save the updated file
        df.to_csv(csv_file, index=False)
        
        # Count how many rows have underlying prices
        has_spot = df['underlying_spot'].notna().sum() if 'underlying_spot' in df.columns else 0
        
        return True, has_spot, len(df)
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, 0

def process_ticker(ticker_symbol: str, data_dir: Path):
    """
    Process all monthly files for a ticker.
    """
    ticker_dir = data_dir / ticker_symbol
    monthly_dir = ticker_dir / 'monthly'
    
    if not monthly_dir.exists():
        print(f"   ⚠️  No monthly directory found for {ticker_symbol}")
        return 0, 0
    
    # Find historical stock data file
    stock_file = ticker_dir / f"HistoricalData_{ticker_symbol}.csv"
    
    # Handle special case for TSLA
    if ticker_symbol == 'TSLA' and not stock_file.exists():
        tsla_files = list(ticker_dir.glob("HistoricalData*.csv"))
        if tsla_files:
            stock_file = tsla_files[0]
    
    if not stock_file.exists():
        print(f"   ⚠️  Historical stock data not found for {ticker_symbol}")
        return 0, 0
    
    # Find all monthly CSV files
    monthly_files = list(monthly_dir.glob('*.csv'))
    
    if not monthly_files:
        return 0, 0
    
    print(f"   Processing {len(monthly_files)} monthly files...")
    
    # Check if files are pessimistic (for use_pessimistic flag)
    use_pessimistic = 'pessimistic' in monthly_files[0].name.lower()
    
    successful = 0
    total_rows = 0
    total_rows_with_data = 0
    
    for csv_file in sorted(monthly_files):
        try:
            success, rows_with_data, total = merge_historical_data_for_file(
                csv_file, stock_file, use_pessimistic=use_pessimistic
            )
            
            if success:
                successful += 1
                total_rows += total
                total_rows_with_data += rows_with_data
                print(f"     ✓ {csv_file.name}: {rows_with_data:,}/{total:,} rows with underlying prices")
            else:
                print(f"     ⚠️  {csv_file.name}: Failed to merge")
                
        except Exception as e:
            print(f"     ❌ {csv_file.name}: {e}")
            continue
    
    return successful, total_rows

def main():
    """
    Main function to process all tickers.
    """
    data_dir = Path('python-boilerplate/data')
    
    # Get all ticker directories
    ticker_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    if not ticker_dirs:
        print("❌ No ticker directories found!")
        return
    
    print("=" * 80)
    print("MERGING HISTORICAL STOCK DATA WITH MONTHLY OPTIONS FILES")
    print("=" * 80)
    print(f"Found {len(ticker_dirs)} ticker directories")
    print()
    
    total_tickers = 0
    total_files = 0
    total_rows = 0
    
    for ticker_dir in ticker_dirs:
        ticker_symbol = ticker_dir.name
        print(f"Processing {ticker_symbol}...")
        
        successful_files, rows = process_ticker(ticker_symbol, data_dir)
        
        if successful_files > 0:
            total_tickers += 1
            total_files += successful_files
            total_rows += rows
            print(f"   ✅ {ticker_symbol}: {successful_files} files, {rows:,} rows")
        else:
            print(f"   ⚠️  {ticker_symbol}: No files processed")
        
        print()
    
    print("=" * 80)
    print("✅ COMPLETED")
    print("=" * 80)
    print(f"Tickers processed: {total_tickers}")
    print(f"Total files updated: {total_files}")
    print(f"Total rows: {total_rows:,}")
    print("=" * 80)

if __name__ == "__main__":
    main()

