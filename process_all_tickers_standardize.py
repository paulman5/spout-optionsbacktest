#!/usr/bin/env python3
"""
Process all tickers (holidays, weekly, monthly) to ensure:
1. Historical stock data is merged
2. fedfunds_rate is added from FEDFUNDS.csv
3. All required columns are present
4. Columns are in the correct order (matching TSLA format)
"""

import pandas as pd
from pathlib import Path
import glob
import sys
from datetime import datetime

# Add the backtesting module to path
sys.path.insert(0, str(Path('python-boilerplate/src/backtesting')))
sys.path.insert(0, str(Path('.')))

from merge_historical_data_monthly import load_historical_stock_prices, merge_historical_data_for_file
from add_premium_columns import add_premium_columns

def load_fedfunds_data():
    """Load FEDFUNDS data and create a lookup dictionary."""
    fedfunds_path = Path('FEDFUNDS.csv')
    if not fedfunds_path.exists():
        print(f"⚠️  FEDFUNDS.csv not found at {fedfunds_path}")
        return {}
    
    df = pd.read_csv(fedfunds_path)
    df['observation_date'] = pd.to_datetime(df['observation_date']).dt.date
    
    # Create lookup: year-month -> fedfunds_rate
    lookup = {}
    for _, row in df.iterrows():
        date = row['observation_date']
        year_month = (date.year, date.month)
        lookup[year_month] = row['FEDFUNDS']
    
    return lookup

def add_fedfunds_rate(df, fedfunds_lookup):
    """Add fedfunds_rate column based on date_only."""
    df = df.copy()
    
    def get_fedfunds_rate(row):
        try:
            date = pd.to_datetime(row['date_only']).date()
            year_month = (date.year, date.month)
            return fedfunds_lookup.get(year_month, 0.02)  # Default to 0.02 if not found
        except:
            return 0.02
    
    df['fedfunds_rate'] = df.apply(get_fedfunds_rate, axis=1)
    return df

def ensure_required_columns(df):
    """Ensure all required columns are present."""
    df = df.copy()
    
    # Calculate mid_price if missing
    if 'mid_price' not in df.columns:
        if 'high_price' in df.columns and 'low_price' in df.columns:
            df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
        elif 'close_price' in df.columns:
            df['mid_price'] = df['close_price']
        else:
            df['mid_price'] = None
    
    # Calculate high_yield_pct if missing
    if 'high_yield_pct' not in df.columns:
        if 'high_price' in df.columns and 'underlying_spot' in df.columns:
            df['high_yield_pct'] = (df['high_price'] / df['underlying_spot'] * 100).round(2)
        else:
            df['high_yield_pct'] = None
    
    return df

def reorder_columns(df):
    """Reorder columns to match TSLA format."""
    # Expected column order from TSLA
    expected_order = [
        'ticker',
        'date_only',
        'expiration_date',
        'underlying_symbol',
        'option_type',
        'strike',
        'volume',
        'open_price',
        'close_price',
        'otm_pct',
        'ITM',
        'premium',
        'premium_yield_pct',
        'premium_low',
        'premium_yield_pct_low',
        'high_price',
        'low_price',
        'transactions',
        'window_start',
        'days_to_expiry',
        'time_remaining_category',
        'underlying_open',
        'underlying_close',
        'underlying_high',
        'underlying_low',
        'underlying_spot',
        'underlying_close_at_expiry',
        'underlying_high_at_expiry',
        'underlying_spot_at_expiry',
        'fedfunds_rate',
        'implied_volatility',
        'probability_itm',
        'mid_price',
        'high_yield_pct'
    ]
    
    # Get existing columns in expected order
    ordered_cols = [col for col in expected_order if col in df.columns]
    
    # Add any remaining columns that aren't in expected order
    remaining_cols = [col for col in df.columns if col not in ordered_cols]
    ordered_cols.extend(remaining_cols)
    
    return df[ordered_cols]

def process_file(csv_file: Path, stock_csv_path: Path, fedfunds_lookup: dict, use_pessimistic: bool = True):
    """Process a single CSV file."""
    try:
        # Load the file
        df = pd.read_csv(csv_file)
        
        if len(df) <= 1:  # Only header or empty
            return False, 0, 0
        
        # Merge historical stock data (this saves the file)
        success, rows_with_data, total = merge_historical_data_for_file(csv_file, stock_csv_path, use_pessimistic)
        
        if not success:
            return False, 0, 0
        
        # Reload after merge
        df = pd.read_csv(csv_file)
        
        # Add premium columns if missing
        if 'premium' not in df.columns or 'otm_pct' not in df.columns:
            df = add_premium_columns(df)
        
        # Ensure required columns
        df = ensure_required_columns(df)
        
        # Add fedfunds_rate if missing
        if 'fedfunds_rate' not in df.columns:
            df = add_fedfunds_rate(df, fedfunds_lookup)
        
        # Reorder columns
        df = reorder_columns(df)
        
        # Save the file
        df.to_csv(csv_file, index=False)
        
        return True, rows_with_data, total
        
    except Exception as e:
        print(f"   ❌ Error processing {csv_file.name}: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, 0

def process_ticker(ticker_symbol: str, data_dir: Path, fedfunds_lookup: dict):
    """Process all files (holidays, weekly, monthly) for a ticker."""
    ticker_dir = data_dir / ticker_symbol
    
    if not ticker_dir.exists():
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
    
    # Find all CSV files in holidays, weekly, and monthly directories
    folders = ['holidays', 'weekly', 'monthly']
    all_files = []
    
    for folder in folders:
        folder_path = ticker_dir / folder
        if folder_path.exists():
            csv_files = list(folder_path.glob('*.csv'))
            all_files.extend(csv_files)
    
    if not all_files:
        return 0, 0
    
    print(f"   Processing {len(all_files)} files ({', '.join(folders)})...")
    
    # Check if files are pessimistic
    use_pessimistic = 'pessimistic' in all_files[0].name.lower() if all_files else True
    
    successful = 0
    total_rows = 0
    total_rows_with_data = 0
    
    for csv_file in sorted(all_files):
        try:
            success, rows_with_data, total = process_file(
                csv_file, stock_file, fedfunds_lookup, use_pessimistic=use_pessimistic
            )
            
            if success:
                successful += 1
                total_rows += total
                total_rows_with_data += rows_with_data
                folder_name = csv_file.parent.name
                print(f"     ✓ {folder_name}/{csv_file.name}: {rows_with_data:,}/{total:,} rows")
            else:
                print(f"     ⚠️  {csv_file.name}: Failed")
                
        except Exception as e:
            print(f"     ❌ {csv_file.name}: {e}")
            continue
    
    return successful, total_rows

def main():
    """Main function to process all tickers."""
    data_dir = Path('python-boilerplate/data')
    
    # Load FEDFUNDS data
    print("=" * 80)
    print("LOADING FEDFUNDS DATA")
    print("=" * 80)
    fedfunds_lookup = load_fedfunds_data()
    print(f"Loaded {len(fedfunds_lookup)} FEDFUNDS rate entries")
    print()
    
    # Get all ticker directories
    ticker_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    if not ticker_dirs:
        print("❌ No ticker directories found!")
        return
    
    print("=" * 80)
    print("PROCESSING ALL TICKERS (HOLIDAYS, WEEKLY, MONTHLY)")
    print("=" * 80)
    print(f"Found {len(ticker_dirs)} ticker directories")
    print()
    
    total_tickers = 0
    total_files = 0
    total_rows = 0
    
    for ticker_dir in ticker_dirs:
        ticker_symbol = ticker_dir.name
        print(f"Processing {ticker_symbol}...")
        
        successful_files, rows = process_ticker(ticker_symbol, data_dir, fedfunds_lookup)
        
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

