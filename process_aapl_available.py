#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


def process_aapl_data():
    """Process available AAPL data with historical stock prices"""
    
    # Stock price file path
    stock_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/HistoricalData_AAPL.csv")
    
    print("📈 Loading stock price data...")
    stock_df = pd.read_csv(stock_file)
    print(f"   Loaded {len(stock_df):,} rows")
    
    # Clean stock price data (remove $ signs and convert to float)
    for col in ['Close/Last', 'Open', 'High', 'Low']:
        stock_df[col] = stock_df[col].astype(str).str.replace('$', '').astype(float)
    
    # Rename columns to match our format
    stock_df = stock_df.rename(columns={
        'Date': 'date',
        'Close/Last': 'underlying_close',
        'Open': 'underlying_open', 
        'High': 'underlying_high',
        'Low': 'underlying_low',
        'Volume': 'underlying_volume'
    })
    
    # Parse dates
    stock_df['date'] = pd.to_datetime(stock_df['date'], format='%m/%d/%Y')
    stock_df['date_only_date'] = stock_df['date'].dt.date
    
    # Process available years
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    
    # Find all parquet files
    monthly_files = sorted(data_dir.glob("options_day_aggs_*_monthly.parquet"))
    weekly_files = sorted(data_dir.glob("options_day_aggs_*_weekly.parquet"))
    
    print(f"\n📊 Found {len(monthly_files)} monthly files and {len(weekly_files)} weekly files")
    
    for file_path in monthly_files:
        year = file_path.stem.split('_')[2]
        print(f"\n📅 Processing {year} monthly data...")
        
        try:
            # Load options data
            options_df = pd.read_parquet(file_path)
            print(f"   Loaded {len(options_df):,} rows")
            
            # Process dates
            options_df['date_only'] = pd.to_datetime(options_df['date_only'])
            options_df['date_only_date'] = options_df['date_only'].dt.date
            
            # Merge with stock prices
            merged_df = options_df.merge(stock_df, on='date_only_date', how='left')
            
            # Handle missing stock prices
            missing_prices = merged_df['underlying_close'].isna().sum()
            if missing_prices > 0:
                print(f"   ⚠️  {missing_prices} rows have missing stock prices")
                merged_df['underlying_spot'] = merged_df['underlying_close'].fillna(merged_df['strike'] + merged_df['close_price'])
            else:
                merged_df['underlying_spot'] = merged_df['underlying_close']
            
            # Fill missing price columns
            merged_df['underlying_open'] = merged_df['underlying_open'].fillna(merged_df['underlying_spot'])
            merged_df['underlying_high'] = merged_df['underlying_high'].fillna(merged_df['underlying_spot'])
            merged_df['underlying_low'] = merged_df['underlying_low'].fillna(merged_df['underlying_spot'])
            
            # Add expiration prices
            merged_df['underlying_close_at_expiry'] = merged_df['underlying_spot']
            merged_df['underlying_high_at_expiry'] = merged_df['underlying_spot']
            merged_df['underlying_spot_at_expiry'] = merged_df['underlying_spot']
            
            # Parse strike from ticker
            def parse_strike_from_ticker(ticker):
                ticker_clean = ticker[2:] if ticker.startswith('O:') else ticker
                strike_str = ticker_clean[-8:]
                return float(strike_str) / 1000.0
            
            merged_df['strike'] = merged_df['ticker'].apply(parse_strike_from_ticker)
            
            # Calculate OTM percentage
            merged_df['otm_pct'] = ((merged_df['strike'] - merged_df['underlying_spot']) / merged_df['underlying_spot'] * 100).round(2)
            merged_df['ITM'] = (merged_df['strike'] < merged_df['underlying_spot']).map({True: 'YES', False: 'NO'})
            
            # Calculate premiums
            merged_df['premium'] = merged_df['close_price']
            merged_df['premium_yield_pct'] = (merged_df['close_price'] / merged_df['underlying_spot'] * 100).round(2)
            merged_df['premium_low'] = merged_df['low_price']
            merged_df['premium_yield_pct_low'] = (merged_df['low_price'] / merged_df['underlying_spot'] * 100).round(2)
            
            # Remove temporary columns
            merged_df = merged_df.drop(['date_only_date', 'date', 'underlying_volume'], axis=1)
            
            # Save result
            output_path = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly/{year}_options_pessimistic.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            
            print(f"   ✅ Saved {len(merged_df):,} rows to {output_path}")
            
        except Exception as e:
            print(f"   ❌ Error processing {year} monthly: {e}")
    
    # Process weekly files
    for file_path in weekly_files:
        year = file_path.stem.split('_')[2]
        print(f"\n📅 Processing {year} weekly data...")
        
        try:
            # Load options data
            options_df = pd.read_parquet(file_path)
            print(f"   Loaded {len(options_df):,} rows")
            
            # Process dates
            options_df['date_only'] = pd.to_datetime(options_df['date_only'])
            options_df['date_only_date'] = options_df['date_only'].dt.date
            
            # Merge with stock prices
            merged_df = options_df.merge(stock_df, on='date_only_date', how='left')
            
            # Handle missing stock prices
            missing_prices = merged_df['underlying_close'].isna().sum()
            if missing_prices > 0:
                print(f"   ⚠️  {missing_prices} rows have missing stock prices")
                merged_df['underlying_spot'] = merged_df['underlying_close'].fillna(merged_df['strike'] + merged_df['close_price'])
            else:
                merged_df['underlying_spot'] = merged_df['underlying_close']
            
            # Fill missing price columns
            merged_df['underlying_open'] = merged_df['underlying_open'].fillna(merged_df['underlying_spot'])
            merged_df['underlying_high'] = merged_df['underlying_high'].fillna(merged_df['underlying_spot'])
            merged_df['underlying_low'] = merged_df['underlying_low'].fillna(merged_df['underlying_spot'])
            
            # Add expiration prices
            merged_df['underlying_close_at_expiry'] = merged_df['underlying_spot']
            merged_df['underlying_high_at_expiry'] = merged_df['underlying_spot']
            merged_df['underlying_spot_at_expiry'] = merged_df['underlying_spot']
            
            # Parse strike from ticker
            def parse_strike_from_ticker(ticker):
                ticker_clean = ticker[2:] if ticker.startswith('O:') else ticker
                strike_str = ticker_clean[-8:]
                return float(strike_str) / 1000.0
            
            merged_df['strike'] = merged_df['ticker'].apply(parse_strike_from_ticker)
            
            # Calculate OTM percentage
            merged_df['otm_pct'] = ((merged_df['strike'] - merged_df['underlying_spot']) / merged_df['underlying_spot'] * 100).round(2)
            merged_df['ITM'] = (merged_df['strike'] < merged_df['underlying_spot']).map({True: 'YES', False: 'NO'})
            
            # Calculate premiums
            merged_df['premium'] = merged_df['close_price']
            merged_df['premium_yield_pct'] = (merged_df['close_price'] / merged_df['underlying_spot'] * 100).round(2)
            merged_df['premium_low'] = merged_df['low_price']
            merged_df['premium_yield_pct_low'] = (merged_df['low_price'] / merged_df['underlying_spot'] * 100).round(2)
            
            # Remove temporary columns
            merged_df = merged_df.drop(['date_only_date', 'date', 'underlying_volume'], axis=1)
            
            # Save result
            output_path = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly/{year}_options_pessimistic.csv")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            
            print(f"   ✅ Saved {len(merged_df):,} rows to {output_path}")
            
        except Exception as e:
            print(f"   ❌ Error processing {year} weekly: {e}")
    
    print(f"\n✅ Processing completed!")
    
    # Summary
    monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly")
    weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly")
    
    print(f"\n📊 Summary of created files:")
    monthly_files = list(monthly_dir.glob("*.csv"))
    weekly_files = list(weekly_dir.glob("*.csv"))
    
    print(f"   Monthly files: {len(monthly_files)}")
    for f in sorted(monthly_files):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"     - {f.name} ({size_mb:.1f} MB)")
    
    print(f"   Weekly files: {len(weekly_files)}")
    for f in sorted(weekly_files):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"     - {f.name} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    process_aapl_data()
