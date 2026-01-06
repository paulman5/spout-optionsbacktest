#!/usr/bin/env python3
"""
Merge historical stock data with options data for a specific ticker.
This script will merge underlying stock prices into options CSV files.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def load_historical_data(ticker):
    """Load historical stock data for a ticker."""
    historical_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}/HistoricalData_{ticker}.csv")
    
    if not historical_file.exists():
        print(f"  ❌ Historical data file not found: {historical_file}")
        return None
    
    print(f"  📊 Loading historical data: {historical_file}")
    df_historical = pd.read_csv(historical_file)
    
    # Clean and prepare historical data
    df_historical['Date'] = pd.to_datetime(df_historical['Date'], format='mixed')
    
    # Remove $ signs and convert to numeric for all price columns
    price_columns = ['Close/Last', 'Open', 'High', 'Low']
    for col in price_columns:
        if col in df_historical.columns:
            df_historical[col] = df_historical[col].astype(str).str.replace('$', '').astype(float)
    
    # Rename columns to match TSLA format
    df_historical = df_historical.rename(columns={
        'Date': 'date',
        'Close/Last': 'underlying_close',
        'Open': 'underlying_open', 
        'High': 'underlying_high',
        'Low': 'underlying_low',
        'Volume': 'underlying_volume'
    })
    
    # Add underlying_spot column (using close price)
    df_historical['underlying_spot'] = df_historical['underlying_close']
    
    print(f"    ✅ Loaded {len(df_historical)} historical price records")
    return df_historical

def merge_options_with_historical(ticker, year, period):
    """Merge options data with historical stock prices."""
    print(f"\n=== Merging {ticker} {year} {period} ===")
    
    # Load options data
    options_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}/{period}/{year}_options_pessimistic.csv")
    
    if not options_file.exists():
        print(f"  ❌ Options file not found: {options_file}")
        return False
    
    print(f"  📄 Loading options data: {options_file.name}")
    df_options = pd.read_csv(options_file)
    
    # Load historical data
    df_historical = load_historical_data(ticker)
    if df_historical is None:
        return False
    
    # Convert options date
    df_options['date_only'] = pd.to_datetime(df_options['date_only'])
    
    # Convert historical date to datetime
    df_historical['date'] = pd.to_datetime(df_historical['date'])
    
    # Merge with historical data
    print(f"  🔗 Merging with historical prices...")
    df_merged = df_options.merge(
        df_historical, 
        left_on='date_only', 
        right_on='date', 
        how='left',
        suffixes=('', '_historical')
    )
    
    # Check merge results
    missing_prices = df_merged['underlying_spot'].isna().sum()
    if missing_prices > 0:
        print(f"  ⚠️  {missing_prices} options have missing historical prices")
    
    # Calculate additional metrics
    print(f"  🧮 Calculating options metrics...")
    
    # OTM percentage
    df_merged['otm_pct'] = ((df_merged['strike'] - df_merged['underlying_spot']) / df_merged['underlying_spot'] * 100).round(2)
    
    # ITM flag (YES/NO like TSLA)
    df_merged['ITM'] = df_merged['underlying_spot'] > df_merged['strike']
    df_merged['ITM'] = df_merged['ITM'].map({True: 'YES', False: 'NO'})
    
    # Premium (using close_price)
    df_merged['premium'] = df_merged['close_price']
    
    # Premium yield percentage
    df_merged['premium_yield_pct'] = ((df_merged['close_price'] / df_merged['strike']) * 100).round(2)
    
    # Premium low (using open_price)
    df_merged['premium_low'] = df_merged['open_price']
    
    # Premium yield low percentage
    df_merged['premium_yield_pct_low'] = ((df_merged['open_price'] / df_merged['strike']) * 100).round(2)
    
    # Time value
    df_merged['time_value'] = np.maximum(0, df_merged['close_price'] - np.maximum(0, df_merged['underlying_spot'] - df_merged['strike']))
    
    # Intrinsic value
    df_merged['intrinsic_value'] = np.maximum(0, df_merged['underlying_spot'] - df_merged['strike'])
    
    # Extrinsic value
    df_merged['extrinsic_value'] = df_merged['close_price'] - df_merged['intrinsic_value']
    
    # Add expiry date historical data (same as current date for entry)
    df_merged['underlying_close_at_expiry'] = df_merged['underlying_close']
    df_merged['underlying_high_at_expiry'] = df_merged['underlying_high']
    df_merged['underlying_spot_at_expiry'] = df_merged['underlying_spot']
    
    # Sort by date and strike
    df_merged = df_merged.sort_values(['date_only', 'strike'])
    
    # Reorder columns to match TSLA format exactly
    tsla_columns = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 
        'premium', 'premium_yield_pct', 'premium_low', 'premium_yield_pct_low', 
        'high_price', 'low_price', 'transactions', 'window_start', 'days_to_expiry', 
        'time_remaining_category', 'underlying_open', 'underlying_close', 'underlying_high', 
        'underlying_low', 'underlying_spot', 'underlying_close_at_expiry', 
        'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    
    # Add calculated columns to the list
    tsla_columns.extend(['time_value', 'intrinsic_value', 'extrinsic_value'])
    
    # Reorder and select columns
    available_columns = [col for col in tsla_columns if col in df_merged.columns]
    df_merged = df_merged[available_columns]
    
    # Save merged data
    print(f"  💾 Saving merged data...")
    df_merged.to_csv(options_file, index=False)
    
    print(f"  ✅ {ticker} {year} {period} merged successfully!")
    print(f"    📊 Total options: {len(df_merged):,}")
    print(f"    📈 Average OTM%: {df_merged['otm_pct'].mean():.1f}%")
    print(f"    💰 Average premium yield: {df_merged['premium_yield_pct'].mean():.1f}%")
    
    return True

def process_ticker(ticker):
    """Process all years for a ticker."""
    print(f"\n🚀 Processing ticker: {ticker}")
    print("=" * 50)
    
    ticker_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}")
    
    if not ticker_dir.exists():
        print(f"  ❌ Ticker directory not found: {ticker_dir}")
        return
    
    # Get available years for monthly and weekly
    monthly_years = []
    weekly_years = []
    
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        monthly_years = [int(f.stem.split('_')[0]) for f in monthly_dir.glob('*.csv')]
        monthly_years.sort()
    
    weekly_dir = ticker_dir / 'weekly'
    if weekly_dir.exists():
        weekly_years = [int(f.stem.split('_')[0]) for f in weekly_dir.glob('*.csv')]
        weekly_years.sort()
    
    print(f"  📅 Found monthly years: {monthly_years}")
    print(f"  📅 Found weekly years: {weekly_years}")
    
    # Process monthly files
    if monthly_years:
        print(f"\n📁 Processing MONTHLY data...")
        for year in monthly_years:
            success = merge_options_with_historical(ticker, year, 'monthly')
            if not success:
                print(f"    ❌ Failed to merge {ticker} {year} monthly")
    
    # Process weekly files
    if weekly_years:
        print(f"\n📁 Processing WEEKLY data...")
        for year in weekly_years:
            success = merge_options_with_historical(ticker, year, 'weekly')
            if not success:
                print(f"    ❌ Failed to merge {ticker} {year} weekly")
    
    print(f"\n✅ {ticker} processing completed!")

def main():
    """Main function - process all remaining tickers."""
    # All tickers except AAPL (already processed)
    tickers = [
        'AMZN', 'MSFT', 'GOOG', 'NVDA', 'SPY', 'META', 
        'HOOD', 'IWM', 'JPM', 'XLE', 'XLF', 'XLK', 'QQQ'
    ]
    
    print(f"🚀 Processing {len(tickers)} remaining tickers...")
    print("=" * 60)
    
    for ticker in tickers:
        process_ticker(ticker)
    
    print(f"\n🎉 All tickers merge process completed!")
    print("✅ All options data now merged with historical stock prices!")

if __name__ == "__main__":
    main()
