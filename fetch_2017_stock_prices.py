#!/usr/bin/env python3
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

def fetch_2017_stock_prices():
    """Fetch 2017 TSLA stock prices and merge with options data"""
    
    # Input and output paths
    input_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_2017_monthly.csv")
    output_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_2017_monthly_with_prices.csv")
    
    print(f"📊 Loading options data from {input_file}...")
    df = pd.read_csv(input_file)
    print(f"   Loaded {len(df):,} rows")
    
    # Convert date columns
    df['date_only'] = pd.to_datetime(df['date_only'])
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    # Get date range for stock prices
    start_date = df['date_only'].min().strftime('%Y-%m-%d')
    end_date = (df['expiration_date'].max() + timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📈 Fetching TSLA stock prices from {start_date} to {end_date}...")
    
    # Fetch stock prices using yfinance
    ticker = yf.Ticker('TSLA')
    hist = ticker.history(start=start_date, end=end_date)
    
    if hist.empty:
        raise ValueError(f"No price data found for TSLA between {start_date} and {end_date}")
    
    # Reset index and prepare for merging
    hist = hist.reset_index()
    hist['date'] = pd.to_datetime(hist['Date']).dt.date
    hist = hist[['date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    hist.columns = ['date', 'underlying_open', 'underlying_high', 'underlying_low', 'underlying_close', 'underlying_volume']
    
    print(f"   Fetched {len(hist):,} days of stock price data")
    
    # Convert options date_only to date for merging
    df['date_only_date'] = df['date_only'].dt.date
    
    # Merge stock prices for trading dates
    df = df.merge(hist, left_on='date_only_date', right_on='date', how='left')
    df = df.drop('date', axis=1)  # Remove the duplicate date column
    
    # Add underlying_spot (using close price)
    df['underlying_spot'] = df['underlying_close']
    
    # For expiration dates, we need the stock price on expiration
    # Create a separate mapping for expiration dates
    exp_dates = df[['expiration_date']].drop_duplicates()
    exp_dates['exp_date'] = exp_dates['expiration_date'].dt.date
    
    exp_prices = exp_dates.merge(hist, left_on='exp_date', right_on='date', how='left')
    exp_prices = exp_prices[['exp_date', 'underlying_close']].rename(columns={'underlying_close': 'underlying_close_at_expiry'})
    
    # Merge expiration prices back
    df = df.merge(exp_prices, left_on='date_only_date', right_on='exp_date', how='left')
    df = df.drop('exp_date', axis=1)
    
    # Fill missing expiration prices with the close price (fallback)
    df['underlying_close_at_expiry'] = df['underlying_close_at_expiry'].fillna(df['underlying_close'])
    
    # Add underlying_high_at_expiry and underlying_spot_at_expiry (same as close for simplicity)
    df['underlying_high_at_expiry'] = df['underlying_close_at_expiry']
    df['underlying_spot_at_expiry'] = df['underlying_close_at_expiry']
    
    # Remove temporary column
    df = df.drop('date_only_date', axis=1)
    
    # Check for missing data
    missing_prices = df['underlying_spot'].isna().sum()
    if missing_prices > 0:
        print(f"⚠️  Warning: {missing_prices} rows have missing stock prices")
    
    print(f"💾 Saving to {output_file}...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    
    print(f"✅ Saved {len(df):,} rows to {output_file}")
    print(f"\n📊 Summary:")
    print(f"   Date range: {df['date_only'].min().strftime('%Y-%m-%d')} to {df['date_only'].max().strftime('%Y-%m-%d')}")
    print(f"   Stock price range: ${df['underlying_close'].min():.2f} to ${df['underlying_close'].max():.2f}")
    
    return df

if __name__ == "__main__":
    fetch_2017_stock_prices()
