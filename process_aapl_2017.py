#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse


def process_aapl_2017_with_historical_data(input_file, output_file):
    """Process 2017 AAPL options data using historical stock prices"""
    
    # Stock price file path
    stock_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/HistoricalData_AAPL.csv")
    
    print(f"📊 Loading options data from {input_file}...")
    options_df = pd.read_csv(input_file)
    print(f"   Loaded {len(options_df):,} rows")
    
    print(f"📈 Loading stock price data from {stock_file}...")
    stock_df = pd.read_csv(stock_file)
    print(f"   Loaded {len(stock_df):,} rows")
    
    # Parse stock data dates and filter for 2017
    stock_df['Date'] = pd.to_datetime(stock_df['Date'], format='%m/%d/%Y')
    stock_2017 = stock_df[stock_df['Date'].dt.year == 2017].copy()
    
    if len(stock_2017) == 0:
        raise ValueError("No 2017 data found in stock price file")
    
    print(f"   Found {len(stock_2017):,} days of 2017 stock data")
    print(f"   Date range: {stock_2017['Date'].min().strftime('%Y-%m-%d')} to {stock_2017['Date'].max().strftime('%Y-%m-%d')}")
    
    # Clean stock price data (remove $ signs and convert to float)
    for col in ['Close/Last', 'Open', 'High', 'Low']:
        stock_2017[col] = stock_2017[col].astype(str).str.replace('$', '').astype(float)
    
    # Rename columns to match our format
    stock_2017 = stock_2017.rename(columns={
        'Date': 'date',
        'Close/Last': 'underlying_close',
        'Open': 'underlying_open', 
        'High': 'underlying_high',
        'Low': 'underlying_low',
        'Volume': 'underlying_volume'
    })
    
    # Convert date to date format for merging
    stock_2017['date_only_date'] = stock_2017['date'].dt.date
    
    # Process options data
    options_df['date_only'] = pd.to_datetime(options_df['date_only'])
    options_df['expiration_date'] = pd.to_datetime(options_df['expiration_date'])
    options_df['date_only_date'] = options_df['date_only'].dt.date
    
    # Merge stock prices with options data
    print(f"🔗 Merging stock prices with options data...")
    merged_df = options_df.merge(stock_2017, on='date_only_date', how='left')
    
    # Check for missing stock prices
    missing_prices = merged_df['underlying_close'].isna().sum()
    if missing_prices > 0:
        print(f"⚠️  Warning: {missing_prices} rows have missing stock prices")
        # For missing data, estimate from options
        merged_df['underlying_spot'] = merged_df.apply(
            lambda row: row['strike'] + row['close_price'] if pd.isna(row['underlying_close']) else row['underlying_close'],
            axis=1
        )
    else:
        merged_df['underlying_spot'] = merged_df['underlying_close']
    
    # Fill missing price columns
    merged_df['underlying_open'] = merged_df['underlying_open'].fillna(merged_df['underlying_spot'])
    merged_df['underlying_high'] = merged_df['underlying_high'].fillna(merged_df['underlying_spot'])
    merged_df['underlying_low'] = merged_df['underlying_low'].fillna(merged_df['underlying_spot'])
    
    # Add expiration prices (same as spot for simplicity)
    merged_df['underlying_close_at_expiry'] = merged_df['underlying_spot']
    merged_df['underlying_high_at_expiry'] = merged_df['underlying_spot']
    merged_df['underlying_spot_at_expiry'] = merged_df['underlying_spot']
    
    # Parse strike directly from ticker and apply adjustments
    def parse_strike_from_ticker(ticker):
        """Extract strike from option ticker"""
        # Remove 'O:' prefix
        ticker_clean = ticker[2:] if ticker.startswith('O:') else ticker
        # Extract strike part (last 8 digits)
        strike_str = ticker_clean[-8:]
        strike_raw = float(strike_str) / 1000.0  # Divide by 1000 for OPRA format
        return strike_raw
    
    def adjust_strike_aapl(row):
        """Parse strike from ticker and apply stock split adjustments for AAPL"""
        date = pd.to_datetime(row['date_only'])
        
        # Parse strike from ticker
        strike_raw = parse_strike_from_ticker(row['ticker'])
        
        # Apply stock split adjustments for AAPL
        # Note: AAPL had different split dates than TSLA
        # For 2017, AAPL had a 7-for-1 split in 2014, so 2017 data should be post-split
        # No additional splits needed for 2017 AAPL
        adjusted_strike = strike_raw / 1.0  # No adjustment for 2017
        
        return round(adjusted_strike, 2)
    
    # Apply strike adjustments
    merged_df['strike'] = merged_df.apply(adjust_strike_aapl, axis=1)
    
    # Calculate OTM percentage using corrected formula
    merged_df['otm_pct'] = ((merged_df['strike'] - merged_df['underlying_spot']) / merged_df['underlying_spot'] * 100).round(2)
    
    # ITM: YES if strike < spot (negative otm_pct), NO otherwise
    merged_df['ITM'] = (merged_df['strike'] < merged_df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Premium in dollars
    merged_df['premium'] = merged_df['close_price']
    
    # Premium yield as percentage: (close_price / underlying_spot) * 100
    merged_df['premium_yield_pct'] = (merged_df['close_price'] / merged_df['underlying_spot'] * 100).round(2)
    
    # Premium low in dollars
    merged_df['premium_low'] = merged_df['low_price']
    
    # Premium yield low as percentage: (low_price / underlying_spot) * 100
    merged_df['premium_yield_pct_low'] = (merged_df['low_price'] / merged_df['underlying_spot'] * 100).round(2)
    
    # Remove temporary columns
    merged_df = merged_df.drop(['date_only_date', 'date', 'underlying_volume'], axis=1)
    
    # Reorder columns to match other files
    column_order = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 'premium',
        'premium_yield_pct', 'premium_low', 'premium_yield_pct_low', 'high_price', 'low_price',
        'transactions', 'window_start', 'days_to_expiry', 'time_remaining_category',
        'underlying_open', 'underlying_close', 'underlying_high', 'underlying_low', 'underlying_spot',
        'underlying_close_at_expiry', 'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    
    # Ensure all columns exist and reorder
    for col in column_order:
        if col not in merged_df.columns:
            merged_df[col] = None
    
    merged_df = merged_df[column_order]
    
    # Save the result
    print(f"💾 Saving to {output_file}...")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    
    print(f"✅ Saved {len(merged_df):,} rows to {output_path}")
    print(f"\n📊 Summary:")
    print(f"   Date range: {merged_df['date_only'].min().strftime('%Y-%m-%d')} to {merged_df['date_only'].max().strftime('%Y-%m-%d')}")
    print(f"   Strike range: ${merged_df['strike'].min():.2f} to ${merged_df['strike'].max():.2f}")
    print(f"   Stock price range: ${merged_df['underlying_spot'].min():.2f} to ${merged_df['underlying_spot'].max():.2f}")
    print(f"   OTM options: {(merged_df['otm_pct'] > 0).sum():,}")
    print(f"   ITM options: {(merged_df['otm_pct'] <= 0).sum():,}")
    
    return merged_df


def main():
    parser = argparse.ArgumentParser(description="Process 2017 AAPL options data with historical prices")
    parser.add_argument("--input-file", required=True, help="Path to input CSV file")
    parser.add_argument("--output-file", required=True, help="Path to output CSV file")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("PROCESSING 2017 AAPL OPTIONS DATA WITH HISTORICAL PRICES")
    print("=" * 60)
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output_file}")
    
    process_aapl_2017_with_historical_data(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
