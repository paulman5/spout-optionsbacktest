#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse


def process_aapl_data_with_historical(year, input_file, output_file):
    """Process AAPL options data for a specific year with historical stock prices"""
    
    # Stock price file path
    stock_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/HistoricalData_AAPL.csv")
    
    print(f"📊 Loading {year} options data from {input_file}...")
    options_df = pd.read_csv(input_file)
    print(f"   Loaded {len(options_df):,} rows")
    
    print(f"📈 Loading stock price data from {stock_file}...")
    stock_df = pd.read_csv(stock_file)
    print(f"   Loaded {len(stock_df):,} rows")
    
    # Parse stock data dates and filter for the specific year
    stock_df['Date'] = pd.to_datetime(stock_df['Date'], format='%m/%d/%Y')
    stock_year = stock_df[stock_df['Date'].dt.year == year].copy()
    
    if len(stock_year) == 0:
        raise ValueError(f"No {year} data found in stock price file")
    
    print(f"   Found {len(stock_year):,} days of {year} stock data")
    print(f"   Date range: {stock_year['Date'].min().strftime('%Y-%m-%d')} to {stock_year['Date'].max().strftime('%Y-%m-%d')}")
    
    # Clean stock price data (remove $ signs and convert to float)
    for col in ['Close/Last', 'Open', 'High', 'Low']:
        stock_year[col] = stock_year[col].astype(str).str.replace('$', '').astype(float)
    
    # Rename columns to match our format
    stock_year = stock_year.rename(columns={
        'Date': 'date',
        'Close/Last': 'underlying_close',
        'Open': 'underlying_open', 
        'High': 'underlying_high',
        'Low': 'underlying_low',
        'Volume': 'underlying_volume'
    })
    
    # Convert date to date format for merging
    stock_year['date_only_date'] = stock_year['date'].dt.date
    
    # Process options data
    options_df['date_only'] = pd.to_datetime(options_df['date_only'])
    options_df['expiration_date'] = pd.to_datetime(options_df['expiration_date'])
    options_df['date_only_date'] = options_df['date_only'].dt.date
    
    # Merge stock prices with options data
    print(f"🔗 Merging stock prices with options data...")
    merged_df = options_df.merge(stock_year, on='date_only_date', how='left')
    
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
        # AAPL had a 7-for-1 split on 2014-06-09, so all data from 2016+ is post-split
        # No additional splits needed for 2016-2025 AAPL
        adjusted_strike = strike_raw / 1.0  # No adjustment for 2016-2025
        
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
    
    # Save result
    print(f"💾 Saving to {output_file}...")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    
    print(f"✅ Saved {len(merged_df):,} rows to {output_path}")
    print(f"\n📊 Summary for {year}:")
    print(f"   Date range: {merged_df['date_only'].min().strftime('%Y-%m-%d')} to {merged_df['date_only'].max().strftime('%Y-%m-%d')}")
    print(f"   Strike range: ${merged_df['strike'].min():.2f} to ${merged_df['strike'].max():.2f}")
    print(f"   Stock price range: ${merged_df['underlying_spot'].min():.2f} to ${merged_df['underlying_spot'].max():.2f}")
    print(f"   OTM options: {(merged_df['otm_pct'] > 0).sum():,}")
    print(f"   ITM options: {(merged_df['otm_pct'] <= 0).sum():,}")
    
    return merged_df


def process_all_aapl_years():
    """Process all AAPL years 2016-2025"""
    
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    print("🚀 Processing AAPL data for years 2016-2025")
    print("=" * 60)
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Check if aggregated data exists
        monthly_input = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_{year}_monthly.csv"
        weekly_input = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_{year}_weekly.csv"
        
        monthly_output = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly/{year}_options_pessimistic.csv"
        weekly_output = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly/{year}_options_pessimistic.csv"
        
        # Process monthly data
        if Path(monthly_input).exists():
            try:
                print(f"   📊 Processing monthly data for {year}...")
                process_aapl_data_with_historical(year, monthly_input, monthly_output)
                print(f"   ✅ Monthly data completed for {year}")
            except Exception as e:
                print(f"   ❌ Error processing monthly data for {year}: {e}")
        else:
            print(f"   ⚠️  Monthly data file not found for {year}: {monthly_input}")
        
        # Process weekly data
        if Path(weekly_input).exists():
            try:
                print(f"   📊 Processing weekly data for {year}...")
                process_aapl_data_with_historical(year, weekly_input, weekly_output)
                print(f"   ✅ Weekly data completed for {year}")
            except Exception as e:
                print(f"   ❌ Error processing weekly data for {year}: {e}")
        else:
            print(f"   ⚠️  Weekly data file not found for {year}: {weekly_input}")
    
    print(f"\n{'='*60}")
    print("✅ AAPL data processing completed for all years 2016-2025")
    
    # Summary of created files
    print(f"\n📊 Summary of created files:")
    aapl_monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly")
    aapl_weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly")
    
    if aapl_monthly_dir.exists():
        monthly_files = list(aapl_monthly_dir.glob("*.csv"))
        print(f"   Monthly files: {len(monthly_files)} created")
        for f in sorted(monthly_files):
            print(f"     - {f.name}")
    
    if aapl_weekly_dir.exists():
        weekly_files = list(aapl_weekly_dir.glob("*.csv"))
        print(f"   Weekly files: {len(weekly_files)} created")
        for f in sorted(weekly_files):
            print(f"     - {f.name}")


def main():
    parser = argparse.ArgumentParser(description="Process AAPL options data with historical prices")
    parser.add_argument("--year", type=int, help="Process specific year (e.g., 2016)")
    parser.add_argument("--input-file", help="Path to input CSV file for specific year")
    parser.add_argument("--output-file", help="Path to output CSV file for specific year")
    parser.add_argument("--all-years", action="store_true", help="Process all years 2016-2025")
    
    args = parser.parse_args()
    
    if args.all_years:
        process_all_aapl_years()
    elif args.year and args.input_file and args.output_file:
        print("=" * 60)
        print(f"PROCESSING AAPL OPTIONS DATA FOR {args.year}")
        print("=" * 60)
        process_aapl_data_with_historical(args.year, args.input_file, args.output_file)
    else:
        print("Please specify either --all-years or --year with --input-file and --output-file")


if __name__ == "__main__":
    main()
