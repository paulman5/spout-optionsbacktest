#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


def fix_all_aapl_files():
    """Fix all AAPL files with correct stock price matching"""
    
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
    
    # Process all years (2016-2025)
    years_to_process = list(range(2016, 2026))
    
    for year in years_to_process:
        print(f"\n📅 Processing {year} data...")
        
        # Check both monthly and weekly files
        for period in ['monthly', 'weekly']:
            file_path = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/{period}/{year}_options_pessimistic.csv")
            
            if not file_path.exists():
                print(f"   ⚠️  {period} file not found: {file_path}")
                continue
            
            try:
                # Load existing data
                df = pd.read_csv(file_path)
                print(f"   📊 Loaded {len(df)} rows from {period} file")
                
                # Process dates
                df['date_only'] = pd.to_datetime(df['date_only'])
                df['date_only_date'] = df['date_only'].dt.date
                
                # Filter stock data for this year
                stock_year = stock_df[stock_df['date'].dt.year == year].copy()
                print(f"   📈 Found {len(stock_year)} days of {year} stock data")
                
                # Show sample dates for verification
                if len(df) > 0:
                    sample_date = df['date_only'].iloc[0]
                    sample_stock_price = stock_year[stock_year['date_only_date'] == sample_date.date()]
                    if len(sample_stock_price) > 0:
                        actual_price = sample_stock_price['underlying_close'].iloc[0]
                        print(f"   🔍 Sample date {sample_date.date()}: actual stock price = ${actual_price:.2f}")
                
                # Merge with stock prices
                merged_df = df.merge(stock_year, on='date_only_date', how='left')
                
                # Handle missing stock prices
                missing_prices = merged_df['underlying_close'].isna().sum()
                if missing_prices > 0:
                    print(f"   ⚠️  {missing_prices} rows have missing stock prices")
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
                merged_df = merged_df.drop(['date_only_date', 'date', 'underlying_volume'], axis=1, errors='ignore')
                
                # Reorder columns to match expected format
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
                merged_df.to_csv(file_path, index=False)
                
                print(f"   ✅ Fixed and saved {len(merged_df)} rows to {file_path}")
                print(f"   📊 Date range: {merged_df['date_only'].min()} to {merged_df['date_only'].max()}")
                print(f"   💰 Stock price range: ${merged_df['underlying_spot'].min():.2f} to ${merged_df['underlying_spot'].max():.2f}")
                
                # Verify a sample calculation
                if len(merged_df) > 0:
                    sample_row = merged_df.iloc[0]
                    print(f"   🔍 Sample verification:")
                    print(f"      Date: {sample_row['date_only']}")
                    print(f"      Strike: ${sample_row['strike']:.2f}")
                    print(f"      Stock price: ${sample_row['underlying_spot']:.2f}")
                    print(f"      OTM %: {sample_row['otm_pct']:.2f}%")
                    print(f"      ITM: {sample_row['ITM']}")
                
            except Exception as e:
                print(f"   ❌ Error fixing {year} {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ All AAPL files have been fixed!")
    
    # Final verification
    print(f"\n📊 Final verification:")
    monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly")
    weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly")
    
    for period in ['monthly', 'weekly']:
        dir_path = monthly_dir if period == 'monthly' else weekly_dir
        files = sorted(dir_path.glob("*.csv"))
        
        print(f"\n   {period.capitalize()} files:")
        for f in files:
            year = f.stem.split('_')[0]
            df = pd.read_csv(f)
            has_stock_data = 'underlying_spot' in df.columns
            status = "✅" if has_stock_data else "❌"
            cols = len(df.columns)
            print(f"     {status} {year}: {cols} columns ({'with' if has_stock_data else 'without'} stock data)")


if __name__ == "__main__":
    fix_all_aapl_files()
