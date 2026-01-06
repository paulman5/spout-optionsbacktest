#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys


def process_all_ticker_data():
    """Process all downloaded ticker data"""
    
    # Ticker configurations with stock splits
    tickers_config = {
        'MSFT': {
            'split_date': pd.to_datetime('2020-01-31'),
            'split_ratio': 2.0
        },
        'GOOG': {
            'split_date': pd.to_datetime('2022-07-18'),
            'split_ratio': 20.0
        },
        'NVDA': {
            'split_date': pd.to_datetime('2021-07-20'),
            'split_ratio': 4.0
        },
        'SPY': {
            'split_date': pd.to_datetime('2099-12-31'),
            'split_ratio': 1.0
        },
        'META': {
            'split_date': pd.to_datetime('2099-12-31'),
            'split_ratio': 1.0
        }
    }
    
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    
    for ticker, config in tickers_config.items():
        print(f"\n{'='*60}")
        print(f"🚀 Processing {ticker} data...")
        print(f"{'='*60}")
        
        # Load historical data
        stock_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}/HistoricalData_{ticker}.csv")
        if not stock_file.exists():
            print(f"❌ Historical data not found: {stock_file}")
            continue
            
        print("📈 Loading historical stock data...")
        stock_df = pd.read_csv(stock_file)
        
        # Clean stock price data
        for col in ['Close/Last', 'Open', 'High', 'Low']:
            if col in stock_df.columns:
                stock_df[col] = stock_df[col].astype(str).str.replace('$', '').astype(float)
        
        # Rename columns
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
        
        # Create lookup
        stock_lookup = {}
        for _, row in stock_df.iterrows():
            stock_lookup[row['date_only_date']] = {
                'underlying_close': row['underlying_close'],
                'underlying_open': row['underlying_open'],
                'underlying_high': row['underlying_high'],
                'underlying_low': row['underlying_low']
            }
        
        print(f"   Created stock price lookup for {len(stock_lookup)} dates")
        
        # Create directory structure
        ticker_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/{ticker}")
        (ticker_dir / "monthly").mkdir(exist_ok=True)
        (ticker_dir / "weekly").mkdir(exist_ok=True)
        
        # Process years 2016-2025 (or available years for each ticker)
        if ticker == 'META':
            years_to_process = list(range(2016, 2026))  # FB 2016, META 2017-2025
        else:
            years_to_process = list(range(2016, 2026))
        success_count = 0
        
        for year in years_to_process:
            print(f"\n📅 Processing {year} {ticker} data...")
            
            # Process both monthly and weekly files
            for period in ['monthly', 'weekly']:
                # For META 2016, check for FB files
                if ticker == 'META' and year == 2016:
                    fb_parquet_file = data_dir / f"options_day_aggs_{year}_{period}.parquet"
                    if fb_parquet_file.exists():
                        parquet_file = fb_parquet_file
                    else:
                        print(f"   ⚠️  FB 2016 {period} parquet file not found")
                        continue
                else:
                    parquet_file = data_dir / f"options_day_aggs_{year}_{period}.parquet"
                
                target_file = ticker_dir / f"{period}/{year}_options_pessimistic.csv"
                
                if parquet_file.exists():
                    print(f"   🔄 Processing {year} {period} data...")
                    
                    try:
                        # Load parquet data
                        df = pd.read_parquet(parquet_file)
                        
                        # Remove PUT options
                        df_calls_only = df[df['option_type'] == 'C'].copy()
                        removed_count = len(df) - len(df_calls_only)
                        print(f"   🗑️  Removed {removed_count:,} PUT options")
                        
                        # For META 2016 (FB data), update ticker to META
                        if ticker == 'META' and year == 2016:
                            df_calls_only['ticker'] = df_calls_only['ticker'].str.replace('O:FB', 'O:META')
                        
                        # Process dates
                        df_calls_only['date_only'] = pd.to_datetime(df_calls_only['date_only'])
                        df_calls_only['date_only_date'] = df_calls_only['date_only'].dt.date
                        
                        # Apply stock split adjustments
                        def adjust_strike(row):
                            date = pd.to_datetime(row['date_only'])
                            ticker_clean = row['ticker'][2:] if row['ticker'].startswith('O:') else row['ticker']
                            strike_str = ticker_clean[-8:]
                            strike_raw = float(strike_str) / 1000.0
                            
                            if date < config['split_date']:
                                return round(strike_raw / config['split_ratio'], 2)
                            else:
                                return round(strike_raw, 2)
                        
                        df_calls_only['strike'] = df_calls_only.apply(adjust_strike, axis=1)
                        
                        # Merge with stock prices
                        def get_stock_price(date):
                            return stock_lookup.get(date, {
                                'underlying_close': None, 'underlying_open': None,
                                'underlying_high': None, 'underlying_low': None
                            })
                        
                        stock_data = df_calls_only['date_only_date'].apply(get_stock_price)
                        df_calls_only['underlying_close'] = [data['underlying_close'] for data in stock_data]
                        df_calls_only['underlying_open'] = [data['underlying_open'] for data in stock_data]
                        df_calls_only['underlying_high'] = [data['underlying_high'] for data in stock_data]
                        df_calls_only['underlying_low'] = [data['underlying_low'] for data in stock_data]
                        df_calls_only['underlying_spot'] = df_calls_only['underlying_close']
                        
                        # Fill missing data and calculate metrics
                        df_calls_only['underlying_open'] = df_calls_only['underlying_open'].fillna(df_calls_only['underlying_spot'])
                        df_calls_only['underlying_high'] = df_calls_only['underlying_high'].fillna(df_calls_only['underlying_spot'])
                        df_calls_only['underlying_low'] = df_calls_only['underlying_low'].fillna(df_calls_only['underlying_spot'])
                        
                        df_calls_only['underlying_close_at_expiry'] = df_calls_only['underlying_spot']
                        df_calls_only['underlying_high_at_expiry'] = df_calls_only['underlying_spot']
                        df_calls_only['underlying_spot_at_expiry'] = df_calls_only['underlying_spot']
                        
                        df_calls_only['otm_pct'] = ((df_calls_only['strike'] - df_calls_only['underlying_spot']) / df_calls_only['underlying_spot'] * 100).round(2)
                        df_calls_only['ITM'] = (df_calls_only['strike'] < df_calls_only['underlying_spot']).map({True: 'YES', False: 'NO'})
                        df_calls_only['premium'] = df_calls_only['close_price']
                        df_calls_only['premium_yield_pct'] = (df_calls_only['close_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                        df_calls_only['premium_low'] = df_calls_only['low_price']
                        df_calls_only['premium_yield_pct_low'] = (df_calls_only['low_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                        
                        # Clean and save
                        df_calls_only = df_calls_only.drop(['date_only_date'], axis=1, errors='ignore')
                        
                        column_order = [
                            'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
                            'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 'premium',
                            'premium_yield_pct', 'premium_low', 'premium_yield_pct_low', 'high_price', 'low_price',
                            'transactions', 'window_start', 'days_to_expiry', 'time_remaining_category',
                            'underlying_open', 'underlying_close', 'underlying_high', 'underlying_low', 'underlying_spot',
                            'underlying_close_at_expiry', 'underlying_high_at_expiry', 'underlying_spot_at_expiry'
                        ]
                        
                        for col in column_order:
                            if col not in df_calls_only.columns:
                                df_calls_only[col] = None
                        
                        df_calls_only = df_calls_only[column_order]
                        df_calls_only.to_csv(target_file, index=False)
                        
                        print(f"   ✅ Processed {len(df_calls_only):,} CALL options to {target_file}")
                        success_count += 1
                        
                    except Exception as e:
                        print(f"   ❌ Error processing {year} {period}: {e}")
                else:
                    print(f"   ⚠️  Parquet file not found: {parquet_file}")
        
        print(f"\n✅ {ticker} processing completed! Successfully processed {success_count} files")
    
    print(f"\n{'='*60}")
    print(f"🎉 ALL TICKERS PROCESSING COMPLETED!")
    print(f"{'='*60}")


if __name__ == "__main__":
    process_all_ticker_data()
