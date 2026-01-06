#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import subprocess
import sys


def process_amzn_from_parquet():
    """Process AMZN data from parquet files and download missing years"""
    
    print("🚀 Processing AMZN data from parquet files...")
    
    # Load AMZN historical data
    stock_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/HistoricalData_AMZN.csv")
    print("📈 Loading AMZN historical stock data...")
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
    
    # Create a lookup dictionary for faster access
    stock_lookup = {}
    for _, row in stock_df.iterrows():
        stock_lookup[row['date_only_date']] = {
            'underlying_close': row['underlying_close'],
            'underlying_open': row['underlying_open'],
            'underlying_high': row['underlying_high'],
            'underlying_low': row['underlying_low']
        }
    
    print(f"   Created stock price lookup for {len(stock_lookup)} dates")
    
    # Process available parquet files first
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    
    # Process 2025 data from parquet
    print(f"\n📅 Processing 2025 AMZN data from parquet...")
    
    for period in ['monthly', 'weekly']:
        parquet_file = data_dir / f"options_day_aggs_2025_{period}.parquet"
        target_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/{period}/2025_options_pessimistic.csv")
        
        if parquet_file.exists():
            try:
                # Load parquet data
                df = pd.read_parquet(parquet_file)
                print(f"   📊 Loaded {len(df)} rows from {period} parquet")
                
                # Count PUT and CALL options before removal
                put_count = len(df[df['option_type'] == 'P'])
                call_count = len(df[df['option_type'] == 'C'])
                print(f"      CALL options: {call_count}")
                print(f"      PUT options: {put_count}")
                
                # Remove PUT options (keep only CALL options)
                df_calls_only = df[df['option_type'] == 'C'].copy()
                removed_count = len(df) - len(df_calls_only)
                print(f"   ✅ Removed {removed_count} PUT options, kept {len(df_calls_only)} CALL options")
                
                # Process dates
                df_calls_only['date_only'] = pd.to_datetime(df_calls_only['date_only'])
                df_calls_only['date_only_date'] = df_calls_only['date_only'].dt.date
                
                # Apply AMZN stock split adjustments
                def adjust_strike_amzn(row):
                    """Apply AMZN stock split adjustments based on date"""
                    date = pd.to_datetime(row['date_only'])
                    
                    # Parse strike from ticker
                    ticker_clean = row['ticker'][2:] if row['ticker'].startswith('O:') else row['ticker']
                    strike_str = ticker_clean[-8:]
                    strike_raw = float(strike_str) / 1000.0  # Divide by 1000 for OPRA format
                    
                    # AMZN stock split history:
                    # - 2-for-1 on June 2, 1998
                    # - 3-for-1 on January 5, 1999  
                    # - 2-for-1 on September 1, 2006
                    # - 2-for-1 on August 27, 2022
                    
                    if date < pd.to_datetime('2022-08-27'):
                        # Pre-2022-08-27: divide by 2 (for 2-for-1 split on 2022-08-27)
                        adjusted_strike = strike_raw / 2.0
                    else:
                        # On or after 2022-08-27: no adjustment needed
                        adjusted_strike = strike_raw / 1.0
                    
                    return round(adjusted_strike, 2)
                
                # Apply strike adjustments
                df_calls_only['strike'] = df_calls_only.apply(adjust_strike_amzn, axis=1)
                
                # Merge with stock prices
                def get_stock_price(date):
                    if date in stock_lookup:
                        return stock_lookup[date]
                    else:
                        return {
                            'underlying_close': None,
                            'underlying_open': None,
                            'underlying_high': None,
                            'underlying_low': None
                        }
                
                # Apply stock prices
                stock_data = df_calls_only['date_only_date'].apply(get_stock_price)
                df_calls_only['underlying_close'] = [data['underlying_close'] for data in stock_data]
                df_calls_only['underlying_open'] = [data['underlying_open'] for data in stock_data]
                df_calls_only['underlying_high'] = [data['underlying_high'] for data in stock_data]
                df_calls_only['underlying_low'] = [data['underlying_low'] for data in stock_data]
                
                # Set underlying_spot to underlying_close
                df_calls_only['underlying_spot'] = df_calls_only['underlying_close']
                
                # Count missing prices
                missing_prices = df_calls_only['underlying_close'].isna().sum()
                if missing_prices > 0:
                    print(f"   ⚠️  {missing_prices} rows have missing stock prices")
                    # Use fallback for missing data
                    df_calls_only['underlying_spot'] = df_calls_only.apply(
                        lambda row: row['strike'] + row['close_price'] if pd.isna(row['underlying_close']) else row['underlying_close'],
                        axis=1
                    )
                
                # Fill missing price columns
                df_calls_only['underlying_open'] = df_calls_only['underlying_open'].fillna(df_calls_only['underlying_spot'])
                df_calls_only['underlying_high'] = df_calls_only['underlying_high'].fillna(df_calls_only['underlying_spot'])
                df_calls_only['underlying_low'] = df_calls_only['underlying_low'].fillna(df_calls_only['underlying_spot'])
                
                # Add expiration prices
                df_calls_only['underlying_close_at_expiry'] = df_calls_only['underlying_spot']
                df_calls_only['underlying_high_at_expiry'] = df_calls_only['underlying_spot']
                df_calls_only['underlying_spot_at_expiry'] = df_calls_only['underlying_spot']
                
                # Calculate OTM percentage
                df_calls_only['otm_pct'] = ((df_calls_only['strike'] - df_calls_only['underlying_spot']) / df_calls_only['underlying_spot'] * 100).round(2)
                df_calls_only['ITM'] = (df_calls_only['strike'] < df_calls_only['underlying_spot']).map({True: 'YES', False: 'NO'})
                
                # Calculate premiums
                df_calls_only['premium'] = df_calls_only['close_price']
                df_calls_only['premium_yield_pct'] = (df_calls_only['close_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                df_calls_only['premium_low'] = df_calls_only['low_price']
                df_calls_only['premium_yield_pct_low'] = (df_calls_only['low_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                
                # Remove temporary columns
                df_calls_only = df_calls_only.drop(['date_only_date'], axis=1, errors='ignore')
                
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
                    if col not in df_calls_only.columns:
                        df_calls_only[col] = None
                
                df_calls_only = df_calls_only[column_order]
                
                # Save result
                df_calls_only.to_csv(target_file, index=False)
                
                print(f"   ✅ Processed and saved {len(df_calls_only)} rows to {target_file}")
                print(f"   📊 Date range: {df_calls_only['date_only'].min()} to {df_calls_only['date_only'].max()}")
                print(f"   💰 Stock price range: ${df_calls_only['underlying_spot'].min():.2f} to ${df_calls_only['underlying_spot'].max():.2f}")
                
                # Show split adjustment examples
                sample_2025 = df_calls_only[df_calls_only['date_only'].dt.year == 2025].iloc[0] if len(df_calls_only[df_calls_only['date_only'].dt.year == 2025]) > 0 else None
                if sample_2025 is not None:
                    print(f"   🔍 2025 example (post-2022 split):")
                    print(f"      Strike: ${sample_2025['strike']:.2f} (no adjustment)")
                
            except Exception as e:
                print(f"   ❌ Error processing 2025 {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ AMZN 2025 data processing completed!")
    
    # Now download missing years 2016-2024
    print(f"\n🚀 Downloading missing AMZN years 2016-2024...")
    
    years_to_download = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    
    for year in years_to_download:
        print(f"\n📅 Downloading year {year}...")
        
        # Update aggregate.py for this year
        aggregate_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py")
        with open(aggregate_file, 'r') as f:
            content = f.read()
        
        # Update TEST_YEAR
        current_year = content.split('TEST_YEAR = ')[1].split('\n')[0]
        content = content.replace(f"TEST_YEAR = {current_year}", f"TEST_YEAR = {year}")
        
        with open(aggregate_file, 'w') as f:
            f.write(content)
        
        print(f"   🔄 Set TEST_YEAR to {year}")
        
        # Remove existing duckdb file
        duckdb_path = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options.duckdb")
        if duckdb_path.exists():
            duckdb_path.unlink()
            print(f"   🗑️  Removed existing database")
        
        # Run aggregation script
        print(f"   📦 Downloading {year} data...")
        try:
            result = subprocess.run([
                sys.executable, "aggregate.py"
            ], cwd="/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data",
                capture_output=True, text=True, timeout=1800)  # 30 minute timeout
            
            if result.returncode == 0:
                print(f"   ✅ Successfully downloaded {year} data")
                
                # Check if parquet files were created and process them immediately
                for period in ['monthly', 'weekly']:
                    parquet_file = data_dir / f"options_day_aggs_{year}_{period}.parquet"
                    target_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/{period}/{year}_options_pessimistic.csv")
                    
                    if parquet_file.exists():
                        print(f"   🔄 Processing {year} {period} data...")
                        
                        # Load and process the parquet file (same logic as above)
                        df = pd.read_parquet(parquet_file)
                        
                        # Remove PUT options
                        df_calls_only = df[df['option_type'] == 'C'].copy()
                        
                        # Process dates
                        df_calls_only['date_only'] = pd.to_datetime(df_calls_only['date_only'])
                        df_calls_only['date_only_date'] = df_calls_only['date_only'].dt.date
                        
                        # Apply strike adjustments
                        df_calls_only['strike'] = df_calls_only.apply(adjust_strike_amzn, axis=1)
                        
                        # Merge with stock prices
                        stock_data = df_calls_only['date_only_date'].apply(get_stock_price)
                        df_calls_only['underlying_close'] = [data['underlying_close'] for data in stock_data]
                        df_calls_only['underlying_open'] = [data['underlying_open'] for data in stock_data]
                        df_calls_only['underlying_high'] = [data['underlying_high'] for data in stock_data]
                        df_calls_only['underlying_low'] = [data['underlying_low'] for data in stock_data]
                        df_calls_only['underlying_spot'] = df_calls_only['underlying_close']
                        
                        # Fill missing data
                        df_calls_only['underlying_open'] = df_calls_only['underlying_open'].fillna(df_calls_only['underlying_spot'])
                        df_calls_only['underlying_high'] = df_calls_only['underlying_high'].fillna(df_calls_only['underlying_spot'])
                        df_calls_only['underlying_low'] = df_calls_only['underlying_low'].fillna(df_calls_only['underlying_spot'])
                        
                        # Add expiration prices
                        df_calls_only['underlying_close_at_expiry'] = df_calls_only['underlying_spot']
                        df_calls_only['underlying_high_at_expiry'] = df_calls_only['underlying_spot']
                        df_calls_only['underlying_spot_at_expiry'] = df_calls_only['underlying_spot']
                        
                        # Calculate metrics
                        df_calls_only['otm_pct'] = ((df_calls_only['strike'] - df_calls_only['underlying_spot']) / df_calls_only['underlying_spot'] * 100).round(2)
                        df_calls_only['ITM'] = (df_calls_only['strike'] < df_calls_only['underlying_spot']).map({True: 'YES', False: 'NO'})
                        df_calls_only['premium'] = df_calls_only['close_price']
                        df_calls_only['premium_yield_pct'] = (df_calls_only['close_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                        df_calls_only['premium_low'] = df_calls_only['low_price']
                        df_calls_only['premium_yield_pct_low'] = (df_calls_only['low_price'] / df_calls_only['underlying_spot'] * 100).round(2)
                        
                        # Clean up and save
                        df_calls_only = df_calls_only.drop(['date_only_date'], axis=1, errors='ignore')
                        
                        # Reorder columns
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
                        
                        print(f"   ✅ Processed {len(df_calls_only)} rows to {target_file}")
                        
            else:
                print(f"   ❌ Failed to download {year} data")
                print(f"   Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Timeout downloading {year} data")
        except Exception as e:
            print(f"   ❌ Error downloading {year} data: {e}")
    
    print(f"\n✅ AMZN complete processing finished!")


if __name__ == "__main__":
    process_amzn_from_parquet()
