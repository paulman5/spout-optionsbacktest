#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import re


def process_amzn_complete():
    """Complete AMZN data processing: remove PUTs, apply stock splits, merge with historical data"""
    
    print("🚀 Processing AMZN data with complete pipeline...")
    
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
    
    # Process all years (2016-2025)
    years_to_process = list(range(2016, 2026))
    
    for year in years_to_process:
        print(f"\n📅 Processing {year} AMZN data...")
        
        # Check both monthly and weekly files
        for period in ['monthly', 'weekly']:
            # Try to find the source file
            source_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_{year}_{period}.csv")
            target_file = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/{period}/{year}_options_pessimistic.csv")
            
            if not source_file.exists():
                print(f"   ⚠️  Source file not found: {source_file}")
                continue
            
            try:
                # Load source data
                df = pd.read_csv(source_file)
                print(f"   📊 Loaded {len(df)} rows from {period} source")
                
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
                if year == 2021:
                    sample_2021 = df_calls_only[df_calls_only['date_only'].dt.year == 2021].iloc[0] if len(df_calls_only[df_calls_only['date_only'].dt.year == 2021]) > 0 else None
                    if sample_2021 is not None:
                        print(f"   🔍 2021 example (pre-2022 split):")
                        print(f"      Strike: ${sample_2021['strike']:.2f} (divided by 2)")
                elif year == 2023:
                    sample_2023 = df_calls_only[df_calls_only['date_only'].dt.year == 2023].iloc[0] if len(df_calls_only[df_calls_only['date_only'].dt.year == 2023]) > 0 else None
                    if sample_2023 is not None:
                        print(f"   🔍 2023 example (post-2022 split):")
                        print(f"      Strike: ${sample_2023['strike']:.2f} (no adjustment)")
                
            except Exception as e:
                print(f"   ❌ Error processing {year} {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ AMZN data processing completed!")
    
    # Show stock split adjustment summary
    print(f"\n📊 AMZN Stock Split Adjustment Summary:")
    print(f"   Pre-2022-08-27: Strikes divided by 2 (for 2-for-1 split on 2022-08-27)")
    print(f"   On or after 2022-08-27: No adjustment needed")
    
    # Final verification
    print(f"\n🔍 Final verification:")
    monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/monthly")
    weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/weekly")
    
    for period in ['monthly', 'weekly']:
        dir_path = monthly_dir if period == 'monthly' else weekly_dir
        files = sorted(dir_path.glob("*.csv"))
        
        print(f"\n   {period.capitalize()} files:")
        for f in files:
            year = f.stem.split('_')[0]
            df = pd.read_csv(f)
            call_count = len(df[df['option_type'] == 'C'])
            put_count = len(df[df['option_type'] == 'P'])
            total_count = len(df)
            
            status = "✅" if put_count == 0 else "❌"
            print(f"     {status} {year}: {total_count} total (CALL: {call_count}, PUT: {put_count})")


if __name__ == "__main__":
    process_amzn_complete()
