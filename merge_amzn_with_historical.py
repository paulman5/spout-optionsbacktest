#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


def merge_amzn_with_historical():
    """Merge AMZN options data with historical stock prices and apply stock splits"""
    
    print("🚀 Merging AMZN data with historical stock prices...")
    
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
            file_path = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/{period}/{year}_options_pessimistic.csv")
            
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
                df['strike'] = df.apply(adjust_strike_amzn, axis=1)
                
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
                stock_data = df['date_only_date'].apply(get_stock_price)
                df['underlying_close'] = [data['underlying_close'] for data in stock_data]
                df['underlying_open'] = [data['underlying_open'] for data in stock_data]
                df['underlying_high'] = [data['underlying_high'] for data in stock_data]
                df['underlying_low'] = [data['underlying_low'] for data in stock_data]
                
                # Set underlying_spot to underlying_close
                df['underlying_spot'] = df['underlying_close']
                
                # Count missing prices
                missing_prices = df['underlying_close'].isna().sum()
                if missing_prices > 0:
                    print(f"   ⚠️  {missing_prices} rows have missing stock prices")
                    # Use fallback for missing data
                    df['underlying_spot'] = df.apply(
                        lambda row: row['strike'] + row['close_price'] if pd.isna(row['underlying_close']) else row['underlying_close'],
                        axis=1
                    )
                
                # Fill missing price columns
                df['underlying_open'] = df['underlying_open'].fillna(df['underlying_spot'])
                df['underlying_high'] = df['underlying_high'].fillna(df['underlying_spot'])
                df['underlying_low'] = df['underlying_low'].fillna(df['underlying_spot'])
                
                # Add expiration prices
                df['underlying_close_at_expiry'] = df['underlying_spot']
                df['underlying_high_at_expiry'] = df['underlying_spot']
                df['underlying_spot_at_expiry'] = df['underlying_spot']
                
                # Calculate OTM percentage
                df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
                df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
                
                # Calculate premiums
                df['premium'] = df['close_price']
                df['premium_yield_pct'] = (df['close_price'] / df['underlying_spot'] * 100).round(2)
                df['premium_low'] = df['low_price']
                df['premium_yield_pct_low'] = (df['low_price'] / df['underlying_spot'] * 100).round(2)
                
                # Remove temporary columns
                df = df.drop(['date_only_date'], axis=1, errors='ignore')
                
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
                    if col not in df.columns:
                        df[col] = None
                
                df = df[column_order]
                
                # Save result
                df.to_csv(file_path, index=False)
                
                print(f"   ✅ Merged and saved {len(df)} rows to {file_path}")
                print(f"   📊 Date range: {df['date_only'].min()} to {df['date_only'].max()}")
                print(f"   💰 Stock price range: ${df['underlying_spot'].min():.2f} to ${df['underlying_spot'].max():.2f}")
                
                # Show split adjustment examples
                if year == 2021:
                    sample_2021 = df[df['date_only'].dt.year == 2021].iloc[0] if len(df[df['date_only'].dt.year == 2021]) > 0 else None
                    if sample_2021 is not None:
                        print(f"   🔍 2021 example (pre-2022 split):")
                        print(f"      Strike: ${sample_2021['strike']:.2f} (divided by 2)")
                elif year == 2023:
                    sample_2023 = df[df['date_only'].dt.year == 2023].iloc[0] if len(df[df['date_only'].dt.year == 2023]) > 0 else None
                    if sample_2023 is not None:
                        print(f"   🔍 2023 example (post-2022 split):")
                        print(f"      Strike: ${sample_2023['strike']:.2f} (no adjustment)")
                
            except Exception as e:
                print(f"   ❌ Error processing {year} {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ AMZN data merging completed!")
    
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
            
            # Check if stock data columns exist
            has_stock_data = all(col in df.columns for col in ['underlying_spot', 'otm_pct', 'ITM', 'premium_yield_pct'])
            status = "✅" if put_count == 0 and has_stock_data else "❌"
            
            stock_status = "✅" if has_stock_data else "❌"
            puts_status = "✅" if put_count == 0 else "❌"
            
            print(f"     {status} {year}: {total_count} total (CALL: {call_count}, PUT: {put_count}) [Stock: {stock_status}, No PUTs: {puts_status}]")


if __name__ == "__main__":
    merge_amzn_with_historical()
