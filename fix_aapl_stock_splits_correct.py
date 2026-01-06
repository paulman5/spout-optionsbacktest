#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


def fix_aapl_stock_splits_correct():
    """Fix AAPL data with CORRECT stock split adjustments"""
    
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
        print(f"\n📅 Processing {year} data with CORRECT stock split adjustments...")
        
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
                
                # Apply CORRECT stock split adjustments to strikes
                def adjust_strike_aapl_correct(row):
                    """Apply CORRECT AAPL stock split adjustments based on date"""
                    date = pd.to_datetime(row['date_only'])
                    
                    # Parse strike from ticker
                    ticker_clean = row['ticker'][2:] if row['ticker'].startswith('O:') else row['ticker']
                    strike_str = ticker_clean[-8:]
                    strike_raw = float(strike_str) / 1000.0  # Divide by 1000 for OPRA format
                    
                    # Apply CORRECT stock split adjustments for AAPL
                    if date < pd.to_datetime('2020-08-31'):
                        # Pre-2020-08-31: divide by 4 (for 4-for-1 split on 2020-08-31)
                        adjusted_strike = strike_raw / 4.0
                    else:
                        # On or after 2020-08-31: divide by 1 (no adjustment needed)
                        adjusted_strike = strike_raw / 1.0
                    
                    return round(adjusted_strike, 2)
                
                # Apply CORRECT strike adjustments
                df['strike'] = df.apply(adjust_strike_aapl_correct, axis=1)
                
                # Manually merge stock prices using lookup
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
                
                print(f"   ✅ Fixed and saved {len(df)} rows to {file_path}")
                print(f"   📊 Date range: {df['date_only'].min()} to {df['date_only'].max()}")
                print(f"   💰 Stock price range: ${df['underlying_spot'].min():.2f} to ${df['underlying_spot'].max():.2f}")
                
                # Show CORRECT split adjustment examples
                if year == 2019:
                    sample_2019 = df[df['date_only'].dt.year == 2019].iloc[0]
                    print(f"   🔍 2019 example (pre-2020 split):")
                    print(f"      Strike: ${sample_2019['strike']:.2f} (divided by 4)")
                elif year == 2021:
                    sample_2021 = df[df['date_only'].dt.year == 2021].iloc[0]
                    print(f"   🔍 2021 example (post-2020 split):")
                    print(f"      Strike: ${sample_2021['strike']:.2f} (no adjustment)")
                
            except Exception as e:
                print(f"   ❌ Error fixing {year} {period}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\n✅ All AAPL files have been fixed with CORRECT stock split adjustments!")
    
    # Show CORRECT split adjustment summary
    print(f"\n📊 CORRECT Stock Split Adjustment Summary:")
    print(f"   Pre-2020-08-31: Strikes divided by 4 (for 4-for-1 split on 2020-08-31)")
    print(f"   On or after 2020-08-31: No adjustment needed")


if __name__ == "__main__":
    fix_aapl_stock_splits_correct()
