"""
Fix IWM holidays CSV files to match with HistoricalData_IWM.csv
Updates underlying price columns (underlying_open, underlying_close, underlying_high, underlying_low, underlying_spot)
by matching dates with the historical data file.
"""

import pandas as pd
from pathlib import Path
import sys

# Add the monthly.py path to import the load function
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting" / "weekly-monthly"))
from monthly import load_historical_stock_prices, add_underlying_prices_from_csv


def fix_holidays_file(holidays_file: Path, historical_data_file: Path):
    """
    Fix a single holidays CSV file by matching with historical data.
    
    Args:
        holidays_file: Path to the holidays CSV file
        historical_data_file: Path to the HistoricalData CSV file
    """
    print(f"\n{'='*80}")
    print(f"Processing: {holidays_file.name}")
    print(f"{'='*80}")
    
    # Load holidays data
    print(f"📂 Loading holidays data...")
    holidays_df = pd.read_csv(holidays_file)
    print(f"   Loaded {len(holidays_df):,} rows")
    
    # Check date range
    holidays_df['date_only_dt'] = pd.to_datetime(holidays_df['date_only'])
    min_date = holidays_df['date_only_dt'].min()
    max_date = holidays_df['date_only_dt'].max()
    print(f"   Date range: {min_date.date()} to {max_date.date()}")
    
    # Load historical stock prices
    print(f"📈 Loading historical stock prices...")
    stock_prices = load_historical_stock_prices(str(historical_data_file))
    print(f"   Loaded {len(stock_prices):,} days of stock price data")
    print(f"   Date range: {stock_prices['date'].min()} to {stock_prices['date'].max()}")
    
    # Convert date_only to date for joining
    holidays_df['date_only_date'] = pd.to_datetime(holidays_df['date_only']).dt.date
    
    # Check how many dates match
    matching_dates = holidays_df['date_only_date'].isin(stock_prices['date'])
    match_count = matching_dates.sum()
    total_count = len(holidays_df)
    print(f"   Matching dates: {match_count:,} / {total_count:,} ({100*match_count/total_count:.1f}%)")
    
    if match_count < total_count:
        missing_dates = holidays_df[~matching_dates]['date_only_date'].unique()
        print(f"   ⚠️  Missing dates: {len(missing_dates)} unique dates not found in historical data")
        if len(missing_dates) <= 10:
            for md in sorted(missing_dates):
                print(f"      - {md}")
        else:
            for md in sorted(missing_dates)[:10]:
                print(f"      - {md}")
            print(f"      ... and {len(missing_dates) - 10} more")
    
    # Manually match and update underlying prices
    print(f"🔄 Matching and updating underlying prices...")
    try:
        # Create a mapping from date to stock prices
        stock_price_map = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
        
        # Update underlying prices for trading dates
        def get_stock_price(row, price_type):
            date = row['date_only_date']
            if date in stock_price_map:
                return stock_price_map[date].get(price_type)
            return None
        
        holidays_df['underlying_open'] = holidays_df.apply(lambda row: get_stock_price(row, 'open'), axis=1)
        holidays_df['underlying_close'] = holidays_df.apply(lambda row: get_stock_price(row, 'close'), axis=1)
        holidays_df['underlying_high'] = holidays_df.apply(lambda row: get_stock_price(row, 'high'), axis=1)
        holidays_df['underlying_low'] = holidays_df.apply(lambda row: get_stock_price(row, 'low'), axis=1)
        
        # Set underlying_spot (pessimistic = high)
        holidays_df['underlying_spot'] = holidays_df['underlying_high']
        
        # Update expiration date prices
        if 'expiration_date' in holidays_df.columns:
            holidays_df['expiration_date_date'] = pd.to_datetime(holidays_df['expiration_date']).dt.date
            
            def get_exp_price(row, price_type):
                date = row['expiration_date_date']
                if date in stock_price_map:
                    return stock_price_map[date].get(price_type)
                return None
            
            holidays_df['underlying_close_at_expiry'] = holidays_df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
            holidays_df['underlying_high_at_expiry'] = holidays_df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
            holidays_df['underlying_spot_at_expiry'] = holidays_df['underlying_high_at_expiry']  # pessimistic
            
            # Drop temporary column
            holidays_df = holidays_df.drop(columns=['expiration_date_date'], errors='ignore')
        
        # Drop temporary columns
        holidays_df_updated = holidays_df.drop(columns=['date_only_dt', 'date_only_date'], errors='ignore')
        
        # Check how many rows have underlying prices filled
        has_open = holidays_df_updated['underlying_open'].notna().sum()
        has_close = holidays_df_updated['underlying_close'].notna().sum()
        has_high = holidays_df_updated['underlying_high'].notna().sum()
        has_low = holidays_df_updated['underlying_low'].notna().sum()
        has_spot = holidays_df_updated['underlying_spot'].notna().sum()
        
        print(f"   Rows with underlying prices:")
        print(f"      - underlying_open: {has_open:,} / {total_count:,}")
        print(f"      - underlying_close: {has_close:,} / {total_count:,}")
        print(f"      - underlying_high: {has_high:,} / {total_count:,}")
        print(f"      - underlying_low: {has_low:,} / {total_count:,}")
        print(f"      - underlying_spot: {has_spot:,} / {total_count:,}")
        
        # Save updated file
        print(f"💾 Saving updated file...")
        holidays_df_updated.to_csv(holidays_file, index=False)
        print(f"   ✅ Saved {holidays_file}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error updating file: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main function to fix all IWM holidays files."""
    base_path = Path(__file__).parent / "python-boilerplate" / "data" / "IWM"
    holidays_dir = base_path / "holidays"
    historical_data_file = base_path / "HistoricalData_IWM.csv"
    
    if not historical_data_file.exists():
        print(f"❌ Historical data file not found: {historical_data_file}")
        return
    
    if not holidays_dir.exists():
        print(f"❌ Holidays directory not found: {holidays_dir}")
        return
    
    # Get all holidays CSV files
    holidays_files = sorted(holidays_dir.glob("*_options_pessimistic.csv"))
    
    if not holidays_files:
        print(f"❌ No holidays CSV files found in {holidays_dir}")
        return
    
    print(f"Found {len(holidays_files)} holidays files to process")
    
    # Process each file
    success_count = 0
    for holidays_file in holidays_files:
        if fix_holidays_file(holidays_file, historical_data_file):
            success_count += 1
    
    print(f"\n{'='*80}")
    print(f"✅ Processing complete!")
    print(f"   Successfully updated: {success_count} / {len(holidays_files)} files")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

