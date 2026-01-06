"""
Fetch 2015 TSLA stock prices and add to historical stock prices CSV.
"""

import pandas as pd
import sys
from pathlib import Path
import importlib.util

# Import monthly.py functions
monthly_path = Path(__file__).parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

fetch_underlying_prices = monthly.fetch_underlying_prices
load_historical_stock_prices = monthly.load_historical_stock_prices


def fetch_2015_prices():
    """Fetch 2015 TSLA stock prices using monthly.py function."""
    print("📈 Fetching 2015 TSLA stock prices...")
    
    try:
        stock_prices = fetch_underlying_prices("TSLA", "2015-01-01", "2016-01-01")
        
        if stock_prices.empty:
            raise ValueError("No price data found for TSLA in 2015")
        
        # Format to match existing CSV format (MM/DD/YYYY, $ prices)
        result = stock_prices.copy()
        result['Date'] = pd.to_datetime(result['date']).dt.strftime('%m/%d/%Y')
        result['Close/Last'] = result['close'].apply(lambda x: f"${x:.2f}")
        result['Open'] = result['open'].apply(lambda x: f"${x:.2f}")
        result['High'] = result['high'].apply(lambda x: f"${x:.2f}")
        result['Low'] = result['low'].apply(lambda x: f"${x:.2f}")
        result['Volume'] = result['volume'].astype(int)
        
        # Select and rename columns to match existing format
        result = result[['Date', 'Open', 'High', 'Low', 'Close/Last', 'Volume']].copy()
        
        print(f"✅ Fetched {len(result)} days of 2015 data")
        print(f"   Date range: {result['Date'].min()} to {result['Date'].max()}")
        
        return result
    except Exception as e:
        print(f"❌ Error fetching 2015 prices: {e}")
        raise


def main():
    stock_file = Path("data/TSLA/HistoricalData_1767476795814.csv")
    
    if not stock_file.exists():
        raise FileNotFoundError(f"Stock prices file not found: {stock_file}")
    
    print("=" * 80)
    print("ADDING 2015 STOCK PRICES")
    print("=" * 80)
    print(f"  Stock file: {stock_file}")
    
    # Load existing stock prices
    print("\n📊 Loading existing stock prices...")
    existing = load_historical_stock_prices(str(stock_file))
    
    # Convert back to original format for saving
    existing_df = pd.read_csv(stock_file)
    print(f"   Loaded {len(existing_df)} rows")
    print(f"   Current date range: {existing_df['Date'].min()} to {existing_df['Date'].max()}")
    
    # Fetch 2015 prices
    prices_2015 = fetch_2015_prices()
    
    # Combine and sort by date
    print("\n🔄 Combining datasets...")
    combined = pd.concat([prices_2015, existing_df], ignore_index=True)
    
    # Sort by date (convert to datetime for sorting, then back to string)
    combined['Date_dt'] = pd.to_datetime(combined['Date'], format='%m/%d/%Y')
    combined = combined.sort_values('Date_dt', ascending=False).reset_index(drop=True)  # Newest first (like original)
    combined = combined.drop(columns=['Date_dt'])
    
    # Save
    print(f"\n💾 Saving updated stock prices to {stock_file}...")
    combined.to_csv(stock_file, index=False)
    
    print(f"✅ Saved {len(combined)} rows (added {len(prices_2015)} days from 2015)")
    print(f"   New date range: {combined['Date'].min()} to {combined['Date'].max()}")


if __name__ == "__main__":
    main()
