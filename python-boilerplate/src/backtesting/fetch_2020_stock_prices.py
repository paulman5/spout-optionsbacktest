"""
Fetch 2020 TSLA stock prices and add to historical stock prices CSV.
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


def fetch_2020_prices():
    """Fetch 2020 TSLA stock prices using monthly.py function."""
    print("📈 Fetching 2020 TSLA stock prices...")
    
    try:
        stock_prices = fetch_underlying_prices("TSLA", "2020-01-01", "2021-01-01")
        
        if stock_prices.empty:
            raise ValueError("No price data found for TSLA in 2020")
        
        # Format to match existing CSV format (MM/DD/YYYY, $ prices)
        stock_prices_formatted = stock_prices.copy()
        stock_prices_formatted['Date'] = pd.to_datetime(stock_prices_formatted['Date']).dt.strftime('%m/%d/%Y')
        stock_prices_formatted['Close'] = stock_prices_formatted['Close'].round(4)
        
        # Save to data directory
        output_path = Path("data/TSLA_stock_prices_2020.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        stock_prices_formatted.to_csv(output_path, index=False)
        
        print(f"✅ Saved {len(stock_prices_formatted)} price records to {output_path}")
        print(f"📅 Date range: {stock_prices_formatted['Date'].iloc[0]} to {stock_prices_formatted['Date'].iloc[-1]}")
        
        return stock_prices_formatted
        
    except Exception as e:
        print(f"❌ Error fetching 2020 prices: {e}")
        return None


if __name__ == "__main__":
    fetch_2020_prices()
