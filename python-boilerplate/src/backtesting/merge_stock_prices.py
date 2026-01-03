"""
Script to merge historical stock prices with options data.

This script loads the historical stock price CSV and merges it with
both weekly and monthly options datasets.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import backtest module
sys.path.insert(0, str(Path(__file__).parent.parent))

from backtesting.backtest import load_options_data, add_underlying_prices_from_csv

def main():
    # Paths
    base_dir = Path(__file__).parent.parent.parent / "data" / "TSLA"
    stock_csv = base_dir / "HistoricalData_1767476795814.csv"
    
    weekly_options = base_dir / "monthly" / "options_day_aggs_2019_weekly.csv"
    monthly_options = base_dir / "weekly" / "options_day_aggs_2019_monthly.csv"
    
    # Output paths (pessimistic scenario - using HIGH prices)
    weekly_output = base_dir / "monthly" / "options_day_aggs_2019_weekly_with_prices_pessimistic.csv"
    monthly_output = base_dir / "weekly" / "options_day_aggs_2019_monthly_with_prices_pessimistic.csv"
    
    print("🔄 Merging stock prices with options data...\n")
    
    # Process weekly options
    if weekly_options.exists():
        print(f"📊 Processing weekly options: {weekly_options.name}")
        print(f"   Loading options data...")
        df_weekly = load_options_data(str(weekly_options))
        print(f"   Loaded {len(df_weekly):,} rows")
        
        print(f"   Merging with stock prices (pessimistic scenario: using HIGH prices)...")
        df_weekly = add_underlying_prices_from_csv(df_weekly, str(stock_csv), symbol='TSLA', use_pessimistic=True)
        
        print(f"   Saving to {weekly_output.name}...")
        df_weekly.to_csv(weekly_output, index=False)
        print(f"✅ Saved {len(df_weekly):,} rows to {weekly_output}\n")
    else:
        print(f"⚠️  Weekly options file not found: {weekly_options}\n")
    
    # Process monthly options
    if monthly_options.exists():
        print(f"📊 Processing monthly options: {monthly_options.name}")
        print(f"   Loading options data...")
        df_monthly = load_options_data(str(monthly_options))
        print(f"   Loaded {len(df_monthly):,} rows")
        
        print(f"   Merging with stock prices (pessimistic scenario: using HIGH prices)...")
        df_monthly = add_underlying_prices_from_csv(df_monthly, str(stock_csv), symbol='TSLA', use_pessimistic=True)
        
        print(f"   Saving to {monthly_output.name}...")
        df_monthly.to_csv(monthly_output, index=False)
        print(f"✅ Saved {len(df_monthly):,} rows to {monthly_output}\n")
    else:
        print(f"⚠️  Monthly options file not found: {monthly_options}\n")
    
    print("✅ Done!")

if __name__ == "__main__":
    main()

