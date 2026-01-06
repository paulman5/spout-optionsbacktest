#!/usr/bin/env python3
"""
Merge historical stock data with AMZN options data for 2022-2025.
"""

import pandas as pd
import os
from datetime import datetime

def load_historical_data():
    """Load historical AMZN stock data."""
    print("Loading historical AMZN stock data...")
    
    # Try to find the historical data file
    hist_file = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/HistoricalData_AMZN.csv"
    
    if not os.path.exists(hist_file):
        print(f"❌ Historical data file not found: {hist_file}")
        return None
    
    df = pd.read_csv(hist_file)
    
    # Convert date column to datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    elif 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.rename(columns={'Date': 'date'})
    else:
        print("❌ No date column found in historical data")
        return None
    
    print(f"✅ Loaded {len(df)} rows of historical data")
    return df

def merge_historical_with_options(options_file, historical_df, output_file):
    """Merge historical stock data with options data."""
    print(f"Processing {options_file}...")
    
    # Load options data
    options_df = pd.read_csv(options_file)
    
    if options_df.empty:
        print(f"  Options file is empty, skipping...")
        return
    
    # Convert date_only to datetime
    options_df['date_only'] = pd.to_datetime(options_df['date_only'])
    
    # Merge with historical data on date
    merged_df = options_df.merge(historical_df, left_on='date_only', right_on='date', how='left')
    
    # Calculate additional metrics
    if 'close' in merged_df.columns and 'strike' in merged_df.columns:
        # OTM (Out of The Money) percentage for calls
        merged_df['otm_pct'] = ((merged_df['strike'] - merged_df['close']) / merged_df['close']) * 100
        
        # Premium yield percentage
        merged_df['premium_yield'] = (merged_df['close_price'] / merged_df['strike']) * 100
    
    # Sort by date and strike
    merged_df = merged_df.sort_values(['date_only', 'strike'])
    
    # Save merged data
    merged_df.to_csv(output_file, index=False)
    print(f"  ✅ Merged {len(merged_df)} rows")

def main():
    """Merge historical data with AMZN options data for 2022-2025."""
    # Load historical data
    historical_df = load_historical_data()
    
    if historical_df is None:
        print("❌ Cannot proceed without historical data")
        return
    
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    years = [2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            {
                'options': os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv"),
                'output': os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv")
            },
            {
                'options': os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv"),
                'output': os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv")
            }
        ]
        
        for file_info in files_to_process:
            if os.path.exists(file_info['options']):
                merge_historical_with_options(file_info['options'], historical_df, file_info['output'])
            else:
                print(f"⚠️ Options file not found: {file_info['options']}")
    
    print("\n✅ Historical data merged with AMZN options data for 2022-2025!")

if __name__ == "__main__":
    main()
