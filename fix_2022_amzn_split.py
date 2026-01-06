#!/usr/bin/env python3
"""
Apply Amazon stock split adjustments to the newly downloaded 2022 data.
The 20-for-1 split occurred on June 6, 2022, so we need to divide strike prices
by 20 for data before that date.
"""

import pandas as pd
import os
from datetime import datetime

def apply_stock_split_to_file(file_path, output_path):
    """Apply stock split adjustment to a CSV file."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Convert date_only to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Apply 20-for-1 split to data before June 6, 2022
    split_date = pd.to_datetime('2022-06-06')
    mask = df['date_only'] < split_date
    
    # Divide strike prices by 20 for pre-split data
    df.loc[mask, 'strike'] = df.loc[mask, 'strike'] / 20
    
    # Also divide price columns by 20 for pre-split data
    price_columns = ['open_price', 'close_price', 'high_price', 'low_price']
    for col in price_columns:
        if col in df.columns:
            df.loc[mask, col] = df.loc[mask, col] / 20
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the adjusted data
    df.to_csv(output_path, index=False)
    print(f"  ✅ Applied stock split adjustment to {len(df[mask])} rows")

def main():
    """Apply stock split adjustments to 2022 AMZN data."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    
    # Process monthly and weekly 2022 files
    files_to_process = [
        os.path.join(amzn_dir, "monthly", "2022_options_pessimistic.csv"),
        os.path.join(amzn_dir, "weekly", "2022_options_pessimistic.csv")
    ]
    
    for file_path in files_to_process:
        if os.path.exists(file_path):
            apply_stock_split_to_file(file_path, file_path)
        else:
            print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ Stock split adjustments completed for 2022 AMZN data!")

if __name__ == "__main__":
    main()
