#!/usr/bin/env python3
"""
Fix AMZN 2022 strike prices by extracting them from ticker symbols and dividing by 1000.
Also apply the 20-for-1 stock split adjustment for data before June 6, 2022.
"""

import pandas as pd
import os
import re
from datetime import datetime

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol.
    Example: O:AMZN220218C03360000 -> 3360000
    """
    # Remove the prefix (O:AMZN) and option type (C/P)
    # The format is: O:AMZN + YYMMDD + C/P + 8-digit strike
    match = re.match(r'O:AMZN\d{6}[CP](\d{8})', ticker)
    if match:
        return int(match.group(1))
    return None

def fix_amzn_2022_data(file_path):
    """Fix strike prices and apply stock split adjustment to 2022 AMZN data."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Extract strike from ticker and divide by 1000
    df['strike_from_ticker'] = df['ticker'].apply(extract_strike_from_ticker)
    df['strike'] = df['strike_from_ticker'] / 1000
    
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
    
    # Drop the temporary column
    df = df.drop('strike_from_ticker', axis=1)
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the adjusted data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Fixed {len(df)} rows ({len(df[mask])} rows adjusted for stock split)")

def main():
    """Fix 2022 AMZN data strike prices."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    
    # Process monthly and weekly 2022 files
    files_to_process = [
        os.path.join(amzn_dir, "monthly", "2022_options_pessimistic.csv"),
        os.path.join(amzn_dir, "weekly", "2022_options_pessimistic.csv")
    ]
    
    for file_path in files_to_process:
        if os.path.exists(file_path):
            fix_amzn_2022_data(file_path)
        else:
            print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ Strike price fixes completed for 2022 AMZN data!")

if __name__ == "__main__":
    main()
