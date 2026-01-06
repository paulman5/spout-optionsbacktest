#!/usr/bin/env python3
"""
Round all numeric columns to 2 decimal places for 2022 AMZN data, except window_start.
"""

import pandas as pd
import os
import numpy as np

def round_numeric_columns(file_path):
    """Round numeric columns to 2 decimal places except window_start."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Get numeric columns excluding window_start
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    columns_to_round = [col for col in numeric_columns if col != 'window_start']
    
    print(f"  Columns to round: {columns_to_round}")
    
    # Round each column to 2 decimal places
    for col in columns_to_round:
        df[col] = df[col].round(2)
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the rounded data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Rounded {len(df)} rows")

def main():
    """Round numeric columns for 2022 AMZN data."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    
    # Process monthly and weekly 2022 files
    files_to_process = [
        os.path.join(amzn_dir, "monthly", "2022_options_pessimistic.csv"),
        os.path.join(amzn_dir, "weekly", "2022_options_pessimistic.csv")
    ]
    
    for file_path in files_to_process:
        if os.path.exists(file_path):
            round_numeric_columns(file_path)
        else:
            print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ Rounding completed for 2022 AMZN data!")

if __name__ == "__main__":
    main()
