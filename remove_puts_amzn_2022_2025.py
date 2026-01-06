#!/usr/bin/env python3
"""
Remove PUT options from AMZN data for years 2022-2025.
Keep only CALL options (option_type = 'C').
"""

import pandas as pd
import os

def remove_put_options(file_path):
    """Remove PUT options from a CSV file, keeping only CALL options."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Count original rows
    original_count = len(df)
    
    # Filter to keep only CALL options (option_type = 'C')
    df_calls = df[df['option_type'] == 'C'].copy()
    
    # Count filtered rows
    filtered_count = len(df_calls)
    removed_count = original_count - filtered_count
    
    # Sort by date and strike
    df_calls = df_calls.sort_values(['date_only', 'strike'])
    
    # Save the filtered data
    df_calls.to_csv(file_path, index=False)
    
    print(f"  ✅ Removed {removed_count} PUT options, kept {filtered_count} CALL options")

def main():
    """Remove PUT options from AMZN data for 2022-2025."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    years = [2022, 2023, 2024, 2025]
    
    for year in years:
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                remove_put_options(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ PUT options removed from AMZN data for 2022-2025!")

if __name__ == "__main__":
    main()
