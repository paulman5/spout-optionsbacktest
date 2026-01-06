#!/usr/bin/env python3
"""
Fix decimal precision in AMZN data:
- OTM percentage: 2 decimals
- Premium yield percentages: 4 decimals  
- All other numeric columns: 2 decimals
"""

import pandas as pd
import os
import numpy as np

def fix_decimal_precision(file_path):
    """Fix decimal precision in a CSV file."""
    print(f"Processing {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    if df.empty:
        print(f"  File is empty, skipping...")
        return
    
    # Fix OTM percentage - 2 decimals
    if 'otm_pct' in df.columns:
        df['otm_pct'] = df['otm_pct'].round(2)
    
    # Fix premium yield percentages - 4 decimals
    if 'premium_yield_pct' in df.columns:
        df['premium_yield_pct'] = df['premium_yield_pct'].round(4)
    if 'premium_yield_pct_low' in df.columns:
        df['premium_yield_pct_low'] = df['premium_yield_pct_low'].round(4)
    
    # Fix all other numeric columns to 2 decimals (except window_start)
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    columns_to_round = [col for col in numeric_columns if col not in ['otm_pct', 'premium_yield_pct', 'premium_yield_pct_low', 'window_start']]
    
    for col in columns_to_round:
        df[col] = df[col].round(2)
    
    # Sort by date and strike
    df = df.sort_values(['date_only', 'strike'])
    
    # Save the fixed data
    df.to_csv(file_path, index=False)
    print(f"  ✅ Fixed decimal precision for {len(df)} rows")

def main():
    """Fix decimal precision for AMZN data 2016-2025."""
    amzn_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN"
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Process monthly and weekly files
        files_to_process = [
            os.path.join(amzn_dir, "monthly", f"{year}_options_pessimistic.csv"),
            os.path.join(amzn_dir, "weekly", f"{year}_options_pessimistic.csv")
        ]
        
        for file_path in files_to_process:
            if os.path.exists(file_path):
                fix_decimal_precision(file_path)
            else:
                print(f"⚠️ File not found: {file_path}")
    
    print("\n✅ Decimal precision fixed for AMZN data 2016-2025!")

if __name__ == "__main__":
    main()
