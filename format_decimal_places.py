#!/usr/bin/env python3
"""
Script to format all numeric columns (except window_start) to 2 decimal places
in all *_options_pessimistic.csv files.
"""

import pandas as pd
import glob
import os
from pathlib import Path

def format_numeric_columns(df, exclude_columns=None):
    """
    Format all numeric columns to 2 decimal places, except those in exclude_columns.
    Preserves non-numeric columns as-is.
    """
    if exclude_columns is None:
        exclude_columns = []
    
    df_formatted = df.copy()
    
    for col in df.columns:
        if col in exclude_columns:
            continue
        
        # Check if column is numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            # Format to 2 decimal places
            df_formatted[col] = df[col].apply(lambda x: round(float(x), 2) if pd.notna(x) else x)
    
    return df_formatted

def process_csv_file(file_path):
    """
    Process a single CSV file to format numeric columns to 2 decimal places.
    """
    print(f"Processing: {file_path}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Get the original column order
        original_columns = df.columns.tolist()
        
        # Format numeric columns (excluding window_start and ticker)
        exclude_cols = ['window_start', 'ticker']
        df_formatted = format_numeric_columns(df, exclude_columns=exclude_cols)
        
        # Ensure column order is preserved
        df_formatted = df_formatted[original_columns]
        
        # Write back to the same file
        df_formatted.to_csv(file_path, index=False)
        print(f"  ✓ Formatted {len(df)} rows")
        
    except Exception as e:
        print(f"  ✗ Error processing {file_path}: {e}")

def main():
    """
    Main function to process all *_options_pessimistic.csv files.
    """
    # Get the base directory
    base_dir = Path(__file__).parent / "python-boilerplate" / "data"
    
    # Find all *_options_pessimistic.csv files
    pattern = str(base_dir / "**" / "*_options_pessimistic.csv")
    csv_files = glob.glob(pattern, recursive=True)
    
    print(f"Found {len(csv_files)} CSV files to process\n")
    
    # Process each file
    for csv_file in sorted(csv_files):
        process_csv_file(csv_file)
    
    print(f"\n✓ Completed processing {len(csv_files)} files")

if __name__ == "__main__":
    main()

