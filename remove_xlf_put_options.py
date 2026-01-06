#!/usr/bin/env python3
"""
Remove PUT options from XLF data for 2016 and 2017
"""

import pandas as pd
from pathlib import Path

def remove_put_options_from_file(file_path):
    """Remove PUT options from a single XLF file"""
    print(f"Processing {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0
    
    # Count original rows
    original_count = len(df)
    
    # Filter to keep only CALL options
    df_calls_only = df[df['option_type'] == 'C'].copy()
    
    # Count remaining rows
    remaining_count = len(df_calls_only)
    removed_count = original_count - remaining_count
    
    # Save the updated file
    df_calls_only.to_csv(file_path, index=False)
    
    print(f"   ✅ Removed {removed_count} PUT options, kept {remaining_count} CALL options")
    
    return remaining_count

def main():
    """Main function to remove PUT options from XLF 2016 and 2017"""
    print("🔄 Removing PUT options from XLF data for 2016 and 2017...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/XLF")
    years = [2016, 2017]
    frequencies = ['monthly', 'weekly']
    
    total_rows_processed = 0
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        for freq in frequencies:
            file_path = base_dir / freq / f"{year}_options_pessimistic.csv"
            
            if file_path.exists():
                rows_processed = remove_put_options_from_file(file_path)
                total_rows_processed += rows_processed
            else:
                print(f"   File not found: {file_path}")
    
    print(f"\n✅ PUT options removed from XLF data!")
    print(f"📊 Total CALL options remaining: {total_rows_processed}")

if __name__ == "__main__":
    main()
