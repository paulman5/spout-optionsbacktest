"""
Sort CSV files by date_only column to ensure chronological order.
This script sorts all options CSV files in the TSLA data directories.
"""

import pandas as pd
import sys
from pathlib import Path
import glob


def sort_csv_by_date(file_path: str):
    """
    Sort a CSV file by date_only column in chronological order.
    
    Args:
        file_path: Path to the CSV file to sort
    """
    print(f"📅 Sorting {file_path}...")
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    # Check if date_only column exists
    if 'date_only' not in df.columns:
        print(f"   ⚠️  No 'date_only' column found, skipping")
        return
    
    # Convert date_only to datetime for proper sorting
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Sort by date_only first, then by strike (low to high) within each date
    df_sorted = df.sort_values(['date_only', 'strike']).reset_index(drop=True)
    
    # Convert back to date string format
    df_sorted['date_only'] = df_sorted['date_only'].dt.strftime('%Y-%m-%d')
    
    # Save the sorted file
    df_sorted.to_csv(file_path, index=False)
    
    print(f"   ✅ Sorted {len(df_sorted):,} rows from {df_sorted['date_only'].min()} to {df_sorted['date_only'].max()}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Sort CSV files by date_only column")
    parser.add_argument("--file", help="Path to a specific CSV file to sort")
    parser.add_argument("--directory", help="Path to directory containing CSV files to sort")
    parser.add_argument("--all-tsla", action="store_true", help="Sort all TSLA CSV files in data directories")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("SORTING CSV FILES BY DATE")
    print("=" * 80)
    
    if args.file:
        # Sort a specific file
        sort_csv_by_date(args.file)
    elif args.directory:
        # Sort all CSV files in directory
        csv_files = glob.glob(str(Path(args.directory) / "*.csv"))
        for csv_file in sorted(csv_files):
            sort_csv_by_date(csv_file)
    elif args.all_tsla:
        # Sort all TSLA CSV files
        base_path = Path("data/TSLA")
        csv_files = []
        
        # Find all CSV files in weekly and monthly directories
        for pattern in ["weekly/*.csv", "monthly/*.csv"]:
            csv_files.extend(glob.glob(str(base_path / pattern)))
        
        print(f"Found {len(csv_files)} CSV files to sort\n")
        
        for csv_file in sorted(csv_files):
            sort_csv_by_date(csv_file)
        
        print(f"\n✅ Sorted {len(csv_files)} files")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

