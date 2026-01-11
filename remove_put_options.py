#!/usr/bin/env python3
"""
Remove all put options (option_type == 'P') from all ticker files.
Keep only call options (option_type == 'C').
"""

import pandas as pd
from pathlib import Path
import glob

def remove_puts_from_file(csv_file: Path):
    """Remove put options from a single CSV file."""
    try:
        df = pd.read_csv(csv_file)
        
        if len(df) <= 1:  # Only header or empty
            return False, 0, 0, 0, 0
        
        # Check if option_type column exists
        if 'option_type' not in df.columns:
            return False, 0, 0, 0, 0
        
        # Count puts before removal
        puts_count = (df['option_type'] == 'P').sum()
        calls_count = (df['option_type'] == 'C').sum()
        total_before = len(df)
        
        # Filter to keep only calls
        df = df[df['option_type'] == 'C'].copy()
        
        total_after = len(df)
        
        # Save the file
        df.to_csv(csv_file, index=False)
        
        return True, total_before, total_after, puts_count, calls_count
        
    except Exception as e:
        print(f"   ❌ Error processing {csv_file.name}: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, 0, 0, 0

def main():
    """Main function to process all tickers."""
    data_dir = Path('python-boilerplate/data')
    
    # Find all CSV files in holidays, weekly, and monthly directories
    pattern = str(data_dir / '**' / '*.csv')
    csv_files = glob.glob(pattern, recursive=True)
    
    # Filter to only options files (exclude HistoricalData files)
    options_files = [f for f in csv_files if 'options_pessimistic' in f and 'HistoricalData' not in f]
    
    print("=" * 80)
    print("REMOVING PUT OPTIONS FROM ALL TICKERS")
    print("=" * 80)
    print(f"Found {len(options_files)} options files")
    print()
    
    total_files = 0
    total_rows_before = 0
    total_rows_after = 0
    total_puts_removed = 0
    total_calls_kept = 0
    failed_files = 0
    
    for csv_file in sorted(options_files):
        csv_path = Path(csv_file)
        ticker = csv_path.parent.parent.name
        folder = csv_path.parent.name
        filename = csv_path.name
        
        success, before, after, puts, calls = remove_puts_from_file(csv_path)
        
        if success:
            total_files += 1
            total_rows_before += before
            total_rows_after += after
            total_puts_removed += puts
            total_calls_kept += calls
            
            if puts > 0:  # Only print if puts were removed
                print(f"✓ {ticker}/{folder}/{filename}")
                print(f"  Removed {puts} puts, kept {calls} calls ({before} → {after} rows)")
            
            if total_files % 100 == 0:  # Print progress every 100 files
                print(f"   Progress: {total_files} files processed...")
        else:
            failed_files += 1
    
    print()
    print("=" * 80)
    print("✅ COMPLETED")
    print("=" * 80)
    print(f"Total files processed: {total_files}")
    print(f"Total rows before: {total_rows_before:,}")
    print(f"Total rows after: {total_rows_after:,}")
    print(f"Total puts removed: {total_puts_removed:,}")
    print(f"Total calls kept: {total_calls_kept:,}")
    print(f"Failed files: {failed_files}")
    print("=" * 80)

if __name__ == "__main__":
    main()

