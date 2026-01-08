#!/usr/bin/env python3
"""
Recalculate ITM (In-The-Money) status based on underlying price at expiration.
For calls: ITM = YES if underlying_spot_at_expiry >= strike
For puts: ITM = YES if underlying_spot_at_expiry <= strike
"""

import pandas as pd
import glob
from pathlib import Path

def recalculate_itm(df):
    """
    Recalculate ITM based on underlying_spot_at_expiry vs strike.
    
    For calls (C): ITM = YES if underlying_spot_at_expiry >= strike
    For puts (P): ITM = YES if underlying_spot_at_expiry <= strike
    """
    df = df.copy()
    
    # Check if we have the required columns
    if 'underlying_spot_at_expiry' not in df.columns:
        print("  ⚠️  Warning: 'underlying_spot_at_expiry' column not found")
        print("     Falling back to 'underlying_close_at_expiry'")
        if 'underlying_close_at_expiry' not in df.columns:
            print("  ❌ Error: No expiration price column found")
            return df
        df['underlying_spot_at_expiry'] = df['underlying_close_at_expiry']
    
    # Handle missing expiration prices
    missing_expiry = df['underlying_spot_at_expiry'].isna().sum()
    if missing_expiry > 0:
        print(f"  ⚠️  Warning: {missing_expiry} rows missing expiration prices")
    
    # Recalculate ITM based on expiration
    def calculate_itm(row):
        if pd.isna(row['underlying_spot_at_expiry']) or pd.isna(row['strike']):
            return row.get('ITM', 'NO')  # Keep original if data missing
        
        spot_at_expiry = float(row['underlying_spot_at_expiry'])
        strike = float(row['strike'])
        option_type = str(row['option_type']).upper()
        
        if option_type == 'C':
            # Call: ITM if spot >= strike
            return 'YES' if spot_at_expiry >= strike else 'NO'
        elif option_type == 'P':
            # Put: ITM if spot <= strike
            return 'YES' if spot_at_expiry <= strike else 'NO'
        else:
            return row.get('ITM', 'NO')  # Keep original if unknown type
    
    df['ITM'] = df.apply(calculate_itm, axis=1)
    
    return df

def process_csv_file(file_path):
    """Process a single CSV file to recalculate ITM."""
    print(f"Processing: {file_path}")
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Count original ITM values
        original_itm_yes = (df['ITM'] == 'YES').sum()
        original_itm_no = (df['ITM'] == 'NO').sum()
        
        # Recalculate ITM
        df = recalculate_itm(df)
        
        # Count new ITM values
        new_itm_yes = (df['ITM'] == 'YES').sum()
        new_itm_no = (df['ITM'] == 'NO').sum()
        
        # Show changes
        changed = (df['ITM'] != pd.read_csv(file_path)['ITM']).sum()
        print(f"  ✓ Processed {len(df)} rows")
        print(f"    ITM changes: {changed} rows updated")
        print(f"    ITM YES: {original_itm_yes} → {new_itm_yes}")
        print(f"    ITM NO: {original_itm_no} → {new_itm_no}")
        
        # Write back to the same file
        df.to_csv(file_path, index=False)
        
    except Exception as e:
        print(f"  ✗ Error processing {file_path}: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to process all *_options_pessimistic.csv files."""
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

