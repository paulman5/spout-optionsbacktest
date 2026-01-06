#!/usr/bin/env python3
"""
Apply TSLA stock split adjustments to strike prices across all CSV files.
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime

def get_split_multiplier(date_str):
    """
    Get the split multiplier based on the date.
    Returns the divisor to apply to the strike price.
    """
    try:
        # Parse date from CSV (assuming YYYY-MM-DD format)
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # TSLA split history:
        # - August 31, 2020: 5-for-1 split
        # - August 25, 2022: 3-for-1 split
        
        if date_obj < datetime(2020, 8, 31):
            # Before 2020 split: apply both splits (5 * 3 = 15)
            return 15.0
        elif date_obj < datetime(2022, 8, 25):
            # Between 2020 and 2022 splits: apply only the 2022 split (3)
            return 3.0
        else:
            # After 2022 split: no adjustment needed
            return 1.0
            
    except (ValueError, TypeError):
        # If date parsing fails, assume no adjustment
        return 1.0

def adjust_strikes_in_csv(csv_path):
    """Adjust strikes in a single CSV file based on split dates."""
    print(f"🔄 Processing {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        
        if 'ticker' not in df.columns or 'strike' not in df.columns or 'date_only' not in df.columns:
            print(f"   ⚠️  Skipping - missing required columns")
            return False
        
        # Calculate split adjustments
        df['split_multiplier'] = df['date_only'].apply(get_split_multiplier)
        
        # Apply split adjustments to strikes and round to 2 decimal places
        df['original_strike'] = df['strike']
        df['adjusted_strike'] = (df['strike'] / df['split_multiplier']).round(2)
        
        # Check if there are any adjustments needed
        needs_adjustment = df['split_multiplier'] > 1.0
        adj_count = needs_adjustment.sum()
        
        if adj_count > 0:
            print(f"   📊 Found {adj_count} rows needing split adjustment")
            
            # Show some examples
            adj_examples = df[needs_adjustment].head(3)
            print("   📝 Example adjustments:")
            for _, row in adj_examples.iterrows():
                print(f"      {row['date_only']}: {row['original_strike']} ÷ {row['split_multiplier']} = {row['adjusted_strike']}")
            
            # Update strikes with adjusted values
            df['strike'] = df['adjusted_strike']
            
            # Clean up temporary columns
            df.drop(['split_multiplier', 'original_strike', 'adjusted_strike'], axis=1, inplace=True)
            
            # Save back
            df.to_csv(csv_path, index=False)
            print(f"   ✅ Applied split adjustments and saved {csv_path}")
            return True
        else:
            print(f"   ✅ No split adjustments needed")
            df.drop(['split_multiplier', 'original_strike', 'adjusted_strike'], axis=1, inplace=True)
            return False
            
    except Exception as e:
        print(f"   ❌ Error processing {csv_path}: {e}")
        return False

def find_all_csv_files(base_path):
    """Find all CSV files that contain options data."""
    csv_files = []
    
    # Look for CSV files with options data patterns
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith('.csv') and (
                'options' in file.lower() or 
                'pessimistic' in file.lower() or
                'day_aggs' in file.lower()
            ):
                csv_path = Path(root) / file
                csv_files.append(csv_path)
    
    return csv_files

def main():
    print("🔄 Applying TSLA stock split adjustments to all CSV files...")
    print("=" * 70)
    print("📅 Split Adjustment Rules:")
    print("   • Before 2020-08-31: Divide by 15 (5:1 × 3:1 splits)")
    print("   • 2020-08-31 to 2022-08-25: Divide by 3 (3:1 split only)")
    print("   • After 2022-08-25: No adjustment (1:1)")
    print("=" * 70)
    
    # Find all CSV files
    base_path = Path(".")
    csv_files = find_all_csv_files(base_path)
    
    print(f"📁 Found {len(csv_files)} CSV files to check")
    
    total_adjusted = 0
    for csv_file in sorted(csv_files):
        if adjust_strikes_in_csv(csv_file):
            total_adjusted += 1
    
    print("=" * 70)
    print(f"✅ Completed! Applied split adjustments to {total_adjusted} files")

if __name__ == "__main__":
    main()
