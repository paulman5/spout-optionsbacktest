#!/usr/bin/env python3
"""
Recalculate premium and premium_yield_pct using mid_price instead of close_price
for all holiday files.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from glob import glob

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Get base path
base_path = Path(__file__).parent.parent.parent.parent
TICKER = 'AAPL'
holidays_dir = base_path / "data" / TICKER / "holidays"

print("=" * 80)
print("RECALCULATING PREMIUM USING MID_PRICE")
print("=" * 80)

# Find all holiday CSV files
csv_files = sorted(glob(str(holidays_dir / "*_options_pessimistic.csv")))
print(f"\n📁 Found {len(csv_files)} files to process")

for file_path in csv_files:
    year = Path(file_path).stem.split('_')[0]
    print(f"\n{'='*80}")
    print(f"PROCESSING {year}")
    print(f"{'='*80}")
    
    print(f"\n📖 Loading {file_path}...")
    df = pd.read_csv(file_path)
    print(f"   Loaded {len(df):,} rows")
    
    # Check if mid_price exists, if not calculate it
    if 'mid_price' not in df.columns:
        if 'high_price' in df.columns and 'low_price' in df.columns:
            print(f"   Calculating mid_price from high_price and low_price...")
            df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
        else:
            print(f"   ⚠️  Cannot calculate mid_price (missing high_price or low_price)")
            print(f"   Using close_price as fallback")
            df['mid_price'] = df['close_price']
    else:
        print(f"   ✅ mid_price column already exists")
    
    # Recalculate premium using mid_price
    print(f"\n🔧 Recalculating premium and premium_yield_pct using mid_price...")
    df['premium'] = df['mid_price']
    df['premium_yield_pct'] = (df['mid_price'] / df['underlying_spot'] * 100).round(2)
    
    # Round mid_price to 2 decimals
    df['mid_price'] = df['mid_price'].round(2)
    
    # Show sample comparison
    sample = df.iloc[0]
    print(f"\n   Sample row:")
    print(f"   close_price: {sample['close_price']:.2f}")
    print(f"   mid_price: {sample['mid_price']:.2f}")
    print(f"   premium (old): {sample.get('premium', 'N/A')}")
    print(f"   premium (new): {df.iloc[0]['premium']:.2f}")
    print(f"   premium_yield_pct (new): {df.iloc[0]['premium_yield_pct']:.2f}%")
    
    # Save
    print(f"\n💾 Saving to {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"   ✅ Saved!")
    
print(f"\n{'='*80}")
print("✅ ALL FILES COMPLETE!")
print(f"{'='*80}")


