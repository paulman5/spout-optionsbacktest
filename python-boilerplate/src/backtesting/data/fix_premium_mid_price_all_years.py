#!/usr/bin/env python3
"""
Ensure premium = mid_price = (low_price + high_price) / 2 for all holiday files.
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
print("FIXING PREMIUM = MID_PRICE = (LOW_PRICE + HIGH_PRICE) / 2")
print("=" * 80)

# Find all holiday CSV files
csv_files = sorted(glob(str(holidays_dir / "*_options_pessimistic.csv")))
print(f"\n📁 Found {len(csv_files)} files to process\n")

for file_path in csv_files:
    year = Path(file_path).stem.split('_')[0]
    print(f"{'='*80}")
    print(f"PROCESSING {year}")
    print(f"{'='*80}")
    
    print(f"\n📖 Loading {file_path}...")
    df = pd.read_csv(file_path)
    print(f"   Loaded {len(df):,} rows")
    
    # Ensure high_price and low_price exist
    if 'high_price' not in df.columns or 'low_price' not in df.columns:
        print(f"   ❌ Missing high_price or low_price columns")
        continue
    
    # Calculate mid_price as (high_price + low_price) / 2
    print(f"\n🔧 Calculating mid_price = (high_price + low_price) / 2...")
    df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
    df['mid_price'] = df['mid_price'].round(2)
    
    # Set premium = mid_price
    print(f"🔧 Setting premium = mid_price...")
    df['premium'] = df['mid_price']
    
    # Recalculate premium_yield_pct using mid_price
    if 'underlying_spot' in df.columns:
        print(f"🔧 Recalculating premium_yield_pct using mid_price...")
        df['premium_yield_pct'] = (df['mid_price'] / df['underlying_spot'] * 100).round(2)
    
    # Verify
    premium_matches = (df['premium'].round(2) == df['mid_price'].round(2)).sum()
    print(f"\n   ✅ Verification: premium = mid_price for {premium_matches}/{len(df)} rows")
    
    if premium_matches < len(df):
        print(f"   ⚠️  Warning: {len(df) - premium_matches} rows don't match (should be 0)")
    
    # Show sample
    sample = df.iloc[0]
    print(f"\n   Sample row:")
    print(f"     high_price: {sample['high_price']:.2f}")
    print(f"     low_price: {sample['low_price']:.2f}")
    print(f"     mid_price: {sample['mid_price']:.2f}")
    print(f"     premium: {sample['premium']:.2f}")
    print(f"     premium_yield_pct: {sample['premium_yield_pct']:.2f}%")
    
    # Save
    print(f"\n💾 Saving to {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"   ✅ Saved!")
    
print(f"\n{'='*80}")
print("✅ ALL FILES COMPLETE!")
print(f"{'='*80}")

