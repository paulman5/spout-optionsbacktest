#!/usr/bin/env python3
"""
Recalculate implied volatility and probability ITM for AAPL 2024 holidays file
using greeks2.py functions.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

file_path = Path("python-boilerplate/data/AAPL/holidays/2024_options_pessimistic.csv")

print("=" * 80)
print("RECALCULATING IV AND PROBABILITY ITM FOR AAPL 2024 HOLIDAYS")
print("=" * 80)
print(f"File: {file_path}")
print()

# Load the file
print(f"📖 Loading {file_path}...")
df = pd.read_csv(file_path)
print(f"   Loaded {len(df):,} rows")

# Check current state
iv_valid_before = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
prob_valid_before = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
print(f"   Before: IV valid: {iv_valid_before:,}/{len(df):,} ({iv_valid_before/len(df)*100:.1f}%)")
print(f"   Before: Prob ITM valid: {prob_valid_before:,}/{len(df):,} ({prob_valid_before/len(df)*100:.1f}%)")

# Ensure mid_price exists
if 'mid_price' not in df.columns:
    if 'high_price' in df.columns and 'low_price' in df.columns:
        df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
        print(f"   Created mid_price from high_price and low_price")
    else:
        df['mid_price'] = df['close_price']
        print(f"   Created mid_price from close_price")

# Check required columns
required_cols = ['mid_price', 'underlying_spot', 'strike', 'days_to_expiry']
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    print(f"   ❌ Missing required columns: {missing_cols}")
    sys.exit(1)

# Add fedfunds_rate if missing
if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
    df['fedfunds_rate'] = 0.02
    print(f"   Set fedfunds_rate to 0.02 (default)")

# Calculate T (time to expiry in years)
df['T'] = df['days_to_expiry'] / 365.0

print(f"\n📊 Calculating IV and probability ITM using greeks2.py...")
print(f"   Using mid_price when available, otherwise close_price")

def calc_iv_prob(row):
    try:
        # Use mid_price if available and valid (> 0), otherwise try close_price
        C = None
        if pd.notna(row.get('mid_price')) and row['mid_price'] > 0:
            C = row['mid_price']
        elif pd.notna(row.get('close_price')) and row['close_price'] > 0:
            C = row['close_price']
        else:
            return np.nan, np.nan
        
        S = row['underlying_spot']
        K = row['strike']
        T = row['T']
        r = row['fedfunds_rate'] if pd.notna(row['fedfunds_rate']) else 0.02
        
        # Validate inputs
        if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or C <= 0 or S <= 0 or K <= 0 or T <= 0:
            return np.nan, np.nan
        
        # For very small prices, use a minimum threshold
        if C < 0.01:
            # For prices less than $0.01, it's likely noise or zero-bid options
            # Only calculate if we have a reasonable price
            return np.nan, np.nan
        
        # Calculate implied volatility
        iv = implied_volatility_call(C, S, K, T, r)
        
        # If IV calculation failed, return NaN
        if pd.isna(iv) or iv <= 0:
            return np.nan, np.nan
        
        # Calculate probability ITM using the calculated IV
        prob = probability_itm(S, K, T, r, iv)
        
        return iv, prob
    except Exception as e:
        return np.nan, np.nan

# Apply calculation to all rows
print(f"   Processing {len(df):,} rows...")
iv_prob = df.apply(calc_iv_prob, axis=1)
df['implied_volatility'] = [x[0] for x in iv_prob]
df['probability_itm'] = [x[1] for x in iv_prob]

# Drop temporary T column
df = df.drop(columns=['T'], errors='ignore')

# Round to 4 decimal places
df['implied_volatility'] = df['implied_volatility'].round(4)
df['probability_itm'] = df['probability_itm'].round(4)

# Check results
iv_valid_after = df['implied_volatility'].notna().sum()
prob_valid_after = df['probability_itm'].notna().sum()
iv_nulls = df['implied_volatility'].isna().sum()
prob_nulls = df['probability_itm'].isna().sum()

print(f"\n📈 Results:")
print(f"   ✅ IV valid: {iv_valid_after:,}/{len(df):,} ({iv_valid_after/len(df)*100:.1f}%)")
print(f"   ✅ Prob ITM valid: {prob_valid_after:,}/{len(df):,} ({prob_valid_after/len(df)*100:.1f}%)")
print(f"   ❌ IV nulls: {iv_nulls:,} ({iv_nulls/len(df)*100:.1f}%)")
print(f"   ❌ Prob ITM nulls: {prob_nulls:,} ({prob_nulls/len(df)*100:.1f}%)")

if iv_valid_after > 0:
    print(f"\n   IV statistics:")
    print(f"      Min: {df['implied_volatility'].min():.4f}")
    print(f"      Max: {df['implied_volatility'].max():.4f}")
    print(f"      Mean: {df['implied_volatility'].mean():.4f}")
    print(f"      Median: {df['implied_volatility'].median():.4f}")

if prob_valid_after > 0:
    print(f"\n   Probability ITM statistics:")
    print(f"      Min: {df['probability_itm'].min():.4f}")
    print(f"      Max: {df['probability_itm'].max():.4f}")
    print(f"      Mean: {df['probability_itm'].mean():.4f}")
    print(f"      Median: {df['probability_itm'].median():.4f}")

# Show improvement
if iv_valid_before > 0:
    improvement = iv_valid_after - iv_valid_before
    print(f"\n   Improvement: +{improvement:,} IV values ({improvement/len(df)*100:.1f}%)")

# Save the file
print(f"\n💾 Saving updated file...")
df.to_csv(file_path, index=False)
print(f"   ✅ Saved {file_path}")

print(f"\n{'='*80}")
print("✅ COMPLETE!")
print(f"{'='*80}")

