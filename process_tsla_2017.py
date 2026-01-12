#!/usr/bin/env python3
"""
Process TSLA 2017 monthly file:
1. Remove all put options
2. Add fedfunds_rate
3. Calculate implied_volatility and probability_itm
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add the backtesting module to path
sys.path.append('python-boilerplate/src/backtesting')
from greeks2 import implied_volatility_call, probability_itm

base_path = Path('python-boilerplate/data')
file_path = base_path / 'TSLA' / 'monthly' / '2017_options_pessimistic.csv'

print(f"Processing {file_path}...")

# Read the file - check if it has headers
with open(file_path, 'r') as f:
    first_line = f.readline().strip()
    
# Check if first line looks like a header (has 'ticker' or 'date_only')
if 'ticker' in first_line.lower() or 'date_only' in first_line.lower():
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df):,} rows (with header)")
else:
    # No header - read with column names
    column_names = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM',
        'premium', 'premium_yield_pct', 'premium_low', 'premium_yield_pct_low',
        'high_price', 'low_price', 'transactions', 'window_start', 'days_to_expiry',
        'time_remaining_category', 'underlying_open', 'underlying_close', 'underlying_high',
        'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
        'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    df = pd.read_csv(file_path, header=None, names=column_names[:len(first_line.split(','))])
    print(f"Loaded {len(df):,} rows (no header, using column names)")

# Check current columns
print(f"\nColumns: {list(df.columns)}")

# Step 1: Remove all put options
print(f"\nStep 1: Removing put options...")
print(f"  Before: {len(df):,} rows")
if 'option_type' in df.columns:
    df = df[df['option_type'] != 'P'].copy()
    print(f"  After: {len(df):,} rows")
else:
    # Check if option type is in the ticker or another column
    if 'ticker' in df.columns:
        # Put options typically have 'P' in the ticker
        df = df[~df['ticker'].str.contains('P', na=False)].copy()
        print(f"  After (filtered by ticker): {len(df):,} rows")
    else:
        print("  ⚠️  Could not identify option_type column, skipping put removal")

# Step 2: Add fedfunds_rate
print(f"\nStep 2: Adding fedfunds_rate...")
if 'fedfunds_rate' not in df.columns:
    # Try to get from another TSLA file
    other_files = [
        base_path / 'TSLA' / 'holidays' / '2017_options_pessimistic.csv',
        base_path / 'TSLA' / 'monthly' / '2018_options_pessimistic.csv',
        base_path / 'TSLA' / 'monthly' / '2016_options_pessimistic.csv',
    ]
    
    fedfunds_map = None
    for other_file in other_files:
        if other_file.exists():
            try:
                other_df = pd.read_csv(other_file)
                if 'fedfunds_rate' in other_df.columns and 'date_only' in other_df.columns:
                    other_df['date_only'] = pd.to_datetime(other_df['date_only'])
                    fedfunds_map = other_df.groupby('date_only')['fedfunds_rate'].first().to_dict()
                    print(f"  ✅ Found fedfunds_rate in {other_file.name}")
                    break
            except Exception as e:
                continue
    
    if fedfunds_map:
        if 'date_only' in df.columns:
            df['date_only_dt'] = pd.to_datetime(df['date_only'])
            df['fedfunds_rate'] = df['date_only_dt'].map(fedfunds_map)
            # Forward fill and backfill
            df = df.sort_values('date_only_dt')
            df['fedfunds_rate'] = df['fedfunds_rate'].ffill().bfill()
            df = df.drop(columns=['date_only_dt'], errors='ignore')
            print(f"  ✅ Mapped fedfunds_rate from reference file")
        else:
            # Use average rate for 2017 (around 1.0-1.5%)
            df['fedfunds_rate'] = 0.0125  # 1.25% average for 2017
            print(f"  ⚠️  No date_only column, using default 1.25%")
    else:
        # Use average rate for 2017
        df['fedfunds_rate'] = 0.0125  # 1.25% average for 2017
        print(f"  ⚠️  No reference file found, using default 1.25%")
else:
    # Fill missing values
    if df['fedfunds_rate'].isna().any():
        df['fedfunds_rate'] = df['fedfunds_rate'].ffill().bfill()
        df['fedfunds_rate'] = df['fedfunds_rate'].fillna(0.0125)
        print(f"  ✅ Filled missing fedfunds_rate values")

# Step 3: Calculate implied_volatility and probability_itm
print(f"\nStep 3: Calculating implied_volatility and probability_itm...")

# Check required columns
required_cols = ['close_price', 'underlying_spot', 'strike', 'days_to_expiry', 'fedfunds_rate']
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    print(f"  ❌ Missing required columns: {missing_cols}")
    print(f"  Available columns: {list(df.columns)}")
    sys.exit(1)

# Calculate T (time to expiry in years) - but we'll delete it after calculations
df['T'] = df['days_to_expiry'] / 365.0

# Initialize columns if they don't exist
if 'implied_volatility' not in df.columns:
    df['implied_volatility'] = np.nan
if 'probability_itm' not in df.columns:
    df['probability_itm'] = np.nan

print(f"  Calculating for {len(df):,} rows...")

def calc_iv_prob(row):
    try:
        C = row['close_price']
        S = row['underlying_spot']
        K = row['strike']
        T = row['T']
        r = row['fedfunds_rate']
        
        # Skip if invalid inputs
        if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or pd.isna(r):
            return pd.Series([np.nan, np.nan])
        
        if C <= 0 or S <= 0 or K <= 0 or T <= 0:
            return pd.Series([np.nan, np.nan])
        
        # Check if C > S (invalid for call options)
        if C > S:
            return pd.Series([np.nan, np.nan])
        
        # Calculate IV
        iv = implied_volatility_call(C, S, K, T, r)
        
        # Calculate probability ITM
        if pd.isna(iv) or iv <= 0:
            prob = np.nan
        else:
            prob = probability_itm(S, K, T, r, iv)
        
        # Round to 2 decimal places
        iv_rounded = round(iv, 2) if not pd.isna(iv) else np.nan
        prob_rounded = round(prob, 2) if not pd.isna(prob) else np.nan
        
        return pd.Series([iv_rounded, prob_rounded])
    except Exception as e:
        return pd.Series([np.nan, np.nan])

# Apply calculation
print("  Processing rows...")
results = df.apply(calc_iv_prob, axis=1)
df['implied_volatility'] = results[0]
df['probability_itm'] = results[1]

# Statistics
iv_valid = df['implied_volatility'].notna().sum()
prob_valid = df['probability_itm'].notna().sum()
print(f"\n  ✅ Implied Volatility: {iv_valid:,}/{len(df):,} ({iv_valid/len(df)*100:.1f}%) valid")
print(f"  ✅ Probability ITM: {prob_valid:,}/{len(df):,} ({prob_valid/len(df)*100:.1f}%) valid")

# Remove the T column (it was only needed for calculations)
if 'T' in df.columns:
    df = df.drop(columns=['T'])
    print(f"\n  ✅ Removed temporary 'T' column")

# Save the file
print(f"\nSaving to {file_path}...")
df.to_csv(file_path, index=False)
print(f"✅ Done! Saved {len(df):,} rows")

