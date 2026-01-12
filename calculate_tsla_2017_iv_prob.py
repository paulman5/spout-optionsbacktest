#!/usr/bin/env python3
"""
Calculate IV and probability ITM for TSLA monthly 2017 file using greeks2.py
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

file_path = Path("python-boilerplate/data/TSLA/monthly/2017_options_pessimistic.csv")

print("=" * 100)
print("CALCULATING IV AND PROBABILITY ITM FOR TSLA 2017 MONTHLY")
print("=" * 100)
print(f"File: {file_path}")
print()

# Load the file
df = pd.read_csv(file_path)
print(f"Total rows: {len(df):,}")
print(f"Call options: {(df['option_type'] == 'C').sum():,}")
print(f"Put options: {(df['option_type'] == 'P').sum():,}")
print()

# Count missing values
iv_missing_before = df['implied_volatility'].isna().sum()
prob_missing_before = df['probability_itm'].isna().sum()
print(f"IV missing before: {iv_missing_before:,}")
print(f"Probability ITM missing before: {prob_missing_before:,}")
print()

def calc_iv_prob(row):
    """Calculate IV and probability ITM for a single row."""
    try:
        # Only process call options
        if row['option_type'] != 'C':
            return row.get('implied_volatility', np.nan), row.get('probability_itm', np.nan)
        
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
        T = row['days_to_expiry'] / 365.0
        r = row['fedfunds_rate'] if pd.notna(row.get('fedfunds_rate')) else 0.02
        
        # Validate inputs
        if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or C <= 0 or S <= 0 or K <= 0 or T <= 0:
            return np.nan, np.nan
        
        # For very small prices, use a minimum threshold
        if C < 0.01:
            return np.nan, np.nan
        
        # Check for impossible scenarios: call price cannot exceed spot price (for standard calls)
        # This would indicate data quality issues
        if C > S * 1.1:  # Allow 10% tolerance for rounding/calculation errors
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

# Calculate IV and probability ITM for all rows
print("Calculating IV and probability ITM...")
print("This may take a few minutes...")
print()

# Process in batches for progress tracking
batch_size = 100
total_rows = len(df)
calculated_iv = 0
calculated_prob = 0

for i in range(0, total_rows, batch_size):
    batch = df.iloc[i:i+batch_size]
    
    for idx, row in batch.iterrows():
        # Only calculate if missing
        if pd.isna(row.get('implied_volatility')) or pd.isna(row.get('probability_itm')):
            iv, prob = calc_iv_prob(row)
            
            if pd.notna(iv):
                df.at[idx, 'implied_volatility'] = iv
                calculated_iv += 1
            
            if pd.notna(prob):
                df.at[idx, 'probability_itm'] = prob
                calculated_prob += 1
    
    if (i // batch_size) % 10 == 0:
        print(f"  Processed {min(i+batch_size, total_rows):,} / {total_rows:,} rows...")

print()
print("=" * 100)
print("RESULTS")
print("=" * 100)

# Count after calculation
iv_missing_after = df['implied_volatility'].isna().sum()
prob_missing_after = df['probability_itm'].isna().sum()
iv_valid = df['implied_volatility'].notna().sum()
prob_valid = df['probability_itm'].notna().sum()

print(f"IV calculated: {calculated_iv:,}")
print(f"Probability ITM calculated: {calculated_prob:,}")
print()
print(f"IV missing after: {iv_missing_after:,}")
print(f"IV valid: {iv_valid:,} ({iv_valid/len(df)*100:.1f}%)")
print()
print(f"Probability ITM missing after: {prob_missing_after:,}")
print(f"Probability ITM valid: {prob_valid:,} ({prob_valid/len(df)*100:.1f}%)")
print()

# Save the file
print("Saving file...")
df.to_csv(file_path, index=False)
print(f"✅ Saved: {file_path}")
print()

# Show some statistics
if iv_valid > 0:
    print("IV Statistics:")
    print(f"  Mean: {df['implied_volatility'].mean():.4f} ({df['implied_volatility'].mean()*100:.2f}%)")
    print(f"  Median: {df['implied_volatility'].median():.4f} ({df['implied_volatility'].median()*100:.2f}%)")
    print(f"  Min: {df['implied_volatility'].min():.4f} ({df['implied_volatility'].min()*100:.2f}%)")
    print(f"  Max: {df['implied_volatility'].max():.4f} ({df['implied_volatility'].max()*100:.2f}%)")
    print()

if prob_valid > 0:
    print("Probability ITM Statistics:")
    print(f"  Mean: {df['probability_itm'].mean():.4f} ({df['probability_itm'].mean()*100:.2f}%)")
    print(f"  Median: {df['probability_itm'].median():.4f} ({df['probability_itm'].median()*100:.2f}%)")
    print(f"  Min: {df['probability_itm'].min():.4f} ({df['probability_itm'].min()*100:.2f}%)")
    print(f"  Max: {df['probability_itm'].max():.4f} ({df['probability_itm'].max()*100:.2f}%)")
    print()

print("=" * 100)
