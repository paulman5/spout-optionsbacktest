#!/usr/bin/env python3
"""
Recalculate IV and probability ITM for TSLA 2017 monthly file using fedfunds_rate.
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
print("RECALCULATING IV AND PROBABILITY ITM FOR TSLA 2017 MONTHLY")
print("Using fedfunds_rate for all calculations")
print("=" * 100)
print(f"File: {file_path}")
print()

# Load the file
df = pd.read_csv(file_path)
print(f"Total rows: {len(df):,}")
print(f"Call options: {(df['option_type'] == 'C').sum():,}")
print(f"Put options: {(df['option_type'] == 'P').sum():,}")
print()

# Check fedfunds_rate
fedfunds_missing = df['fedfunds_rate'].isna().sum()
fedfunds_unique = df['fedfunds_rate'].unique()
print(f"fedfunds_rate missing: {fedfunds_missing}")
print(f"fedfunds_rate range: {fedfunds_unique.min():.4f} to {fedfunds_unique.max():.4f}")
print(f"fedfunds_rate unique values: {len(fedfunds_unique)}")
print()

# Count missing values before
iv_missing_before = df['implied_volatility'].isna().sum()
prob_missing_before = df['probability_itm'].isna().sum()
iv_valid_before = df['implied_volatility'].notna().sum()
prob_valid_before = df['probability_itm'].notna().sum()

print(f"Before recalculation:")
print(f"  IV missing: {iv_missing_before:,} ({iv_missing_before/len(df)*100:.1f}%)")
print(f"  IV valid: {iv_valid_before:,} ({iv_valid_before/len(df)*100:.1f}%)")
print(f"  Probability ITM missing: {prob_missing_before:,} ({prob_missing_before/len(df)*100:.1f}%)")
print(f"  Probability ITM valid: {prob_valid_before:,} ({prob_valid_before/len(df)*100:.1f}%)")
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
        
        # Use fedfunds_rate, with fallback to 0.02 if missing
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

# Recalculate IV and probability ITM for all rows
print("Recalculating IV and probability ITM for all rows...")
print("This may take a few minutes...")
print()

# Process in batches for progress tracking
batch_size = 100
total_rows = len(df)
calculated_iv = 0
calculated_prob = 0
recalculated_iv = 0
recalculated_prob = 0

for i in range(0, total_rows, batch_size):
    batch = df.iloc[i:i+batch_size]
    
    for idx, row in batch.iterrows():
        iv, prob = calc_iv_prob(row)
        
        # Update if we got valid values
        if pd.notna(iv):
            if pd.isna(df.at[idx, 'implied_volatility']):
                calculated_iv += 1
            else:
                recalculated_iv += 1
            df.at[idx, 'implied_volatility'] = iv
        
        if pd.notna(prob):
            if pd.isna(df.at[idx, 'probability_itm']):
                calculated_prob += 1
            else:
                recalculated_prob += 1
            df.at[idx, 'probability_itm'] = prob
    
    if (i // batch_size) % 10 == 0:
        print(f"  Processed {min(i+batch_size, total_rows):,} / {total_rows:,} rows...")

print()
print("=" * 100)
print("RESULTS")
print("=" * 100)

# Count after calculation
iv_missing_after = df['implied_volatility'].isna().sum()
prob_missing_after = df['probability_itm'].isna().sum()
iv_valid_after = df['implied_volatility'].notna().sum()
prob_valid_after = df['probability_itm'].notna().sum()

print(f"IV newly calculated: {calculated_iv:,}")
print(f"IV recalculated: {recalculated_iv:,}")
print(f"Probability ITM newly calculated: {calculated_prob:,}")
print(f"Probability ITM recalculated: {recalculated_prob:,}")
print()
print(f"After recalculation:")
print(f"  IV missing: {iv_missing_after:,} ({iv_missing_after/len(df)*100:.1f}%)")
print(f"  IV valid: {iv_valid_after:,} ({iv_valid_after/len(df)*100:.1f}%)")
print(f"  Probability ITM missing: {prob_missing_after:,} ({prob_missing_after/len(df)*100:.1f}%)")
print(f"  Probability ITM valid: {prob_valid_after:,} ({prob_valid_after/len(df)*100:.1f}%)")
print()

# Save the file
print("Saving file...")
df.to_csv(file_path, index=False)
print(f"✅ Saved: {file_path}")
print()

# Show some statistics
if iv_valid_after > 0:
    print("IV Statistics:")
    print(f"  Mean: {df['implied_volatility'].mean():.4f} ({df['implied_volatility'].mean()*100:.2f}%)")
    print(f"  Median: {df['implied_volatility'].median():.4f} ({df['implied_volatility'].median()*100:.2f}%)")
    print(f"  Min: {df['implied_volatility'].min():.4f} ({df['implied_volatility'].min()*100:.2f}%)")
    print(f"  Max: {df['implied_volatility'].max():.4f} ({df['implied_volatility'].max()*100:.2f}%)")
    print()

if prob_valid_after > 0:
    print("Probability ITM Statistics:")
    print(f"  Mean: {df['probability_itm'].mean():.4f} ({df['probability_itm'].mean()*100:.2f}%)")
    print(f"  Median: {df['probability_itm'].median():.4f} ({df['probability_itm'].median()*100:.2f}%)")
    print(f"  Min: {df['probability_itm'].min():.4f} ({df['probability_itm'].min()*100:.2f}%)")
    print(f"  Max: {df['probability_itm'].max():.4f} ({df['probability_itm'].max()*100:.2f}%)")
    print()

# Show why some rows couldn't be calculated
if iv_missing_after > 0:
    missing_rows = df[df['implied_volatility'].isna() & (df['option_type'] == 'C')]
    if len(missing_rows) > 0:
        print("Analysis of rows with missing IV:")
        print(f"  Total call rows missing IV: {len(missing_rows):,}")
        
        # Check reasons
        c_gt_s = ((missing_rows['close_price'] > missing_rows['underlying_spot'] * 1.1) | 
                  (missing_rows['mid_price'] > missing_rows['underlying_spot'] * 1.1)).sum()
        missing_data = (missing_rows['underlying_spot'].isna() | 
                       missing_rows['strike'].isna() | 
                       missing_rows['days_to_expiry'].isna()).sum()
        low_price = ((missing_rows['close_price'] < 0.01) | 
                     (missing_rows['mid_price'] < 0.01)).sum()
        
        print(f"  Call price > Spot price (invalid data): {c_gt_s:,}")
        print(f"  Missing required data: {missing_data:,}")
        print(f"  Price too low (< $0.01): {low_price:,}")
        print()

print("=" * 100)

