#!/usr/bin/env python3
"""
Recalculate IV and probability ITM for all PLTR monthly options files using greeks2.py
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

base_dir = Path("python-boilerplate/data")
ticker = "PLTR"
monthly_dir = base_dir / ticker / 'monthly'

print("=" * 100)
print("RECALCULATING IV AND PROBABILITY ITM FOR PLTR MONTHLY OPTIONS")
print("=" * 100)
print()

if not monthly_dir.exists():
    print(f"❌ Monthly directory not found: {monthly_dir}")
    exit(1)

def calc_iv_prob(row):
    """Calculate IV and probability ITM for a single row."""
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
        T = row['days_to_expiry'] / 365.0
        r = row['fedfunds_rate'] if pd.notna(row.get('fedfunds_rate')) else 0.02
        
        # Validate inputs
        if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or C <= 0 or S <= 0 or K <= 0 or T <= 0:
            return np.nan, np.nan
        
        # Check if option price needs adjustment
        # Pattern varies: some prices in dollars, some in dimes (×10), some in cents (×100)
        price_ratio = C / S if S > 0 else 0
        original_C = C
        
        # Calculate intrinsic value for validation
        intrinsic = max(0, S - K)
        
        # Try different price adjustments for ITM options
        if price_ratio > 2.0:
            # Try dividing by 100 first (cents)
            C_test = C / 100.0
            if intrinsic > 0.01:
                # For ITM, price must be >= intrinsic
                if C_test >= intrinsic * 0.9:
                    C = C_test
                else:
                    # Try dividing by 10 (dimes)
                    C_test = C / 10.0
                    if C_test >= intrinsic * 0.9 and C_test <= S * 1.1:
                        C = C_test
                    else:
                        # Try other divisors
                        for divisor in [20, 40, 50]:
                            C_test = C / divisor
                            if C_test >= intrinsic * 0.9 and C_test <= S * 1.1:
                                C = C_test
                                break
            else:
                # OTM option, just divide by 100
                C = C_test
        
        # Final validation: for ITM calls (S > K), price must be >= intrinsic
        if intrinsic > 0.01 and C < intrinsic * 0.9:
            # Price is less than 90% of intrinsic, which is impossible for ITM calls
            return np.nan, np.nan
        
        # For very small prices, use a minimum threshold
        if C < 0.01:
            return np.nan, np.nan
        
        # Check for impossible scenarios: call price cannot exceed spot price (for standard calls)
        if C > S * 1.1:  # Allow 10% tolerance for rounding/calculation errors
            return np.nan, np.nan
        
        # Calculate implied volatility
        iv = implied_volatility_call(C, S, K, T, r)
        
        # If IV calculation failed, return NaN
        if pd.isna(iv) or iv <= 0:
            return np.nan, np.nan
        
        # Calculate probability ITM using the calculated IV
        prob = probability_itm(S, K, T, r, iv)
        
        # Round to 2 decimal places
        iv = round(iv, 2) if pd.notna(iv) else np.nan
        prob = round(prob, 2) if pd.notna(prob) else np.nan
        
        return iv, prob
    except Exception as e:
        return np.nan, np.nan

# Find all PLTR monthly files
files = sorted(monthly_dir.glob('*_options_pessimistic.csv'))
print(f"Found {len(files)} PLTR monthly files to process")
print()

# Process each file
for file in files:
    print(f"Processing: {file.name}")
    
    try:
        # Read the file
        df = pd.read_csv(file)
        print(f"  Total rows: {len(df):,}")
        
        # Check current coverage
        iv_missing_before = df['implied_volatility'].isna().sum() if 'implied_volatility' in df.columns else len(df)
        prob_missing_before = df['probability_itm'].isna().sum() if 'probability_itm' in df.columns else len(df)
        
        # Ensure columns exist
        if 'implied_volatility' not in df.columns:
            df['implied_volatility'] = np.nan
        if 'probability_itm' not in df.columns:
            df['probability_itm'] = np.nan
        
        # Ensure mid_price exists
        if 'mid_price' not in df.columns:
            if 'high_price' in df.columns and 'low_price' in df.columns:
                df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
            elif 'close_price' in df.columns:
                df['mid_price'] = df['close_price']
            else:
                print(f"  ⚠️  No price column available, skipping")
                continue
        
        # Ensure fedfunds_rate exists
        if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
            df['fedfunds_rate'] = 0.02
        
        # Only process call options
        call_df = df[df['option_type'] == 'C'].copy()
        print(f"  Call options: {len(call_df):,}")
        
        if len(call_df) == 0:
            print(f"  ⚠️  No call options found, skipping")
            continue
        
        # Calculate IV and probability ITM
        print(f"  Calculating IV and probability ITM...")
        results = call_df.apply(calc_iv_prob, axis=1)
        call_df['implied_volatility'] = [r[0] for r in results]
        call_df['probability_itm'] = [r[1] for r in results]
        
        # Update the original dataframe
        df.loc[df['option_type'] == 'C', 'implied_volatility'] = call_df['implied_volatility']
        df.loc[df['option_type'] == 'C', 'probability_itm'] = call_df['probability_itm']
        
        # Check results
        iv_missing_after = df['implied_volatility'].isna().sum()
        prob_missing_after = df['probability_itm'].isna().sum()
        
        iv_calculated = (~df['implied_volatility'].isna()).sum()
        prob_calculated = (~df['probability_itm'].isna()).sum()
        
        print(f"  IV: {iv_calculated:,} calculated ({iv_calculated/len(df)*100:.1f}%), {iv_missing_after:,} missing")
        print(f"  Probability ITM: {prob_calculated:,} calculated ({prob_calculated/len(df)*100:.1f}%), {prob_missing_after:,} missing")
        
        # Save the updated file
        df.to_csv(file, index=False)
        print(f"  ✅ Saved: {file.name}")
        print()
        
    except Exception as e:
        print(f"  ❌ Error processing {file.name}: {e}")
        import traceback
        traceback.print_exc()
        print()
        continue

print("=" * 100)
print("✅ COMPLETE: All PLTR monthly files processed")
print("=" * 100)
