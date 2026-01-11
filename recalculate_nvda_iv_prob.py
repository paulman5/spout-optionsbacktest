#!/usr/bin/env python3
"""
Recalculate implied volatility and probability ITM for all NVDA holidays and monthly files
using greeks2.py functions.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

base_dir = Path("python-boilerplate/data/NVDA")
directories = ['holidays', 'monthly']

print("=" * 80)
print("RECALCULATING IV AND PROBABILITY ITM FOR ALL NVDA FILES")
print("=" * 80)
print()

# First, check current state
print("📊 Checking current state...")
total_rows_before = 0
total_iv_before = 0
total_prob_before = 0

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        continue
    
    files = sorted(data_dir.glob('*_options_pessimistic.csv'))
    for file in files:
        df = pd.read_csv(file)
        rows = len(df)
        iv_valid = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
        prob_valid = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
        total_rows_before += rows
        total_iv_before += iv_valid
        total_prob_before += prob_valid

print(f"   Before: {total_rows_before:,} total rows")
print(f"   IV valid: {total_iv_before:,} ({total_iv_before/total_rows_before*100:.1f}%)")
print(f"   Prob ITM valid: {total_prob_before:,} ({total_prob_before/total_rows_before*100:.1f}%)")
print()

# Process each directory
total_rows_after = 0
total_iv_after = 0
total_prob_after = 0
files_processed = 0

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        print(f"⚠️  Directory not found: {data_dir}")
        continue
    
    files = sorted(data_dir.glob('*_options_pessimistic.csv'))
    print(f"\n{'='*80}")
    print(f"Processing {subdir.upper()} directory ({len(files)} files)")
    print(f"{'='*80}")
    
    for file in files:
        year = file.stem.split('_')[0]
        print(f"\nProcessing {subdir}/{year}...")
        
        try:
            df = pd.read_csv(file)
            original_rows = len(df)
            
            # Check current state
            iv_valid_before = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
            prob_valid_before = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
            print(f"   Rows: {original_rows:,}")
            print(f"   Before: IV: {iv_valid_before:,} ({iv_valid_before/original_rows*100:.1f}%), Prob: {prob_valid_before:,} ({prob_valid_before/original_rows*100:.1f}%)")
            
            # Ensure mid_price exists
            if 'mid_price' not in df.columns:
                if 'high_price' in df.columns and 'low_price' in df.columns:
                    df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
                else:
                    df['mid_price'] = df['close_price']
            
            # Check required columns
            required_cols = ['mid_price', 'underlying_spot', 'strike', 'days_to_expiry']
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                print(f"   ❌ Missing required columns: {missing_cols}")
                continue
            
            # Add fedfunds_rate if missing
            if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
                df['fedfunds_rate'] = 0.02
            
            # Calculate T (time to expiry in years)
            df['T'] = df['days_to_expiry'] / 365.0
            
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
            print(f"   Calculating IV and probability ITM...")
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
            
            print(f"   After: IV: {iv_valid_after:,} ({iv_valid_after/original_rows*100:.1f}%), Prob: {iv_valid_after:,} ({prob_valid_after/original_rows*100:.1f}%)")
            print(f"   Improvement: IV: +{iv_valid_after - iv_valid_before:,}, Prob: +{prob_valid_after - prob_valid_before:,}")
            
            if iv_valid_after > 0:
                print(f"   IV range: {df['implied_volatility'].min():.4f} - {df['implied_volatility'].max():.4f}")
            
            # Save the file
            df.to_csv(file, index=False)
            print(f"   ✅ Saved")
            
            total_rows_after += original_rows
            total_iv_after += iv_valid_after
            total_prob_after += prob_valid_after
            files_processed += 1
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"✅ Successfully processed: {files_processed} files")
print(f"Total rows: {total_rows_after:,}")
print(f"IV valid: {total_iv_after:,} ({total_iv_after/total_rows_after*100:.1f}%)")
print(f"Prob ITM valid: {total_prob_after:,} ({total_prob_after/total_rows_after*100:.1f}%)")
print(f"Improvement: IV: +{total_iv_after - total_iv_before:,}, Prob: +{total_prob_after - total_prob_before:,}")
print(f"{'='*80}")
