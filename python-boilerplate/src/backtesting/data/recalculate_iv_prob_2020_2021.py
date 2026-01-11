#!/usr/bin/env python3
"""
Recalculate implied volatility and probability ITM for 2020 and 2021 holiday files.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import importlib.util

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Load environment variables
load_dotenv()

# Get base path
base_path = Path(__file__).parent.parent.parent.parent

# Import helper functions
greeks2_path = Path(__file__).parent.parent / "greeks2.py"
spec = importlib.util.spec_from_file_location("greeks2", greeks2_path)
greeks2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(greeks2)

implied_volatility_call = greeks2.implied_volatility_call
probability_itm = greeks2.probability_itm

TICKER = 'AAPL'

for year in [2020, 2021]:
    print(f"\n{'='*80}")
    print(f"RECALCULATING IV AND PROB ITM FOR {year}")
    print(f"{'='*80}")
    
    file_path = base_path / "data" / TICKER / "holidays" / f"{year}_options_pessimistic.csv"
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        continue
    
    print(f"\n📖 Loading {file_path}...")
    df = pd.read_csv(file_path)
    print(f"   Loaded {len(df):,} rows")
    
    # Check current state
    iv_nulls_before = df['implied_volatility'].isna().sum()
    prob_nulls_before = df['probability_itm'].isna().sum()
    print(f"   Before: IV nulls: {iv_nulls_before}/{len(df)} ({iv_nulls_before/len(df)*100:.1f}%)")
    print(f"   Before: Prob ITM nulls: {prob_nulls_before}/{len(df)} ({prob_nulls_before/len(df)*100:.1f}%)")
    
    # Ensure required columns exist
    required_cols = ['close_price', 'underlying_spot', 'strike', 'days_to_expiry', 'fedfunds_rate']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        print(f"   ❌ Missing required columns: {missing_cols}")
        continue
    
    # Add fedfunds_rate if missing
    if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
        df['fedfunds_rate'] = 0.02
        print(f"   ⚠️  fedfunds_rate not found, using default 0.02")
    
    # Calculate T (time to expiry in years)
    df['T'] = df['days_to_expiry'] / 365.0
    
    print(f"\n📊 Calculating implied volatility and probability ITM...")
    
    def calc_iv_prob(row):
        try:
            C = row['close_price']
            S = row['underlying_spot']
            K = row['strike']
            T = row['T']
            r = row['fedfunds_rate'] if pd.notna(row['fedfunds_rate']) else 0.02
            
            if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or C <= 0 or S <= 0 or K <= 0 or T <= 0:
                return np.nan, np.nan
            
            iv = implied_volatility_call(C, S, K, T, r)
            if pd.isna(iv) or iv <= 0:
                return np.nan, np.nan
            
            prob = probability_itm(S, K, T, r, iv)
            return iv, prob
        except:
            return np.nan, np.nan
    
    iv_prob = df.apply(calc_iv_prob, axis=1)
    df['implied_volatility'] = [x[0] for x in iv_prob]
    df['probability_itm'] = [x[1] for x in iv_prob]
    df = df.drop(columns=['T'])
    
    # Round: 4 decimals for IV & prob_itm
    df['implied_volatility'] = df['implied_volatility'].round(4)
    df['probability_itm'] = df['probability_itm'].round(4)
    
    # Check results
    iv_nulls_after = df['implied_volatility'].isna().sum()
    prob_nulls_after = df['probability_itm'].isna().sum()
    iv_valid = df['implied_volatility'].notna().sum()
    prob_valid = df['probability_itm'].notna().sum()
    
    print(f"\n   After: IV nulls: {iv_nulls_after}/{len(df)} ({iv_nulls_after/len(df)*100:.1f}%)")
    print(f"   After: Prob ITM nulls: {prob_nulls_after}/{len(df)} ({prob_nulls_after/len(df)*100:.1f}%)")
    print(f"   ✅ IV valid: {iv_valid:,} ({iv_valid/len(df)*100:.1f}%)")
    print(f"   ✅ Prob ITM valid: {prob_valid:,} ({prob_valid/len(df)*100:.1f}%)")
    
    if iv_valid > 0:
        print(f"   IV range: {df['implied_volatility'].min():.4f} - {df['implied_volatility'].max():.4f}")
    if prob_valid > 0:
        print(f"   Prob ITM range: {df['probability_itm'].min():.4f} - {df['probability_itm'].max():.4f}")
    
    # Save
    print(f"\n💾 Saving to {file_path}...")
    df.to_csv(file_path, index=False)
    print(f"   ✅ Saved!")
    
print(f"\n{'='*80}")
print("✅ COMPLETE!")
print(f"{'='*80}")

