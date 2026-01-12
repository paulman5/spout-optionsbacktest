#!/usr/bin/env python3
"""
Efficient batch recalculation of IV and probability ITM.
Processes files one at a time with progress tracking.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("BATCH IV AND PROBABILITY ITM RECALCULATION")
print("=" * 100)
print()

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

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

# Collect all files first
all_files = []
for ticker_dir in ticker_dirs:
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if data_dir.exists():
            files = sorted(data_dir.glob('*_options_pessimistic.csv'))
            for file in files:
                all_files.append((ticker_dir.name, subdir, file))

print(f"Found {len(all_files)} files to process")
print()

total_files_processed = 0
total_iv_added = 0
total_prob_added = 0

# Process files one at a time
for i, (ticker, subdir, file) in enumerate(all_files, 1):
    try:
        df = pd.read_csv(file)
        rows = len(df)
        
        # Ensure mid_price exists
        if 'mid_price' not in df.columns:
            if 'high_price' in df.columns and 'low_price' in df.columns:
                df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
            elif 'close_price' in df.columns:
                df['mid_price'] = df['close_price']
            else:
                continue
        
        # Ensure fedfunds_rate exists
        if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
            df['fedfunds_rate'] = 0.02
        
        # Ensure IV and prob columns exist
        if 'implied_volatility' not in df.columns:
            df['implied_volatility'] = np.nan
        if 'probability_itm' not in df.columns:
            df['probability_itm'] = np.nan
        
        # Find rows that need IV calculation
        has_price = (df['mid_price'].notna() & (df['mid_price'] >= 0.01))
        if 'close_price' in df.columns:
            has_price = has_price | (df['close_price'].notna() & (df['close_price'] >= 0.01))
        
        has_spot = df['underlying_spot'].notna() & (df['underlying_spot'] > 0) if 'underlying_spot' in df.columns else pd.Series([False] * len(df))
        has_strike = df['strike'].notna() & (df['strike'] > 0) if 'strike' in df.columns else pd.Series([False] * len(df))
        has_dte = df['days_to_expiry'].notna() & (df['days_to_expiry'] > 0) if 'days_to_expiry' in df.columns else pd.Series([False] * len(df))
        
        can_calculate = has_price & has_spot & has_strike & has_dte
        missing_iv = can_calculate & df['implied_volatility'].isna()
        
        if missing_iv.sum() == 0:
            continue
        
        # Calculate IV and prob for missing rows in batches
        df['T'] = df['days_to_expiry'] / 365.0
        
        # Process in smaller batches to avoid memory issues
        batch_size = 1000
        missing_indices = df[missing_iv].index.tolist()
        
        iv_added = 0
        prob_added = 0
        
        for batch_start in range(0, len(missing_indices), batch_size):
            batch_end = min(batch_start + batch_size, len(missing_indices))
            batch_indices = missing_indices[batch_start:batch_end]
            
            batch_df = df.loc[batch_indices]
            iv_prob_results = batch_df.apply(calc_iv_prob, axis=1)
            
            for idx, (iv, prob) in iv_prob_results.items():
                if pd.notna(iv):
                    df.at[idx, 'implied_volatility'] = iv
                    iv_added += 1
                if pd.notna(prob):
                    df.at[idx, 'probability_itm'] = prob
                    prob_added += 1
        
        # Drop temporary column
        df = df.drop(columns=['T'], errors='ignore')
        
        # Save the file
        df.to_csv(file, index=False)
        
        total_files_processed += 1
        total_iv_added += iv_added
        total_prob_added += prob_added
        
        if iv_added > 0:
            print(f"[{i}/{len(all_files)}] ✅ {ticker}/{subdir}/{file.name}: +{iv_added:,} IV, +{prob_added:,} Prob")
        
    except Exception as e:
        print(f"[{i}/{len(all_files)}] ❌ Error processing {file.name}: {str(e)}")
        continue
    
    # Show progress every 10 files
    if i % 10 == 0:
        print(f"Progress: {i}/{len(all_files)} files, +{total_iv_added:,} IV, +{total_prob_added:,} Prob")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files processed: {total_files_processed:,}")
print(f"IV values added: {total_iv_added:,}")
print(f"Probability ITM values added: {total_prob_added:,}")
print("=" * 100)

#!/usr/bin/env python3
"""
Efficient batch recalculation of IV and probability ITM.
Processes files one at a time with progress tracking.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add the src directory to the path to import greeks2
sys.path.insert(0, str(Path(__file__).parent / "python-boilerplate" / "src" / "backtesting"))

from greeks2 import implied_volatility_call, probability_itm

base_dir = Path("python-boilerplate/data")
directories = ['holidays', 'monthly']

print("=" * 100)
print("BATCH IV AND PROBABILITY ITM RECALCULATION")
print("=" * 100)
print()

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

# Find all tickers
ticker_dirs = [d for d in base_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
ticker_dirs = sorted(ticker_dirs)

# Collect all files first
all_files = []
for ticker_dir in ticker_dirs:
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if data_dir.exists():
            files = sorted(data_dir.glob('*_options_pessimistic.csv'))
            for file in files:
                all_files.append((ticker_dir.name, subdir, file))

print(f"Found {len(all_files)} files to process")
print()

total_files_processed = 0
total_iv_added = 0
total_prob_added = 0

# Process files one at a time
for i, (ticker, subdir, file) in enumerate(all_files, 1):
    try:
        df = pd.read_csv(file)
        rows = len(df)
        
        # Ensure mid_price exists
        if 'mid_price' not in df.columns:
            if 'high_price' in df.columns and 'low_price' in df.columns:
                df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
            elif 'close_price' in df.columns:
                df['mid_price'] = df['close_price']
            else:
                continue
        
        # Ensure fedfunds_rate exists
        if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
            df['fedfunds_rate'] = 0.02
        
        # Ensure IV and prob columns exist
        if 'implied_volatility' not in df.columns:
            df['implied_volatility'] = np.nan
        if 'probability_itm' not in df.columns:
            df['probability_itm'] = np.nan
        
        # Find rows that need IV calculation
        has_price = (df['mid_price'].notna() & (df['mid_price'] >= 0.01))
        if 'close_price' in df.columns:
            has_price = has_price | (df['close_price'].notna() & (df['close_price'] >= 0.01))
        
        has_spot = df['underlying_spot'].notna() & (df['underlying_spot'] > 0) if 'underlying_spot' in df.columns else pd.Series([False] * len(df))
        has_strike = df['strike'].notna() & (df['strike'] > 0) if 'strike' in df.columns else pd.Series([False] * len(df))
        has_dte = df['days_to_expiry'].notna() & (df['days_to_expiry'] > 0) if 'days_to_expiry' in df.columns else pd.Series([False] * len(df))
        
        can_calculate = has_price & has_spot & has_strike & has_dte
        missing_iv = can_calculate & df['implied_volatility'].isna()
        
        if missing_iv.sum() == 0:
            continue
        
        # Calculate IV and prob for missing rows in batches
        df['T'] = df['days_to_expiry'] / 365.0
        
        # Process in smaller batches to avoid memory issues
        batch_size = 1000
        missing_indices = df[missing_iv].index.tolist()
        
        iv_added = 0
        prob_added = 0
        
        for batch_start in range(0, len(missing_indices), batch_size):
            batch_end = min(batch_start + batch_size, len(missing_indices))
            batch_indices = missing_indices[batch_start:batch_end]
            
            batch_df = df.loc[batch_indices]
            iv_prob_results = batch_df.apply(calc_iv_prob, axis=1)
            
            for idx, (iv, prob) in iv_prob_results.items():
                if pd.notna(iv):
                    df.at[idx, 'implied_volatility'] = iv
                    iv_added += 1
                if pd.notna(prob):
                    df.at[idx, 'probability_itm'] = prob
                    prob_added += 1
        
        # Drop temporary column
        df = df.drop(columns=['T'], errors='ignore')
        
        # Save the file
        df.to_csv(file, index=False)
        
        total_files_processed += 1
        total_iv_added += iv_added
        total_prob_added += prob_added
        
        if iv_added > 0:
            print(f"[{i}/{len(all_files)}] ✅ {ticker}/{subdir}/{file.name}: +{iv_added:,} IV, +{prob_added:,} Prob")
        
    except Exception as e:
        print(f"[{i}/{len(all_files)}] ❌ Error processing {file.name}: {str(e)}")
        continue
    
    # Show progress every 10 files
    if i % 10 == 0:
        print(f"Progress: {i}/{len(all_files)} files, +{total_iv_added:,} IV, +{total_prob_added:,} Prob")

print("\n" + "=" * 100)
print("SUMMARY")
print("=" * 100)
print(f"Files processed: {total_files_processed:,}")
print(f"IV values added: {total_iv_added:,}")
print(f"Probability ITM values added: {total_prob_added:,}")
print("=" * 100)

