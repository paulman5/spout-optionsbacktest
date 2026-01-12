#!/usr/bin/env python3
"""
Recalculate implied volatility and probability ITM for tickers with 0% coverage.
Focuses on the 22 tickers that have no IV/prob ITM data.
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

# Focus on the 22 tickers with 0% coverage
target_tickers = [
    'AVGO', 'COST', 'CRM', 'CRWD', 'CSCO', 'INTC', 'KO', 'LRCX', 
    'MRK', 'MU', 'NFLX', 'ORCL', 'PLTR', 'QCOM', 'SMCI', 'SOFI', 
    'SPOT', 'UBER', 'UNH', 'V', 'WMT', 'XOM'
]

print("=" * 100)
print("RECALCULATING IV AND PROBABILITY ITM FOR TICKERS WITH 0% COVERAGE")
print("=" * 100)
print(f"Target tickers: {', '.join(target_tickers)}")
print(f"Total: {len(target_tickers)} tickers")
print()

total_files_processed = 0
total_rows_processed = 0
total_iv_added = 0
total_prob_added = 0
files_failed = []

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

for ticker in target_tickers:
    ticker_dir = base_dir / ticker
    if not ticker_dir.exists():
        print(f"\n⚠️  {ticker}: Directory not found, skipping...")
        continue
    
    print(f"\n{'='*100}")
    print(f"Processing {ticker}")
    print(f"{'='*100}")
    
    ticker_files = 0
    ticker_rows = 0
    ticker_iv_added = 0
    ticker_prob_added = 0
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            year = file.stem.split('_')[0]
            print(f"\n  {subdir}/{year}...", end=" ", flush=True)
            
            try:
                df = pd.read_csv(file)
                original_rows = len(df)
                
                # Check current state
                iv_valid_before = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                prob_valid_before = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
                
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
                    print(f"❌ Missing columns: {missing_cols}")
                    files_failed.append((f"{ticker}/{subdir}/{year}", f"Missing columns: {missing_cols}"))
                    continue
                
                # Add fedfunds_rate if missing
                if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
                    df['fedfunds_rate'] = 0.02
                
                # Calculate T (time to expiry in years)
                df['T'] = df['days_to_expiry'] / 365.0
                
                # Apply calculation to all rows
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
                iv_added = iv_valid_after - iv_valid_before
                prob_added = prob_valid_after - prob_valid_before
                
                # Save the file
                df.to_csv(file, index=False)
                
                print(f"✅ {iv_valid_after:,}/{original_rows:,} IV ({iv_valid_after/original_rows*100:.1f}%), "
                      f"{prob_valid_after:,}/{original_rows:,} Prob ({prob_valid_after/original_rows*100:.1f}%), "
                      f"+{iv_added:,} IV, +{prob_added:,} Prob")
                
                total_files_processed += 1
                ticker_files += 1
                total_rows_processed += original_rows
                ticker_rows += original_rows
                total_iv_added += iv_added
                ticker_iv_added += iv_added
                total_prob_added += prob_added
                ticker_prob_added += prob_added
                
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                files_failed.append((f"{ticker}/{subdir}/{year}", str(e)))
                import traceback
                traceback.print_exc()
    
    # Print ticker summary
    if ticker_files > 0:
        print(f"\n  {ticker} Summary: {ticker_files} files, {ticker_rows:,} rows, "
              f"+{ticker_iv_added:,} IV, +{ticker_prob_added:,} Prob")

print(f"\n{'='*100}")
print("SUMMARY")
print(f"{'='*100}")
print(f"✅ Successfully processed: {total_files_processed} files")
print(f"📊 Total rows processed: {total_rows_processed:,}")
print(f"📈 Total IV values added: {total_iv_added:,}")
print(f"📈 Total Prob ITM values added: {total_prob_added:,}")

if files_failed:
    print(f"\n❌ Failed files: {len(files_failed)}")
    for file_path, error in files_failed[:10]:  # Show first 10 failures
        print(f"   {file_path}: {error}")
    if len(files_failed) > 10:
        print(f"   ... and {len(files_failed) - 10} more")

print(f"{'='*100}")

#!/usr/bin/env python3
"""
Recalculate implied volatility and probability ITM for tickers with 0% coverage.
Focuses on the 22 tickers that have no IV/prob ITM data.
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

# Focus on the 22 tickers with 0% coverage
target_tickers = [
    'AVGO', 'COST', 'CRM', 'CRWD', 'CSCO', 'INTC', 'KO', 'LRCX', 
    'MRK', 'MU', 'NFLX', 'ORCL', 'PLTR', 'QCOM', 'SMCI', 'SOFI', 
    'SPOT', 'UBER', 'UNH', 'V', 'WMT', 'XOM'
]

print("=" * 100)
print("RECALCULATING IV AND PROBABILITY ITM FOR TICKERS WITH 0% COVERAGE")
print("=" * 100)
print(f"Target tickers: {', '.join(target_tickers)}")
print(f"Total: {len(target_tickers)} tickers")
print()

total_files_processed = 0
total_rows_processed = 0
total_iv_added = 0
total_prob_added = 0
files_failed = []

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

for ticker in target_tickers:
    ticker_dir = base_dir / ticker
    if not ticker_dir.exists():
        print(f"\n⚠️  {ticker}: Directory not found, skipping...")
        continue
    
    print(f"\n{'='*100}")
    print(f"Processing {ticker}")
    print(f"{'='*100}")
    
    ticker_files = 0
    ticker_rows = 0
    ticker_iv_added = 0
    ticker_prob_added = 0
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        files = sorted(data_dir.glob('*_options_pessimistic.csv'))
        
        for file in files:
            year = file.stem.split('_')[0]
            print(f"\n  {subdir}/{year}...", end=" ", flush=True)
            
            try:
                df = pd.read_csv(file)
                original_rows = len(df)
                
                # Check current state
                iv_valid_before = df['implied_volatility'].notna().sum() if 'implied_volatility' in df.columns else 0
                prob_valid_before = df['probability_itm'].notna().sum() if 'probability_itm' in df.columns else 0
                
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
                    print(f"❌ Missing columns: {missing_cols}")
                    files_failed.append((f"{ticker}/{subdir}/{year}", f"Missing columns: {missing_cols}"))
                    continue
                
                # Add fedfunds_rate if missing
                if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
                    df['fedfunds_rate'] = 0.02
                
                # Calculate T (time to expiry in years)
                df['T'] = df['days_to_expiry'] / 365.0
                
                # Apply calculation to all rows
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
                iv_added = iv_valid_after - iv_valid_before
                prob_added = prob_valid_after - prob_valid_before
                
                # Save the file
                df.to_csv(file, index=False)
                
                print(f"✅ {iv_valid_after:,}/{original_rows:,} IV ({iv_valid_after/original_rows*100:.1f}%), "
                      f"{prob_valid_after:,}/{original_rows:,} Prob ({prob_valid_after/original_rows*100:.1f}%), "
                      f"+{iv_added:,} IV, +{prob_added:,} Prob")
                
                total_files_processed += 1
                ticker_files += 1
                total_rows_processed += original_rows
                ticker_rows += original_rows
                total_iv_added += iv_added
                ticker_iv_added += iv_added
                total_prob_added += prob_added
                ticker_prob_added += prob_added
                
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                files_failed.append((f"{ticker}/{subdir}/{year}", str(e)))
                import traceback
                traceback.print_exc()
    
    # Print ticker summary
    if ticker_files > 0:
        print(f"\n  {ticker} Summary: {ticker_files} files, {ticker_rows:,} rows, "
              f"+{ticker_iv_added:,} IV, +{ticker_prob_added:,} Prob")

print(f"\n{'='*100}")
print("SUMMARY")
print(f"{'='*100}")
print(f"✅ Successfully processed: {total_files_processed} files")
print(f"📊 Total rows processed: {total_rows_processed:,}")
print(f"📈 Total IV values added: {total_iv_added:,}")
print(f"📈 Total Prob ITM values added: {total_prob_added:,}")

if files_failed:
    print(f"\n❌ Failed files: {len(files_failed)}")
    for file_path, error in files_failed[:10]:  # Show first 10 failures
        print(f"   {file_path}: {error}")
    if len(files_failed) > 10:
        print(f"   ... and {len(files_failed) - 10} more")

print(f"{'='*100}")

