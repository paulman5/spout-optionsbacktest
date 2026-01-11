#!/usr/bin/env python3
"""
Fix strike prices for ALL tickers' holidays CSV files.
Rules:
- Extract strike from ticker: ticker_strike / 1000
- For dates BEFORE August 31, 2020: divide by 15
- For dates ON or AFTER August 31, 2020: keep as is (just /1000)

TSLA Special Rules:
- Before August 31, 2020: divide by 15
- August 31, 2020 - August 25, 2022: divide by 3
- On/After August 25, 2022: keep as is (just /1000)

XLK Special Rules:
- Before December 5, 2025: divide by 2
- On/After December 5, 2025: keep as is (just /1000)

XLF Special Rules:
- Before September 19, 2016: divide by 1.231
- On/After September 19, 2016: keep as is (just /1000)

IWM Special Rules:
- All dates: keep as is (just /1000, no stock splits)

AMZN Special Rules:
- Before June 6, 2022: divide by 20
- On/After June 6, 2022: keep as is (just /1000)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime, date


CUTOFF_DATE = date(2020, 8, 31)
TSLA_CUTOFF_DATE_1 = date(2020, 8, 31)  # First split date for TSLA
TSLA_CUTOFF_DATE_2 = date(2022, 8, 25)  # Second split date for TSLA
XLK_CUTOFF_DATE = date(2025, 12, 5)  # Split date for XLK
XLF_CUTOFF_DATE = date(2016, 9, 19)  # Split date for XLF
AMZN_CUTOFF_DATE = date(2022, 6, 6)  # Split date for AMZN


def extract_strike_from_ticker(ticker: str) -> float:
    """
    Extract strike from option ticker format: O:AAPL180126C00197500
    Returns: strike_raw / 1000.0
    """
    try:
        # Remove 'O:' prefix if present
        if ticker.startswith('O:'):
            ticker = ticker[2:]
        
        # Extract strike part: AAPL180126C00197500 -> 00197500
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        strike_str = match.group(4)  # e.g., "00197500"
        strike_raw = float(strike_str) / 1000.0  # e.g., 197.5
        
        return strike_raw
    except (ValueError, IndexError, AttributeError, TypeError):
        return None


def fix_holidays_file(holidays_file: Path):
    """
    Fix strikes in a single holidays CSV file.
    
    Args:
        holidays_file: Path to the holidays CSV file
    """
    print(f"\n{'='*80}")
    print(f"Processing: {holidays_file.name}")
    print(f"{'='*80}")
    
    # Load holidays data
    print(f"📂 Loading holidays data...")
    try:
        df = pd.read_csv(holidays_file)
        print(f"   Loaded {len(df):,} rows")
    except Exception as e:
        print(f"   ❌ Error loading file: {e}")
        return False
    
    # Check required columns
    if 'ticker' not in df.columns:
        print(f"   ❌ Missing 'ticker' column")
        return False
    
    if 'date_only' not in df.columns:
        print(f"   ❌ Missing 'date_only' column")
        return False
    
    # Convert date_only to datetime
    df['date_only_dt'] = pd.to_datetime(df['date_only'])
    
    # Get ticker name from file path
    ticker_name = holidays_file.parent.parent.name
    
    # Show sample of current strikes
    print(f"\n   Sample of current strikes (first 5 rows):")
    for idx, row in df.head(5).iterrows():
        print(f"      {row['ticker']} (date: {row['date_only']}) -> strike: {row.get('strike', 'N/A')}")
    
    # Extract correct strikes from tickers
    print(f"\n🔄 Extracting correct strikes from tickers...")
    df['strike_raw'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Check how many were successfully parsed
    parsed_count = df['strike_raw'].notna().sum()
    print(f"   Successfully parsed: {parsed_count:,} / {len(df):,} ({100*parsed_count/len(df):.1f}%)")
    
    if parsed_count < len(df):
        failed = df[df['strike_raw'].isna()]
        print(f"   ⚠️  Failed to parse {len(failed)} tickers")
        if len(failed) <= 5:
            for idx, row in failed.iterrows():
                print(f"      - {row['ticker']}")
        else:
            for idx, row in failed.head(5).iterrows():
                print(f"      - {row['ticker']}")
            print(f"      ... and {len(failed) - 5} more")
    
    # Apply date-based division
    print(f"\n📅 Applying date-based strike adjustment...")
    
    # Check if this is TSLA (special rules)
    if ticker_name == 'TSLA':
        before_cutoff1 = df['date_only_dt'].dt.date < TSLA_CUTOFF_DATE_1
        between_cutoffs = (df['date_only_dt'].dt.date >= TSLA_CUTOFF_DATE_1) & (df['date_only_dt'].dt.date < TSLA_CUTOFF_DATE_2)
        after_cutoff2 = df['date_only_dt'].dt.date >= TSLA_CUTOFF_DATE_2
        
        print(f"   Before August 31, 2020: {before_cutoff1.sum():,} rows (divide by 15)")
        print(f"   August 31, 2020 - August 25, 2022: {between_cutoffs.sum():,} rows (divide by 3)")
        print(f"   On/After August 25, 2022: {after_cutoff2.sum():,} rows (no division)")
        
        # Calculate final strike for TSLA
        df['strike_corrected'] = np.nan
        df.loc[before_cutoff1, 'strike_corrected'] = (df.loc[before_cutoff1, 'strike_raw'] / 15.0).round(2)
        df.loc[between_cutoffs, 'strike_corrected'] = (df.loc[between_cutoffs, 'strike_raw'] / 3.0).round(2)
        df.loc[after_cutoff2, 'strike_corrected'] = df.loc[after_cutoff2, 'strike_raw'].round(2)
    elif ticker_name == 'XLK':
        # For XLK: divide by 2 before December 5, 2025
        before_cutoff = df['date_only_dt'].dt.date < XLK_CUTOFF_DATE
        after_cutoff = df['date_only_dt'].dt.date >= XLK_CUTOFF_DATE
        
        print(f"   Before December 5, 2025: {before_cutoff.sum():,} rows (divide by 2)")
        print(f"   On/After December 5, 2025: {after_cutoff.sum():,} rows (no division)")
        
        # Calculate final strike for XLK
        df['strike_corrected'] = np.nan
        df.loc[before_cutoff, 'strike_corrected'] = (df.loc[before_cutoff, 'strike_raw'] / 2.0).round(2)
        df.loc[after_cutoff, 'strike_corrected'] = df.loc[after_cutoff, 'strike_raw'].round(2)
    elif ticker_name == 'XLF':
        # For XLF: divide by 1.231 before September 19, 2016
        before_cutoff = df['date_only_dt'].dt.date < XLF_CUTOFF_DATE
        after_cutoff = df['date_only_dt'].dt.date >= XLF_CUTOFF_DATE
        
        print(f"   Before September 19, 2016: {before_cutoff.sum():,} rows (divide by 1.231)")
        print(f"   On/After September 19, 2016: {after_cutoff.sum():,} rows (no division)")
        
        # Calculate final strike for XLF
        df['strike_corrected'] = np.nan
        df.loc[before_cutoff, 'strike_corrected'] = (df.loc[before_cutoff, 'strike_raw'] / 1.231).round(2)
        df.loc[after_cutoff, 'strike_corrected'] = df.loc[after_cutoff, 'strike_raw'].round(2)
    elif ticker_name == 'IWM':
        # For IWM: no stock splits, just divide by 1000
        print(f"   All rows: {len(df):,} rows (divide by 1000 only, no stock splits)")
        
        # Calculate final strike for IWM
        df['strike_corrected'] = df['strike_raw'].round(2)
    elif ticker_name == 'AMZN':
        # For AMZN: divide by 20 before June 6, 2022
        before_cutoff = df['date_only_dt'].dt.date < AMZN_CUTOFF_DATE
        after_cutoff = df['date_only_dt'].dt.date >= AMZN_CUTOFF_DATE
        
        print(f"   Before June 6, 2022: {before_cutoff.sum():,} rows (divide by 20)")
        print(f"   On/After June 6, 2022: {after_cutoff.sum():,} rows (no division)")
        
        # Calculate final strike for AMZN
        df['strike_corrected'] = np.nan
        df.loc[before_cutoff, 'strike_corrected'] = (df.loc[before_cutoff, 'strike_raw'] / 20.0).round(2)
        df.loc[after_cutoff, 'strike_corrected'] = df.loc[after_cutoff, 'strike_raw'].round(2)
    else:
        # For all other tickers: standard rules
        before_cutoff = df['date_only_dt'].dt.date < CUTOFF_DATE
        after_cutoff = df['date_only_dt'].dt.date >= CUTOFF_DATE
        
        print(f"   Before August 31, 2020: {before_cutoff.sum():,} rows (divide by 15)")
        print(f"   On/After August 31, 2020: {after_cutoff.sum():,} rows (no division)")
        
        # Calculate final strike
        df['strike_corrected'] = np.nan
        df.loc[before_cutoff, 'strike_corrected'] = (df.loc[before_cutoff, 'strike_raw'] / 15.0).round(2)
        df.loc[after_cutoff, 'strike_corrected'] = df.loc[after_cutoff, 'strike_raw'].round(2)
    
    # Compare old vs new strikes (if strike column exists)
    if 'strike' in df.columns:
        df_comparison = df[df['strike_corrected'].notna() & df['strike'].notna()].copy()
        if len(df_comparison) > 0:
            df_comparison['strike_diff'] = df_comparison['strike_corrected'] - df_comparison['strike']
            df_comparison['strike_ratio'] = df_comparison['strike_corrected'] / df_comparison['strike']
            
            print(f"\n   Strike comparison (where both exist):")
            print(f"      Average old strike: {df_comparison['strike'].mean():.2f}")
            print(f"      Average new strike: {df_comparison['strike_corrected'].mean():.2f}")
            print(f"      Average difference: {df_comparison['strike_diff'].mean():.2f}")
            print(f"      Average ratio: {df_comparison['strike_ratio'].mean():.4f}")
            
            # Show sample of corrections
            print(f"\n   Sample of corrections (first 5 rows with changes):")
            sample = df_comparison.head(5)
            for idx, row in sample.iterrows():
                print(f"      {row['ticker']} (date: {row['date_only']})")
                print(f"         Old: {row['strike']:.2f} -> New: {row['strike_corrected']:.2f} (diff: {row['strike_diff']:.2f})")
    
    # Update strikes (ensure rounded to 2 decimal places)
    print(f"\n💾 Updating strikes...")
    df['strike'] = df['strike_corrected'].round(2)
    df = df.drop(columns=['strike_corrected', 'strike_raw', 'date_only_dt'], errors='ignore')
    
    # Save updated file
    print(f"💾 Saving updated file...")
    try:
        df.to_csv(holidays_file, index=False)
        print(f"   ✅ Saved {holidays_file}")
        return True
    except Exception as e:
        print(f"   ❌ Error saving file: {e}")
        return False


def main():
    """Main function to fix all holidays files for all tickers."""
    base_path = Path(__file__).parent / "python-boilerplate" / "data"
    
    if not base_path.exists():
        print(f"❌ Data directory not found: {base_path}")
        return
    
    # Find all holidays directories
    holidays_files = []
    ticker_files = {}
    
    for ticker_dir in sorted(base_path.iterdir()):
        if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
            continue
        
        holidays_dir = ticker_dir / "holidays"
        if holidays_dir.exists():
            # Find all CSV files in holidays directory
            csv_files = sorted(holidays_dir.glob("*_options_pessimistic.csv"))
            if csv_files:
                holidays_files.extend(csv_files)
                ticker_files[ticker_dir.name] = csv_files
    
    if not holidays_files:
        print(f"❌ No holidays CSV files found")
        return
    
    print("=" * 80)
    print(f"FIXING STRIKE PRICES FOR ALL HOLIDAYS FILES")
    print("=" * 80)
    print(f"Rules:")
    print(f"  1. Extract strike from ticker: ticker_strike / 1000")
    print(f"  2. Before August 31, 2020: divide by 15")
    print(f"  3. On/After August 31, 2020: keep as is (just /1000)")
    print(f"\nFound {len(holidays_files)} holidays files across {len(ticker_files)} tickers")
    print("=" * 80)
    
    # Process each ticker
    success_count = 0
    failed_count = 0
    
    for ticker, files in sorted(ticker_files.items()):
        print(f"\n📊 {ticker}: {len(files)} files")
        ticker_success = 0
        ticker_failed = 0
        
        for holidays_file in files:
            if fix_holidays_file(holidays_file):
                success_count += 1
                ticker_success += 1
            else:
                failed_count += 1
                ticker_failed += 1
        
        if ticker_failed == 0:
            print(f"   ✅ All {ticker_success} files processed successfully")
        else:
            print(f"   ⚠️  {ticker_success} successful, {ticker_failed} failed")
    
    print("\n" + "=" * 80)
    print("✅ PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"   Successfully updated: {success_count} / {len(holidays_files)} files")
    if failed_count > 0:
        print(f"   Failed: {failed_count} files")
    print("=" * 80)


if __name__ == "__main__":
    main()

