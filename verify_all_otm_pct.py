#!/usr/bin/env python3
"""
Verify OTM percentage calculation for all tickers' holidays CSV files.
Formula: otm_pct = ((strike - underlying_spot) / underlying_spot) * 100
Output: Maximum 2 decimal places
"""

import pandas as pd
import numpy as np
from pathlib import Path


def verify_file(holidays_file: Path):
    """
    Verify otm_pct calculation in a single holidays CSV file.
    
    Returns:
        (is_correct, error_message, stats)
    """
    try:
        df = pd.read_csv(holidays_file)
    except Exception as e:
        return False, f"Error loading: {e}", None
    
    # Check required columns
    required_cols = ['strike', 'underlying_spot', 'otm_pct']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return False, f"Missing columns: {missing_cols}", None
    
    # Find valid rows
    mask = df['strike'].notna() & df['underlying_spot'].notna() & (df['underlying_spot'] != 0) & df['otm_pct'].notna()
    valid_rows = df[mask]
    
    if len(valid_rows) == 0:
        return False, "No valid rows with strike, underlying_spot, and otm_pct", None
    
    # Verify calculation for all valid rows
    expected = ((valid_rows['strike'] - valid_rows['underlying_spot']) / valid_rows['underlying_spot']) * 100
    expected_rounded = expected.round(2)
    actual = valid_rows['otm_pct']
    
    # Check if all match (within 0.01 tolerance)
    differences = np.abs(expected_rounded - actual)
    mismatches = differences >= 0.01
    
    if mismatches.sum() > 0:
        mismatch_count = mismatches.sum()
        max_diff = differences.max()
        sample_mismatch = valid_rows[mismatches].iloc[0]
        return False, f"{mismatch_count}/{len(valid_rows)} rows incorrect (max diff: {max_diff:.4f})", {
            'total_rows': len(df),
            'valid_rows': len(valid_rows),
            'mismatches': mismatch_count,
            'sample': {
                'strike': sample_mismatch['strike'],
                'spot': sample_mismatch['underlying_spot'],
                'expected': round(((sample_mismatch['strike'] - sample_mismatch['underlying_spot']) / sample_mismatch['underlying_spot']) * 100, 2),
                'actual': sample_mismatch['otm_pct']
            }
        }
    
    # All correct
    stats = {
        'total_rows': len(df),
        'valid_rows': len(valid_rows),
        'mismatches': 0
    }
    return True, None, stats


def main():
    """Main function to verify all holidays files for all tickers."""
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
    print(f"VERIFYING OTM_PCT CALCULATION FOR ALL HOLIDAYS FILES")
    print("=" * 80)
    print(f"Formula: otm_pct = ((strike - underlying_spot) / underlying_spot) * 100")
    print(f"Output: Maximum 2 decimal places")
    print(f"\nFound {len(holidays_files)} holidays files across {len(ticker_files)} tickers")
    print("=" * 80)
    
    # Verify each ticker
    ticker_results = {}
    
    for ticker, files in sorted(ticker_files.items()):
        print(f"\n📊 {ticker}: {len(files)} files")
        correct_count = 0
        incorrect_count = 0
        error_count = 0
        total_rows = 0
        
        for holidays_file in files:
            is_correct, error_msg, stats = verify_file(holidays_file)
            
            if stats:
                total_rows += stats['valid_rows']
            
            if is_correct:
                correct_count += 1
            elif error_msg and "No valid rows" in error_msg:
                error_count += 1
            else:
                incorrect_count += 1
                year = holidays_file.stem.split('_')[0]
                print(f"   ❌ {year}: {error_msg}")
                if stats and 'sample' in stats:
                    s = stats['sample']
                    print(f"      Sample: Strike={s['strike']:.2f}, Spot={s['spot']:.2f}, Expected={s['expected']:.2f}, Actual={s['actual']:.2f}")
        
        ticker_results[ticker] = {
            'correct': correct_count,
            'incorrect': incorrect_count,
            'errors': error_count,
            'total_files': len(files),
            'total_rows': total_rows
        }
        
        if incorrect_count == 0 and error_count == 0:
            print(f"   ✅ All {correct_count} files verified correctly ({total_rows:,} rows)")
        elif incorrect_count == 0:
            print(f"   ✅ {correct_count} files correct, {error_count} files with no data")
        else:
            print(f"   ⚠️  {correct_count} correct, {incorrect_count} incorrect, {error_count} errors")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    total_correct = sum(r['correct'] for r in ticker_results.values())
    total_incorrect = sum(r['incorrect'] for r in ticker_results.values())
    total_errors = sum(r['errors'] for r in ticker_results.values())
    total_files = sum(r['total_files'] for r in ticker_results.values())
    total_rows = sum(r['total_rows'] for r in ticker_results.values())
    
    print(f"Total files: {total_files}")
    print(f"  ✅ Correct: {total_correct}")
    print(f"  ❌ Incorrect: {total_incorrect}")
    print(f"  ⚠️  No data: {total_errors}")
    print(f"Total valid rows verified: {total_rows:,}")
    
    if total_incorrect > 0:
        print(f"\n⚠️  {total_incorrect} files need fixing!")
        print("\nTickers with issues:")
        for ticker, results in sorted(ticker_results.items()):
            if results['incorrect'] > 0:
                print(f"  {ticker}: {results['incorrect']} files incorrect")
    else:
        print(f"\n✅ All files with data are correctly calculated!")
    
    print("=" * 80)


if __name__ == "__main__":
    main()

