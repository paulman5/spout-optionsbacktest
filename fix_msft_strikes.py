"""
Fix MSFT strikes in all holidays CSV files.
For MSFT, strikes should be: ticker_strike / 1000 (not / 1000 / 4)
Example: MSFT180105C00074000 -> 00074000 / 1000 = 74.0
"""

import pandas as pd
import re
from pathlib import Path


def extract_strike_from_ticker(ticker: str) -> float:
    """
    Extract strike from MSFT option ticker and divide by 1000.
    Example: MSFT180105C00074000 -> 00074000 / 1000 = 74.0
    """
    try:
        # Remove 'O:' prefix if present
        if ticker.startswith('O:'):
            ticker = ticker[2:]
        
        # Extract strike part: MSFT180105C00074000 -> 00074000
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        strike_str = match.group(4)  # e.g., "00074000"
        strike_raw = float(strike_str)  # e.g., 74000.0
        strike = strike_raw / 1000.0  # e.g., 74.0
        
        return strike
    except (ValueError, IndexError, AttributeError):
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
    df = pd.read_csv(holidays_file)
    print(f"   Loaded {len(df):,} rows")
    
    # Check current strikes
    if 'strike' not in df.columns:
        print(f"   ❌ No 'strike' column found!")
        return False
    
    if 'ticker' not in df.columns:
        print(f"   ❌ No 'ticker' column found!")
        return False
    
    # Show sample of current strikes
    print(f"\n   Sample of current strikes (first 5 rows):")
    for idx, row in df.head(5).iterrows():
        print(f"      {row['ticker']} -> strike: {row['strike']}")
    
    # Extract correct strikes from tickers
    print(f"\n🔄 Extracting correct strikes from tickers...")
    df['strike_corrected'] = df['ticker'].apply(extract_strike_from_ticker)
    
    # Check how many were successfully parsed
    parsed_count = df['strike_corrected'].notna().sum()
    print(f"   Successfully parsed: {parsed_count:,} / {len(df):,} ({100*parsed_count/len(df):.1f}%)")
    
    if parsed_count < len(df):
        failed = df[df['strike_corrected'].isna()]
        print(f"   ⚠️  Failed to parse {len(failed)} tickers")
        if len(failed) <= 10:
            for idx, row in failed.iterrows():
                print(f"      - {row['ticker']}")
        else:
            for idx, row in failed.head(10).iterrows():
                print(f"      - {row['ticker']}")
            print(f"      ... and {len(failed) - 10} more")
    
    # Compare old vs new strikes
    df_comparison = df[df['strike_corrected'].notna()].copy()
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
            print(f"      {row['ticker']}")
            print(f"         Old: {row['strike']:.2f} -> New: {row['strike_corrected']:.2f} (diff: {row['strike_diff']:.2f})")
    
    # Update strikes
    print(f"\n💾 Updating strikes...")
    df['strike'] = df['strike_corrected']
    df = df.drop(columns=['strike_corrected'], errors='ignore')
    
    # Save updated file
    print(f"💾 Saving updated file...")
    df.to_csv(holidays_file, index=False)
    print(f"   ✅ Saved {holidays_file}")
    
    return True


def main():
    """Main function to fix all MSFT holidays files."""
    base_path = Path(__file__).parent / "python-boilerplate" / "data" / "MSFT"
    holidays_dir = base_path / "holidays"
    
    if not holidays_dir.exists():
        print(f"❌ Holidays directory not found: {holidays_dir}")
        return
    
    # Get all holidays CSV files
    holidays_files = sorted(holidays_dir.glob("*_options_pessimistic.csv"))
    
    if not holidays_files:
        print(f"❌ No holidays CSV files found in {holidays_dir}")
        return
    
    print(f"Found {len(holidays_files)} holidays files to process")
    
    # Process each file
    success_count = 0
    for holidays_file in holidays_files:
        if fix_holidays_file(holidays_file):
            success_count += 1
    
    print(f"\n{'='*80}")
    print(f"✅ Processing complete!")
    print(f"   Successfully updated: {success_count} / {len(holidays_files)} files")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

