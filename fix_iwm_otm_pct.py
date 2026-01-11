"""
Fix IWM otm_pct (out-of-the-money percentage) in all holidays CSV files.
Formula: otm_pct = ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
import numpy as np
from pathlib import Path


def fix_holidays_file(holidays_file: Path):
    """
    Fix otm_pct in a single holidays CSV file.
    
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
    
    # Check required columns
    required_cols = ['strike', 'underlying_spot', 'otm_pct']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"   ❌ Missing required columns: {missing_cols}")
        return False
    
    # Check how many rows have underlying_spot
    has_spot = df['underlying_spot'].notna().sum()
    print(f"   Rows with underlying_spot: {has_spot:,} / {len(df):,} ({100*has_spot/len(df):.1f}%)")
    
    if has_spot == 0:
        print(f"   ⚠️  No underlying_spot values found, skipping...")
        return False
    
    # Show sample of current values
    print(f"\n   Sample of current values (first 5 rows with spot):")
    sample = df[df['underlying_spot'].notna()].head(5)
    for idx, row in sample.iterrows():
        print(f"      Strike: {row['strike']:.2f}, Spot: {row['underlying_spot']:.2f}, Current OTM%: {row['otm_pct']}")
    
    # Calculate new otm_pct
    print(f"\n🔄 Calculating new otm_pct...")
    # Formula: ((strike - underlying_spot) / underlying_spot) * 100
    df['otm_pct_new'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
    
    # Round to 2 decimal places
    df['otm_pct_new'] = df['otm_pct_new'].round(2)
    
    # Only update where we have underlying_spot
    mask = df['underlying_spot'].notna()
    df.loc[mask, 'otm_pct'] = df.loc[mask, 'otm_pct_new']
    
    # Drop temporary column
    df = df.drop(columns=['otm_pct_new'], errors='ignore')
    
    # Show sample of new values
    print(f"\n   Sample of new values (first 5 rows with spot):")
    sample = df[df['underlying_spot'].notna()].head(5)
    for idx, row in sample.iterrows():
        print(f"      Strike: {row['strike']:.2f}, Spot: {row['underlying_spot']:.2f}, New OTM%: {row['otm_pct']:.2f}")
    
    # Statistics
    updated_rows = df[df['underlying_spot'].notna() & df['otm_pct'].notna()]
    if len(updated_rows) > 0:
        print(f"\n   Statistics:")
        print(f"      Rows updated: {len(updated_rows):,}")
        print(f"      Average OTM%: {updated_rows['otm_pct'].mean():.2f}")
        print(f"      Min OTM%: {updated_rows['otm_pct'].min():.2f}")
        print(f"      Max OTM%: {updated_rows['otm_pct'].max():.2f}")
        
        # Count ITM vs OTM
        itm_count = (updated_rows['otm_pct'] < 0).sum()
        otm_count = (updated_rows['otm_pct'] >= 0).sum()
        print(f"      ITM (OTM% < 0): {itm_count:,}")
        print(f"      OTM (OTM% >= 0): {otm_count:,}")
    
    # Save updated file
    print(f"\n💾 Saving updated file...")
    df.to_csv(holidays_file, index=False)
    print(f"   ✅ Saved {holidays_file}")
    
    return True


def main():
    """Main function to fix all IWM holidays files."""
    base_path = Path(__file__).parent / "python-boilerplate" / "data" / "IWM"
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

