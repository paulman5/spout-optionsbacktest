#!/usr/bin/env python3
"""
Check for mismatches between negative OTM and ITM=YES.
Since ITM is now based on expiration prices but OTM is based on trading date prices,
they might not always match.
"""

import pandas as pd
import glob
from pathlib import Path

def check_mismatches(df, file_path):
    """Check for mismatches between OTM and ITM."""
    mismatches = []
    
    # Filter to calls only (puts have different logic)
    calls_df = df[df['option_type'] == 'C'].copy()
    
    if len(calls_df) == 0:
        return mismatches
    
    # Check for negative OTM but ITM=NO
    negative_otm_no_itm = calls_df[
        (calls_df['otm_pct'] < 0) & (calls_df['ITM'] == 'NO')
    ].copy()
    
    # Check for positive OTM but ITM=YES (might happen if stock moved up by expiration)
    positive_otm_yes_itm = calls_df[
        (calls_df['otm_pct'] >= 0) & (calls_df['ITM'] == 'YES')
    ].copy()
    
    if len(negative_otm_no_itm) > 0:
        for _, row in negative_otm_no_itm.iterrows():
            mismatches.append({
                'file': file_path.name,
                'ticker': row['ticker'],
                'date': row['date_only'],
                'expiration': row['expiration_date'],
                'strike': row['strike'],
                'underlying_spot': row['underlying_spot'],
                'underlying_spot_at_expiry': row.get('underlying_spot_at_expiry', 'N/A'),
                'otm_pct': row['otm_pct'],
                'ITM': row['ITM'],
                'issue': 'Negative OTM but ITM=NO'
            })
    
    if len(positive_otm_yes_itm) > 0:
        for _, row in positive_otm_yes_itm.iterrows():
            mismatches.append({
                'file': file_path.name,
                'ticker': row['ticker'],
                'date': row['date_only'],
                'expiration': row['expiration_date'],
                'strike': row['strike'],
                'underlying_spot': row['underlying_spot'],
                'underlying_spot_at_expiry': row.get('underlying_spot_at_expiry', 'N/A'),
                'otm_pct': row['otm_pct'],
                'ITM': row['ITM'],
                'issue': 'Positive OTM but ITM=YES (stock moved up by expiration)'
            })
    
    return mismatches

def main():
    """Main function to check all CSV files."""
    base_dir = Path(__file__).parent / "python-boilerplate" / "data"
    
    # Find all *_options_pessimistic.csv files
    pattern = str(base_dir / "**" / "*_options_pessimistic.csv")
    csv_files = glob.glob(pattern, recursive=True)
    
    print(f"Checking {len(csv_files)} CSV files for OTM/ITM mismatches...\n")
    
    all_mismatches = []
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file)
            
            if 'ITM' not in df.columns or 'otm_pct' not in df.columns:
                continue
            
            mismatches = check_mismatches(df, Path(csv_file))
            if mismatches:
                all_mismatches.extend(mismatches)
                print(f"⚠️  {Path(csv_file).name}: {len(mismatches)} mismatches found")
        
        except Exception as e:
            print(f"✗ Error processing {csv_file}: {e}")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total mismatches found: {len(all_mismatches)}")
    
    if len(all_mismatches) > 0:
        # Group by issue type
        negative_otm_no_itm = [m for m in all_mismatches if m['issue'] == 'Negative OTM but ITM=NO']
        positive_otm_yes_itm = [m for m in all_mismatches if m['issue'] == 'Positive OTM but ITM=YES (stock moved up by expiration)']
        
        print(f"\nNegative OTM but ITM=NO: {len(negative_otm_no_itm)} cases")
        print(f"Positive OTM but ITM=YES: {len(positive_otm_yes_itm)} cases")
        
        # Show first 20 examples of negative OTM but ITM=NO (this is the main concern)
        if negative_otm_no_itm:
            print(f"\n{'='*80}")
            print(f"EXAMPLES: Negative OTM but ITM=NO (first 20)")
            print(f"{'='*80}")
            for i, m in enumerate(negative_otm_no_itm[:20], 1):
                print(f"\n{i}. {m['file']}")
                print(f"   Ticker: {m['ticker']}")
                print(f"   Date: {m['date']} → Expiration: {m['expiration']}")
                print(f"   Strike: ${m['strike']:.2f}")
                print(f"   Underlying (trading date): ${m['underlying_spot']:.2f}")
                print(f"   Underlying (expiration): ${m['underlying_spot_at_expiry']:.2f}" if m['underlying_spot_at_expiry'] != 'N/A' else f"   Underlying (expiration): {m['underlying_spot_at_expiry']}")
                print(f"   OTM%: {m['otm_pct']:.2f}%")
                print(f"   ITM: {m['ITM']}")
        
        # Show first 10 examples of positive OTM but ITM=YES (expected behavior)
        if positive_otm_yes_itm:
            print(f"\n{'='*80}")
            print(f"EXAMPLES: Positive OTM but ITM=YES (stock moved up by expiration)")
            print(f"{'='*80}")
            for i, m in enumerate(positive_otm_yes_itm[:10], 1):
                print(f"\n{i}. {m['file']}")
                print(f"   Ticker: {m['ticker']}")
                print(f"   Date: {m['date']} → Expiration: {m['expiration']}")
                print(f"   Strike: ${m['strike']:.2f}")
                print(f"   Underlying (trading date): ${m['underlying_spot']:.2f}")
                print(f"   Underlying (expiration): ${m['underlying_spot_at_expiry']:.2f}" if m['underlying_spot_at_expiry'] != 'N/A' else f"   Underlying (expiration): {m['underlying_spot_at_expiry']}")
                print(f"   OTM%: {m['otm_pct']:.2f}%")
                print(f"   ITM: {m['ITM']}")
    else:
        print("\n✓ No mismatches found! All negative OTM values have ITM=YES")

if __name__ == "__main__":
    main()

