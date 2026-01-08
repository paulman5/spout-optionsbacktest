#!/usr/bin/env python3
"""
Show specific examples of negative OTM but ITM=NO from a specific file.
"""

import pandas as pd
import sys

def show_examples(file_path, max_examples=20):
    """Show examples from a specific file."""
    df = pd.read_csv(file_path)
    
    # Filter to calls only
    calls_df = df[df['option_type'] == 'C'].copy()
    
    # Find negative OTM but ITM=NO
    mismatches = calls_df[
        (calls_df['otm_pct'] < 0) & (calls_df['ITM'] == 'NO')
    ].copy()
    
    print(f"File: {file_path}")
    print(f"Total rows: {len(df)}")
    print(f"Call options: {len(calls_df)}")
    print(f"Negative OTM but ITM=NO: {len(mismatches)}")
    print(f"\n{'='*100}")
    
    if len(mismatches) > 0:
        print(f"EXAMPLES (showing first {min(max_examples, len(mismatches))}):")
        print(f"{'='*100}\n")
        
        for i, (_, row) in enumerate(mismatches.head(max_examples).iterrows(), 1):
            print(f"{i}. {row['ticker']}")
            print(f"   Date: {row['date_only']} → Expiration: {row['expiration_date']}")
            print(f"   Strike: ${row['strike']:.2f}")
            print(f"   Underlying (trading date): ${row['underlying_spot']:.2f}")
            expiry_spot = row.get('underlying_spot_at_expiry', 'N/A')
            if expiry_spot != 'N/A':
                print(f"   Underlying (expiration): ${expiry_spot:.2f}")
                print(f"   Price change: ${expiry_spot - row['underlying_spot']:.2f} ({((expiry_spot / row['underlying_spot']) - 1) * 100:.2f}%)")
            else:
                print(f"   Underlying (expiration): {expiry_spot}")
            print(f"   OTM% (based on trading date): {row['otm_pct']:.2f}%")
            print(f"   ITM (based on expiration): {row['ITM']}")
            print()
    else:
        print("✓ No mismatches found in this file!")
        print("  All negative OTM values correctly have ITM=YES")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "python-boilerplate/data/AAPL/weekly/2022_options_pessimistic.csv"
    
    show_examples(file_path)

