#!/usr/bin/env python3
"""
Recalculate OTM percentages for ALL TSLA yearly options data after stock split.
OTM percentage = (strike - underlying_spot) / underlying_spot * 100
"""

import pandas as pd
import os

def recalculate_otm_for_year(year):
    """Recalculate OTM percentages for a specific year"""
    print(f"📊 Recalculating OTM percentages for TSLA {year} data...")
    
    input_file = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/{year}_options_pessimistic.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return 0
    
    df = pd.read_csv(input_file)
    print(f"   Loaded {year} data: {len(df):,} rows")
    
    # Check if OTM column exists
    if 'otm_pct' not in df.columns:
        print(f"❌ OTM column not found in {year} data")
        return 0
    
    # Check if underlying_spot column exists
    if 'underlying_spot' not in df.columns:
        print(f"❌ underlying_spot column not found in {year} data")
        return 0
    
    # Store original OTM values for comparison
    original_otm = df['otm_pct'].copy()
    
    # Recalculate OTM percentage
    # OTM = (strike - underlying_spot) / underlying_spot * 100
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
    
    # Update ITM status based on new OTM
    df['ITM'] = df['otm_pct'] <= 0
    df['ITM'] = df['ITM'].map({True: 'YES', False: 'NO'})
    
    # Show sample recalculations
    print(f"   Sample OTM recalculations:")
    for i in range(min(5, len(df))):
        orig_otm = original_otm.iloc[i]
        new_otm = df['otm_pct'].iloc[i]
        orig_itm = 'YES' if orig_otm <= 0 else 'NO'
        new_itm = df['ITM'].iloc[i]
        strike = df['strike'].iloc[i]
        underlying = df['underlying_spot'].iloc[i]
        
        print(f"     Strike: ${strike:.2f}, Underlying: ${underlying:.2f}")
        print(f"     OTM: {orig_otm:.2f}% → {new_otm:.2f}%")
        print(f"     ITM: {orig_itm} → {new_itm}")
        print()
    
    # Save updated data
    df.to_csv(input_file, index=False)
    
    print(f"   ✅ Recalculated OTM percentages for {len(df):,} contracts")
    return len(df)

def main():
    """Process all TSLA yearly files"""
    print("🚀 Recalculating OTM percentages for ALL TSLA yearly data...")
    print("📊 Formula: (strike - underlying_spot) / underlying_spot * 100")
    
    # Process all available years
    years = range(2016, 2026)  # 2016-2025
    total_updated = 0
    
    for year in years:
        updated_count = recalculate_otm_for_year(year)
        total_updated += updated_count
    
    print(f"\n🎉 OTM recalculation complete for ALL years!")
    print(f"📊 Summary:")
    print(f"   Years processed: {len(years)}")
    print(f"   Total contracts updated: {total_updated:,}")
    print(f"   OTM formula: (strike - underlying_spot) / underlying_spot * 100")
    
    return 0

if __name__ == "__main__":
    exit(main())
