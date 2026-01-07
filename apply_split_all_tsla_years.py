#!/usr/bin/env python3
"""
Apply 3:1 stock split to ALL TSLA yearly options data.
Divide strike prices by 3 for contracts ON or AFTER Aug 31, 2020 and BEFORE Aug 25, 2022.
Keep all other data unchanged and format strikes to 2 decimal places.
"""

import pandas as pd
import os
from datetime import datetime

def apply_split_to_year(year):
    """Apply 3:1 stock split to a specific year"""
    print(f"📊 Applying 3:1 stock split to TSLA {year} data...")
    
    input_file = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/{year}_options_pessimistic.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ File not found: {input_file}")
        return 0
    
    df = pd.read_csv(input_file)
    print(f"   Loaded {year} data: {len(df):,} rows")
    
    # Convert date column to datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Define date boundaries for stock split
    start_date = datetime(2020, 8, 31)  # ON or AFTER August 31, 2020
    end_date = datetime(2022, 8, 25)    # BEFORE August 25, 2022
    
    # Identify contracts that need strike adjustment
    mask = (df['date_only'] > start_date) & (df['date_only'] < end_date)
    contracts_to_adjust = df[mask].copy()
    
    print(f"   Contracts in split range: {len(contracts_to_adjust):,}")
    
    if len(contracts_to_adjust) > 0:
        # Store original strikes for comparison
        original_strikes = contracts_to_adjust['strike'].copy()
        
        # Apply 3:1 split (divide strike by 3) and round to 2 decimals
        contracts_to_adjust['strike'] = (contracts_to_adjust['strike'] / 3).round(2)
        
        # Update ticker symbols to reflect new strike prices
        def update_ticker(ticker, new_strike):
            """Update option ticker with new strike price"""
            if not ticker.startswith('O:TSLA'):
                return ticker
            
            # Extract date, type from ticker
            symbol_part = 'O:TSLA'
            date_part = ticker[7:13]  # YYMMDD
            type_part = ticker[13]     # C or P
            old_strike_part = ticker[14:]  # XXXXXXXX (strike * 100000)
            
            # Calculate new strike part (strike * 100000)
            new_strike_int = int(round(new_strike * 100000))
            new_strike_part = f"{new_strike_int:08d}"  # Pad to 8 digits
            
            new_ticker = f"{symbol_part}{date_part}{type_part}{new_strike_part}"
            return new_ticker
        
        contracts_to_adjust['ticker'] = contracts_to_adjust.apply(
            lambda row: update_ticker(row['ticker'], row['strike']), axis=1
        )
        
        # Show sample adjustments
        print(f"   Sample adjustments:")
        for i in range(min(3, len(contracts_to_adjust))):
            orig_strike = original_strikes.iloc[i]
            new_strike = contracts_to_adjust['strike'].iloc[i]
            old_ticker = df.loc[mask].iloc[i]['ticker']
            new_ticker = contracts_to_adjust['ticker'].iloc[i]
            print(f"     {old_ticker} → {new_ticker}")
            print(f"     Strike: ${orig_strike:.2f} → ${new_strike:.2f}")
        
        # Update original dataframe
        df.loc[mask, 'strike'] = contracts_to_adjust['strike']
        df.loc[mask, 'ticker'] = contracts_to_adjust['ticker']
        
        # Save back to original file
        df.to_csv(input_file, index=False)
        
        print(f"   ✅ Applied 3:1 split to {len(contracts_to_adjust):,} contracts")
        return len(contracts_to_adjust)
    else:
        print(f"   ⚠️  No contracts need adjustment for {year}")
        return 0

def main():
    """Process all TSLA yearly files"""
    print("🚀 Applying 3:1 TSLA stock split to ALL yearly data...")
    print("📅 Date range: ON or AFTER August 31, 2020 AND BEFORE August 25, 2022")
    print("⚠️  This will modify existing TSLA files directly!")
    
    # Process all available years
    years = range(2016, 2026)  # 2016-2025
    total_adjusted = 0
    
    for year in years:
        adjusted_count = apply_split_to_year(year)
        total_adjusted += adjusted_count
    
    print(f"\n🎉 Stock split adjustment complete for ALL years!")
    print(f"📊 Summary:")
    print(f"   Years processed: {len(years)}")
    print(f"   Total contracts adjusted: {total_adjusted:,}")
    print(f"   Split ratio: 3:1 (divide by 3)")
    print(f"   Strike precision: 2 decimal places")
    
    return 0

if __name__ == "__main__":
    exit(main())
