#!/usr/bin/env python3
import pandas as pd
import os
import glob

def fix_2017_strikes_correct():
    """Fix strike prices in 2017 CSV files with correct stock split adjustments"""
    
    # 2017 files
    csv_files = [
        '/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/2017_options_pessimistic.csv',
        '/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/weekly/2017_options_pessimistic.csv'
    ]
    
    print(f"Found {len(csv_files)} 2017 CSV files to process")
    
    for file_path in csv_files:
        print(f"\n🔄 Fixing strikes in {file_path}...")
        
        try:
            # Read the CSV
            df = pd.read_csv(file_path)
            
            # Store original values for comparison
            original_strike = df['strike'].iloc[0] if len(df) > 0 else None
            
            # Apply CORRECT stock split adjustments to strikes based on date
            def adjust_strike_correct(row):
                date = pd.to_datetime(row['date_only'])
                strike = row['strike']
                
                # First, get the original strike from ticker (reverse the incorrect division)
                # The current strike is already divided by 1000, so we need to multiply back
                original_from_ticker = strike * 1000.0
                
                # Apply stock split adjustments based on date
                if date < pd.to_datetime('2020-08-31'):
                    # Pre-2020-08-31: divide by 15
                    adjusted_strike = original_from_ticker / 1000.0 / 15.0
                elif date < pd.to_datetime('2022-08-25'):
                    # 2020-08-31 to 2022-08-25: divide by 3
                    adjusted_strike = original_from_ticker / 1000.0 / 3.0
                else:
                    # Post-2022-08-25: divide by 1
                    adjusted_strike = original_from_ticker / 1000.0 / 1.0
                
                return round(adjusted_strike, 2)
            
            # Apply the CORRECT adjustment
            df['strike'] = df.apply(adjust_strike_correct, axis=1)
            
            # Recalculate OTM percentages with corrected strikes
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
            
            # Recalculate ITM status
            df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
            
            # Count rows with changes
            changed_rows = len(df)
            
            if changed_rows > 0:
                print(f"📊 Found {changed_rows} rows to update")
                
                # Show example corrections
                examples = []
                for i in range(min(3, len(df))):
                    examples.append(f"  {df.iloc[i]['ticker']}: {original_strike:.2f} → {df.iloc[i]['strike']:.2f}")
                
                if examples:
                    print("📝 Example corrections:")
                    for example in examples:
                        print(example)
                
                # Save the updated CSV
                df.to_csv(file_path, index=False)
                print(f"✅ Fixed and saved {file_path}")
            else:
                print(f"ℹ️  No changes needed")
                
        except Exception as e:
            print(f"❌ Error processing {file_path}: {e}")
    
    print(f"\n{'='*50}")
    print("✅ Completed! Fixed 2017 strike prices with correct adjustments")

if __name__ == "__main__":
    fix_2017_strikes_correct()
