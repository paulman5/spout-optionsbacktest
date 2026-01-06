#!/usr/bin/env python3
import pandas as pd
import os
import glob

def fix_premium_yields_v2():
    """Fix premium yield calculations using (low_price / underlying_spot) * 100"""
    
    # Find all CSV files
    csv_files = glob.glob('/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/*.csv')
    csv_files.extend(glob.glob('/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/weekly/*.csv'))
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    for file_path in csv_files:
        print(f"\n🔄 Fixing premium yields in {file_path}...")
        
        try:
            # Read the CSV
            df = pd.read_csv(file_path)
            
            # Check if required columns exist
            required_cols = ['low_price', 'underlying_spot', 'premium_yield_pct', 'premium_yield_pct_low']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"⚠️  Skipping - missing columns: {missing_cols}")
                continue
            
            # Store original values for comparison
            original_premium_yield = df['premium_yield_pct'].iloc[0] if len(df) > 0 else None
            original_premium_yield_low = df['premium_yield_pct_low'].iloc[0] if len(df) > 0 else None
            
            # Calculate new premium yields using (low_price / underlying_spot) * 100
            df['premium_yield_pct'] = (df['low_price'] / df['underlying_spot']) * 100
            df['premium_yield_pct_low'] = (df['low_price'] / df['underlying_spot']) * 100
            
            # Count rows with changes
            changed_rows = len(df)
            
            if changed_rows > 0:
                print(f"📊 Found {changed_rows} rows to update")
                
                # Show example corrections
                examples = []
                for i in range(min(3, len(df))):
                    if pd.notna(df.iloc[i]['low_price']) and pd.notna(df.iloc[i]['underlying_spot']):
                        examples.append(f"  {df.iloc[i]['ticker']}: {original_premium_yield:.2f}% → {df.iloc[i]['premium_yield_pct']:.2f}%")
                
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
    print("✅ Completed! Fixed premium yields in all files")

if __name__ == "__main__":
    fix_premium_yields_v2()
