#!/usr/bin/env python3
"""
Run aggregate script to fetch XLF data for 2016
"""

import subprocess
import sys
import pandas as pd
from pathlib import Path

def run_aggregate_for_xlf_2016():
    """Run aggregate script specifically for XLF 2016"""
    print("🔄 Running aggregate script for XLF 2016...")
    
    try:
        base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
        
        # Temporarily modify aggregate script
        script_path = base_dir / "src/backtesting/data/aggregate.py"
        
        # Read current script
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Make modifications for XLF 2016
        modified_content = script_content.replace("TICKERS_TO_FILTER = ['AMZN']", "TICKERS_TO_FILTER = ['XLF']")
        modified_content = modified_content.replace("TEST_YEAR = 2025", "TEST_YEAR = 2016")
        modified_content = modified_content.replace("WEEKLY_ENTRY_DAYS_RANGE = 5", "WEEKLY_ENTRY_DAYS_RANGE = None")
        modified_content = modified_content.replace("MONTHLY_ENTRY_DAYS_RANGE = (28, 32)", "MONTHLY_ENTRY_DAYS_RANGE = None")
        
        # Write the modified script
        with open(script_path, 'w') as f:
            f.write(modified_content)
        
        try:
            # Run aggregate script
            cmd = [sys.executable, "src/backtesting/data/aggregate.py"]
            
            print("   Running aggregate script...")
            result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
            
            print("   Script output:")
            lines = result.stdout.split('\n')
            for line in lines:
                if 'XLF' in line or 'rows' in line or 'Found' in line or 'ERROR' in line:
                    print(f"     {line}")
            
            if result.stderr:
                print(f"   Script errors: {result.stderr}")
            
            # Check if files were created
            monthly_file = base_dir / f"options_day_aggs_2016_monthly.csv"
            weekly_file = base_dir / f"options_day_aggs_2016_weekly.csv"
            
            success = False
            if monthly_file.exists():
                df_monthly = pd.read_csv(monthly_file)
                print(f"   ✅ Monthly file created: {len(df_monthly)} rows")
                
                # Check for XLF specifically
                if 'ticker' in df_monthly.columns:
                    xlf_count = df_monthly[df_monthly['ticker'].str.contains('O:XLF', na=False)].shape[0]
                    print(f"   XLF rows in monthly: {xlf_count}")
                    
                    if xlf_count > 0:
                        xlf_sample = df_monthly[df_monthly['ticker'].str.contains('O:XLF', na=False)].head(3)
                        print(f"   Sample XLF tickers: {xlf_sample['ticker'].tolist()}")
                        success = True
                    else:
                        print(f"   ❌ No XLF data found in monthly file")
                        
                    # Check all unique symbols
                    symbols = df_monthly['ticker'].str.extract(r'O:([A-Z]{1,5})')[0].dropna().unique()
                    print(f"   All symbols found: {sorted(symbols)}")
            else:
                print(f"   ❌ Monthly file not found")
                
            if weekly_file.exists():
                df_weekly = pd.read_csv(weekly_file)
                print(f"   ✅ Weekly file created: {len(df_weekly)} rows")
                
                # Check for XLF specifically
                if 'ticker' in df_weekly.columns:
                    xlf_count = df_weekly[df_weekly['ticker'].str.contains('O:XLF', na=False)].shape[0]
                    print(f"   XLF rows in weekly: {xlf_count}")
            else:
                print(f"   ❌ Weekly file not found")
                
        finally:
            # Restore the original script
            with open(script_path, 'w') as f:
                f.write(script_content)
            
        return success
        
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return False

def main():
    """Main function"""
    print("🔄 Fetching XLF data for 2016 using aggregate script...")
    
    if run_aggregate_for_xlf_2016():
        print("\n✅ XLF data for 2016 fetched successfully!")
    else:
        print("\n❌ Failed to fetch XLF data for 2016")

if __name__ == "__main__":
    main()
