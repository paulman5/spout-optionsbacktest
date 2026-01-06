#!/usr/bin/env python3
"""
Download real XLF data from S3 for 2016 and 2017
"""

import subprocess
import sys
import pandas as pd
from pathlib import Path

def download_real_xlf_data(year):
    """Download real XLF data for a specific year"""
    print(f"📅 Downloading real XLF data for {year}...")
    
    try:
        base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
        
        # Temporarily modify the aggregate script to use XLF and no entry day filtering
        script_path = base_dir / "src/backtesting/data/aggregate.py"
        
        # Read the current script
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Make modifications for XLF data download
        modified_content = script_content.replace("TICKERS_TO_FILTER = ['AMZN']", "TICKERS_TO_FILTER = ['XLF']")
        modified_content = modified_content.replace("TEST_YEAR = 2025", f"TEST_YEAR = {year}")
        modified_content = modified_content.replace("WEEKLY_ENTRY_DAYS_RANGE = 5", "WEEKLY_ENTRY_DAYS_RANGE = None")
        modified_content = modified_content.replace("MONTHLY_ENTRY_DAYS_RANGE = (28, 32)", "MONTHLY_ENTRY_DAYS_RANGE = None")
        
        # Write the modified script
        with open(script_path, 'w') as f:
            f.write(modified_content)
        
        try:
            # Run the aggregate script
            cmd = [sys.executable, "src/backtesting/data/aggregate.py"]
            
            result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
            
            print(f"   Script output: {result.stdout}")
            if result.stderr:
                print(f"   Script errors: {result.stderr}")
            
            # Check if files were created
            monthly_file = base_dir / f"options_day_aggs_{year}_monthly.csv"
            weekly_file = base_dir / f"options_day_aggs_{year}_weekly.csv"
            
            if monthly_file.exists():
                df_monthly = pd.read_csv(monthly_file)
                print(f"   ✅ Monthly file created: {len(df_monthly)} rows")
                print(f"   Sample monthly data: {df_monthly.head(2)['ticker'].tolist() if 'ticker' in df_monthly.columns else []}")
            else:
                print(f"   ❌ Monthly file not found")
            
            if weekly_file.exists():
                df_weekly = pd.read_csv(weekly_file)
                print(f"   ✅ Weekly file created: {len(df_weekly)} rows")
                ticker_list = df_weekly.head(2)['ticker'].tolist() if 'ticker' in df_weekly.columns else []
                print(f"   Sample weekly data: {ticker_list}")
            else:
                print(f"   ❌ Weekly file not found")
                
        finally:
            # Restore the original script
            with open(script_path, 'w') as f:
                f.write(script_content)
            
        return True
        
    except Exception as e:
        print(f"   ❌ Exception downloading XLF data for {year}: {e}")
        return False

def main():
    """Main function to download real XLF data"""
    print("🔄 Downloading real XLF data for 2016 and 2017...")
    
    years = [2016, 2017]
    success_count = 0
    
    for year in years:
        if download_real_xlf_data(year):
            success_count += 1
    
    print(f"\n✅ Download completed for {success_count}/{len(years)} years")

if __name__ == "__main__":
    main()
