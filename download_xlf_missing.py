#!/usr/bin/env python3
"""
Download missing XLF data for 2016 and 2017 using aggregate script
"""

import subprocess
import sys
from pathlib import Path

def download_xlf_data(year):
    """Download XLF data for a specific year"""
    print(f"📅 Downloading XLF data for {year}...")
    
    try:
        # Change to the correct directory and run the aggregate script
        base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
        
        # Temporarily modify the aggregate script to use XLF
        script_path = base_dir / "src/backtesting/data/aggregate.py"
        
        # Read the current script
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Replace AMZN with XLF
        modified_content = script_content.replace("TICKERS_TO_FILTER = ['AMZN']", "TICKERS_TO_FILTER = ['XLF']")
        
        # Write the modified script
        with open(script_path, 'w') as f:
            f.write(modified_content)
        
        try:
            # Run the aggregate script for XLF monthly
            cmd = [
                sys.executable, 
                "src/backtesting/data/aggregate.py",
                "--year", str(year),
                "--frequency", "monthly"
            ]
            
            result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Successfully downloaded XLF monthly data for {year}")
            else:
                print(f"   ❌ Error downloading XLF monthly data for {year}: {result.stderr}")
                return False
            
            # Download weekly data as well
            cmd_weekly = [
                sys.executable, 
                "src/backtesting/data/aggregate.py",
                "--year", str(year),
                "--frequency", "weekly"
            ]
            
            result_weekly = subprocess.run(cmd_weekly, cwd=base_dir, capture_output=True, text=True)
            
            if result_weekly.returncode == 0:
                print(f"   ✅ Successfully downloaded XLF weekly data for {year}")
            else:
                print(f"   ❌ Error downloading XLF weekly data for {year}: {result_weekly.stderr}")
                return False
                
        finally:
            # Restore the original script
            with open(script_path, 'w') as f:
                f.write(script_content)
            
        return True
        
    except Exception as e:
        print(f"   ❌ Exception downloading XLF data for {year}: {e}")
        return False

def main():
    """Main function to download missing XLF data"""
    print("🔄 Downloading missing XLF data for 2016 and 2017...")
    
    years = [2016, 2017]
    success_count = 0
    
    for year in years:
        if download_xlf_data(year):
            success_count += 1
    
    print(f"\n✅ Download completed for {success_count}/{len(years)} years")
    
    if success_count == len(years):
        print("📊 All missing XLF data has been downloaded successfully!")
    else:
        print("⚠️  Some downloads failed. Please check the error messages above.")

if __name__ == "__main__":
    main()
