#!/usr/bin/env python3
"""
Download AMZN data for each year from 2022 to 2025.
"""

import os
import sys
import subprocess
from pathlib import Path

def run_aggregate_for_year(year):
    """Run aggregate script for a specific year."""
    print(f"\n🚀 Downloading AMZN data for {year}...", flush=True)
    
    # Change to the correct directory
    script_dir = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data"
    
    # Run aggregate script for the year
    result = subprocess.run([
        "python3", "aggregate.py"
    ], cwd=script_dir, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ Successfully downloaded AMZN data for {year}")
        return True
    else:
        print(f"❌ Failed to download AMZN data for {year}")
        print(f"Error: {result.stderr}")
        return False

def main():
    """Download AMZN data from 2022 to 2025."""
    years = [2022, 2023, 2024, 2025]
    
    print("🎯 Downloading AMZN data from 2022 to 2025...", flush=True)
    
    success_count = 0
    for year in years:
        if run_aggregate_for_year(year):
            success_count += 1
    
    print(f"\n🎉 === DOWNLOAD SUMMARY ===")
    print(f"📊 Years processed: {len(years)}")
    print(f"✅ Successfully downloaded: {success_count}")
    print(f"❌ Failed: {len(years) - success_count}")
    
    if success_count == len(years):
        print("✅ All AMZN data downloaded successfully!")
    else:
        print("⚠️ Some downloads failed. Check the errors above.")

if __name__ == "__main__":
    main()
