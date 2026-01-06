#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse
import subprocess
import sys


def fetch_aapl_all_years():
    """Fetch AAPL options data for all years 2016-2025"""
    
    years = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    
    print("🚀 Starting AAPL data fetch for years 2016-2025")
    print("=" * 60)
    
    for year in years:
        print(f"\n📅 Processing year {year}...")
        
        # Step 1: Update aggregation script for this year and ticker
        print(f"   🔄 Updating aggregation script for {year}...")
        
        # Read the current aggregate.py
        aggregate_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py")
        with open(aggregate_file, 'r') as f:
            content = f.read()
        
        # Update TEST_YEAR and ensure TICKERS_TO_FILTER is AAPL
        current_year = content.split('TEST_YEAR = ')[1].split('\n')[0]
        content = content.replace(f"TEST_YEAR = {current_year}", f"TEST_YEAR = {year}")
        content = content.replace("TICKERS_TO_FILTER = ['TSLA']", "TICKERS_TO_FILTER = ['AAPL']")
        content = content.replace("TICKERS_TO_FILTER = ['AAPL']", "TICKERS_TO_FILTER = ['AAPL']")  # Ensure it stays AAPL
        
        with open(aggregate_file, 'w') as f:
            f.write(content)
        
        # Step 2: Run aggregation script
        print(f"   📦 Running aggregation for {year}...")
        try:
            result = subprocess.run([
                sys.executable, "aggregate.py"
            ], cwd="/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data",
            capture_output=True, text=True, timeout=600)  # 10 minute timeout
            
            if result.returncode != 0:
                print(f"   ❌ Aggregation failed for {year}: {result.stderr}")
                continue
                
            print(f"   ✅ Aggregation completed for {year}")
            
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Aggregation timed out for {year}")
            continue
        except Exception as e:
            print(f"   ❌ Error running aggregation for {year}: {e}")
            continue
        
        # Step 3: Process monthly data
        print(f"   📊 Processing monthly data for {year}...")
        try:
            monthly_input = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_{year}_monthly.csv"
            monthly_output = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly/{year}_options_pessimistic.csv"
            
            result = subprocess.run([
                sys.executable, "/Users/paulvanmierlo/spout-optionsbacktest/process_aapl_2017.py",
                "--input-file", monthly_input,
                "--output-file", monthly_output
            ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
            
            if result.returncode == 0:
                print(f"   ✅ Monthly data processed for {year}")
            else:
                print(f"   ❌ Monthly processing failed for {year}: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Monthly processing timed out for {year}")
        except Exception as e:
            print(f"   ❌ Error processing monthly data for {year}: {e}")
        
        # Step 4: Process weekly data
        print(f"   📊 Processing weekly data for {year}...")
        try:
            weekly_input = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/options_day_aggs_{year}_weekly.csv"
            weekly_output = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly/{year}_options_pessimistic.csv"
            
            result = subprocess.run([
                sys.executable, "/Users/paulvanmierlo/spout-optionsbacktest/process_aapl_2017.py",
                "--input-file", weekly_input,
                "--output-file", weekly_output
            ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
            
            if result.returncode == 0:
                print(f"   ✅ Weekly data processed for {year}")
            else:
                print(f"   ❌ Weekly processing failed for {year}: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Weekly processing timed out for {year}")
        except Exception as e:
            print(f"   ❌ Error processing weekly data for {year}: {e}")
        
        print(f"   🎉 Year {year} completed!")
    
    print(f"\n{'='*60}")
    print("✅ AAPL data fetch completed for all years 2016-2025")
    
    # Summary of created files
    print(f"\n📊 Summary of created files:")
    aapl_monthly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/monthly")
    aapl_weekly_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AAPL/weekly")
    
    if aapl_monthly_dir.exists():
        monthly_files = list(aapl_monthly_dir.glob("*.csv"))
        print(f"   Monthly files: {len(monthly_files)} created")
        for f in sorted(monthly_files):
            print(f"     - {f.name}")
    
    if aapl_weekly_dir.exists():
        weekly_files = list(aapl_weekly_dir.glob("*.csv"))
        print(f"   Weekly files: {len(weekly_files)} created")
        for f in sorted(weekly_files):
            print(f"     - {f.name}")


if __name__ == "__main__":
    fetch_aapl_all_years()
