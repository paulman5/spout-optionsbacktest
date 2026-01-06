#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path


def fetch_aapl_years_from_s3():
    """Fetch AAPL data for years 2021-2025 directly from S3"""
    
    years = [2021, 2022, 2023, 2024, 2025]
    
    print("🚀 Fetching AAPL data from S3 for years 2021-2025")
    print("=" * 60)
    
    for year in years:
        print(f"\n📅 Fetching year {year}...")
        
        # Update aggregate.py for this year
        aggregate_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py")
        with open(aggregate_file, 'r') as f:
            content = f.read()
        
        # Update TEST_YEAR
        current_year = content.split('TEST_YEAR = ')[1].split('\n')[0]
        content = content.replace(f"TEST_YEAR = {current_year}", f"TEST_YEAR = {year}")
        
        with open(aggregate_file, 'w') as f:
            f.write(content)
        
        print(f"   🔄 Updated TEST_YEAR to {year}")
        
        # Run aggregation script
        print(f"   📦 Running aggregation for {year}...")
        try:
            result = subprocess.run([
                sys.executable, "aggregate.py"
            ], cwd="/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data",
                capture_output=True, text=True, timeout=1800)  # 30 minute timeout
            
            if result.returncode == 0:
                print(f"   ✅ Successfully fetched {year} data")
                # Check if files were created
                monthly_file = f"options_day_aggs_{year}_monthly.csv"
                weekly_file = f"options_day_aggs_{year}_weekly.csv"
                
                if Path(monthly_file).exists():
                    print(f"   📄 Created {monthly_file}")
                if Path(weekly_file).exists():
                    print(f"   📄 Created {weekly_file}")
                    
            else:
                print(f"   ❌ Failed to fetch {year} data")
                print(f"   Error: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print(f"   ⏰ Timeout fetching {year} data")
        except Exception as e:
            print(f"   ❌ Error fetching {year} data: {e}")
    
    print(f"\n{'='*60}")
    print("✅ S3 fetch completed for all years 2021-2025")
    
    # Summary of downloaded files
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    print(f"\n📊 Summary of downloaded files:")
    
    for year in years:
        monthly_file = data_dir / f"options_day_aggs_{year}_monthly.csv"
        weekly_file = data_dir / f"options_day_aggs_{year}_weekly.csv"
        
        if monthly_file.exists():
            size_mb = monthly_file.stat().st_size / (1024 * 1024)
            print(f"   ✅ {year} Monthly: {monthly_file.name} ({size_mb:.1f} MB)")
        else:
            print(f"   ❌ {year} Monthly: NOT FOUND")
            
        if weekly_file.exists():
            size_mb = weekly_file.stat().st_size / (1024 * 1024)
            print(f"   ✅ {year} Weekly: {weekly_file.name} ({size_mb:.1f} MB)")
        else:
            print(f"   ❌ {year} Weekly: NOT FOUND")


if __name__ == "__main__":
    fetch_aapl_years_from_s3()
