#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
import os


def download_year_from_s3(year):
    """Download all options data for a specific year from S3"""
    
    print(f"📅 Downloading {year} data from S3...")
    
    # Create directory for the year
    year_dir = Path(f"/Users/paulvanmierlo/spout-optionsbacktest/downloaded_data/{year}")
    year_dir.mkdir(parents=True, exist_ok=True)
    
    # Download all files for the year
    s3_path = f"s3://flatfiles/us_options_opra/day_aggs_v1/{year}/"
    
    try:
        # Use AWS CLI to sync the entire year directory
        result = subprocess.run([
            "aws", "s3", "sync", s3_path, str(year_dir),
            "--endpoint-url", "https://files.massive.com",
            "--exclude", "*", "--include", "*.csv.gz"
        ], capture_output=True, text=True, timeout=3600)  # 1 hour timeout
        
        if result.returncode == 0:
            print(f"   ✅ Successfully downloaded {year} data")
            
            # Count downloaded files
            downloaded_files = list(year_dir.glob("**/*.csv.gz"))
            print(f"   📄 Downloaded {len(downloaded_files):,} files")
            
            # Calculate total size
            total_size = sum(f.stat().st_size for f in downloaded_files)
            total_mb = total_size / (1024 * 1024)
            print(f"   💾 Total size: {total_mb:.1f} MB")
            
            return True
        else:
            print(f"   ❌ Failed to download {year} data")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ Timeout downloading {year} data")
        return False
    except Exception as e:
        print(f"   ❌ Error downloading {year} data: {e}")
        return False


def process_downloaded_data(year):
    """Process downloaded data using the aggregate.py script"""
    
    print(f"   🔄 Processing {year} data...")
    
    # Update aggregate.py for this year
    aggregate_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py")
    with open(aggregate_file, 'r') as f:
        content = f.read()
    
    # Update TEST_YEAR and set USE_LOCAL_FILES to True
    current_year = content.split('TEST_YEAR = ')[1].split('\n')[0]
    content = content.replace(f"TEST_YEAR = {current_year}", f"TEST_YEAR = {year}")
    content = content.replace("USE_LOCAL_FILES = False", "USE_LOCAL_FILES = True")
    content = content.replace(f'LOCAL_FILES_PATH = "./downloaded_data"', f'LOCAL_FILES_PATH = "./downloaded_data/{year}"')
    
    with open(aggregate_file, 'w') as f:
        f.write(content)
    
    print(f"   📝 Updated aggregate.py for {year}")
    
    # Run aggregation script
    try:
        result = subprocess.run([
            sys.executable, "aggregate.py"
        ], cwd="/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data",
            capture_output=True, text=True, timeout=1800)  # 30 minute timeout
        
        if result.returncode == 0:
            print(f"   ✅ Successfully processed {year} data")
            
            # Check if files were created
            monthly_file = f"options_day_aggs_{year}_monthly.csv"
            weekly_file = f"options_day_aggs_{year}_weekly.csv"
            
            if Path(monthly_file).exists():
                size_mb = Path(monthly_file).stat().st_size / (1024 * 1024)
                print(f"   📄 Created {monthly_file} ({size_mb:.1f} MB)")
            if Path(weekly_file).exists():
                size_mb = Path(weekly_file).stat().st_size / (1024 * 1024)
                print(f"   📄 Created {weekly_file} ({size_mb:.1f} MB)")
                
            return True
        else:
            print(f"   ❌ Failed to process {year} data")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ Timeout processing {year} data")
        return False
    except Exception as e:
        print(f"   ❌ Error processing {year} data: {e}")
        return False


def fetch_all_missing_years():
    """Fetch all missing years (2021-2025) from S3"""
    
    years = [2021, 2022, 2023, 2024, 2025]
    
    print("🚀 Fetching AAPL data for years 2021-2025 from S3")
    print("=" * 60)
    
    successful_years = []
    
    for year in years:
        print(f"\n{'='*20} YEAR {year} {'='*20}")
        
        # Step 1: Download from S3
        if download_year_from_s3(year):
            # Step 2: Process the data
            if process_downloaded_data(year):
                successful_years.append(year)
                print(f"   🎉 Year {year} completed successfully!")
            else:
                print(f"   ❌ Failed to process {year}")
        else:
            print(f"   ❌ Failed to download {year}")
    
    print(f"\n{'='*60}")
    print("✅ S3 fetch completed!")
    
    # Summary
    print(f"\n📊 Summary:")
    print(f"   Successfully processed: {successful_years}")
    print(f"   Failed years: {[y for y in years if y not in successful_years]}")
    
    # Reset aggregate.py to original settings
    print(f"\n🔄 Resetting aggregate.py to original settings...")
    aggregate_file = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py")
    with open(aggregate_file, 'r') as f:
        content = f.read()
    
    content = content.replace("USE_LOCAL_FILES = True", "USE_LOCAL_FILES = False")
    content = content.replace(f'LOCAL_FILES_PATH = "./downloaded_data/2025"', 'LOCAL_FILES_PATH = "./downloaded_data"')
    
    with open(aggregate_file, 'w') as f:
        f.write(content)
    
    print("   ✅ Reset complete")


if __name__ == "__main__":
    fetch_all_missing_years()
