#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path


def process_all_remaining():
    """Process MSFT, GOOG, NVDA, SPY for all years 2016-2025"""
    
    tickers = ['MSFT', 'GOOG', 'NVDA', 'SPY']
    years = list(range(2016, 2026))
    
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    
    for ticker in tickers:
        print(f"\n{'='*60}")
        print(f"🚀 Processing {ticker} for all years 2016-2025")
        print(f"{'='*60}")
        
        for year in years:
            print(f"\n📅 Processing {ticker} {year}...")
            
            # Update aggregate.py
            with open(data_dir / "aggregate.py", 'r') as f:
                content = f.read()
            
            lines = content.split('\n')
            new_lines = []
            
            for line in lines:
                if 'TEST_YEAR' in line and '=' in line and not line.strip().startswith('#'):
                    new_lines.append(f"TEST_YEAR = {year}")
                elif 'TICKERS_TO_FILTER' in line and '=' in line and not line.strip().startswith('#'):
                    new_lines.append(f"TICKERS_TO_FILTER = ['{ticker}']")
                else:
                    new_lines.append(line)
            
            with open(data_dir / "aggregate.py", 'w') as f:
                f.write('\n'.join(new_lines))
            
            # Remove existing duckdb file
            duckdb_path = data_dir / "options.duckdb"
            if duckdb_path.exists():
                duckdb_path.unlink()
            
            # Run aggregate.py
            result = subprocess.run([
                sys.executable, "aggregate.py"
            ], cwd=str(data_dir),
                capture_output=True, text=True, timeout=1800)
            
            if result.returncode == 0:
                print(f"   ✅ Successfully processed {ticker} {year}")
            else:
                print(f"   ❌ Failed to process {ticker} {year}: {result.stderr[:200]}")
    
    print(f"\n{'='*60}")
    print(f"🎉 ALL TICKERS PROCESSING COMPLETED!")
    print(f"{'='*60}")


if __name__ == "__main__":
    process_all_remaining()
