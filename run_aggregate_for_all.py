#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path


def run_aggregate_for_ticker(ticker, year):
    """Run aggregate.py for a specific ticker and year"""
    
    data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data")
    aggregate_file = data_dir / "aggregate.py"
    
    # Read current content
    with open(aggregate_file, 'r') as f:
        content = f.read()
    
    # Create new content with proper replacements
    lines = content.split('\n')
    new_lines = []
    
    for line in lines:
        if 'TICKERS_TO_FILTER' in line and '=' in line:
            new_lines.append(f"TICKERS_TO_FILTER = ['{ticker}']")
        elif 'TEST_YEAR' in line and '=' in line:
            new_lines.append(f"TEST_YEAR = {year}")
        else:
            new_lines.append(line)
    
    # Write back the file
    with open(aggregate_file, 'w') as f:
        f.write('\n'.join(new_lines))
    
    # Run aggregate.py
    print(f"   📦 Running aggregate.py for {ticker} {year}...")
    result = subprocess.run([
        sys.executable, "aggregate.py"
    ], cwd=str(data_dir),
        capture_output=True, text=True, timeout=1800)
    
    return result.returncode == 0


def main():
    """Process all tickers for all years"""
    
    tickers = ['MSFT', 'GOOG', 'NVDA', 'SPY']
    years = list(range(2016, 2026))
    
    total_success = 0
    
    for ticker in tickers:
        print(f"\n{'='*60}")
        print(f"🚀 Processing {ticker} for all years 2016-2025")
        print(f"{'='*60}")
        
        ticker_success = 0
        
        for year in years:
            success = run_aggregate_for_ticker(ticker, year)
            if success:
                ticker_success += 1
                total_success += 1
            else:
                print(f"   ❌ Failed to process {ticker} {year}")
        
        print(f"\n✅ {ticker}: Successfully processed {ticker_success}/10 years")
    
    print(f"\n{'='*60}")
    print(f"🎉 ALL TICKERS PROCESSING COMPLETED!")
    print(f"✅ Successfully processed {total_success} ticker-year combinations")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
