#!/usr/bin/env python3
"""
Check raw S3 files for XLF data in 2016
"""

import subprocess
import sys
from pathlib import Path

def check_raw_s3_files():
    """Check raw S3 files for XLF data"""
    print("🔍 Checking raw S3 files for XLF data in 2016...")
    
    try:
        base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
        
        # Modify aggregate script to check all data without any filtering
        script_path = base_dir / "src/backtesting/data/aggregate.py"
        
        with open(script_path, 'r') as f:
            script_content = f.read()
        
        # Remove all filtering
        modified_content = script_content.replace("TICKERS_TO_FILTER = ['AMZN']", "TICKERS_TO_FILTER = []")
        modified_content = modified_content.replace("TEST_YEAR = 2025", "TEST_YEAR = 2016")
        modified_content = modified_content.replace("WEEKLY_ENTRY_DAYS_RANGE = 5", "WEEKLY_ENTRY_DAYS_RANGE = None")
        modified_content = modified_content.replace("MONTHLY_ENTRY_DAYS_RANGE = (28, 32)", "MONTHLY_ENTRY_DAYS_RANGE = None")
        
        # Also disable the weekly/monthly filtering
        modified_content = modified_content.replace("# Filter to Weekly and Monthly options only", "# DISABLED: Filter to Weekly and Monthly options only")
        modified_content = modified_content.replace("df = df[df['expiration_category'].isin(['Weekly', 'Monthly'])].copy()", "# DISABLED: df = df[df['expiration_category'].isin(['Weekly', 'Monthly'])].copy()")
        
        with open(script_path, 'w') as f:
            f.write(modified_content)
        
        try:
            cmd = [sys.executable, "src/backtesting/data/aggregate.py"]
            
            print("   Running with all filtering disabled...")
            result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
            
            # Look for any XLF mentions in output
            lines = result.stdout.split('\n')
            for line in lines:
                if 'XLF' in line or 'symbol' in line.lower() or 'ticker' in line.lower():
                    print(f"     {line}")
            
            # Check the final files
            monthly_file = base_dir / f"options_day_aggs_2016_monthly.csv"
            if monthly_file.exists():
                import pandas as pd
                df = pd.read_csv(monthly_file)
                print(f"   Final monthly file: {len(df)} rows")
                
                if len(df) > 0 and 'ticker' in df.columns:
                    # Check for any XLF
                    xlf_mask = df['ticker'].str.contains('XLF', na=False)
                    xlf_count = xlf_mask.sum()
                    print(f"   XLF rows found: {xlf_count}")
                    
                    if xlf_count > 0:
                        print(f"   Sample XLF tickers: {df[xlf_mask]['ticker'].head(5).tolist()}")
                    
                    # Show unique symbols
                    symbols = df['ticker'].str.extract(r'O:([A-Z]{1,5})')[0].dropna().unique()
                    print(f"   All symbols: {sorted(symbols)}")
                    
                    # Show first few rows
                    print(f"   First 3 rows:")
                    for i, row in df.head(3).iterrows():
                        print(f"     {row.get('ticker', 'N/A')}")
            
        finally:
            # Restore original script
            with open(script_path, 'w') as f:
                f.write(script_content)
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")

def main():
    """Main function"""
    check_raw_s3_files()

if __name__ == "__main__":
    main()
