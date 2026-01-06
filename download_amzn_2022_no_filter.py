#!/usr/bin/env python3
"""
Download AMZN data for 2022 with disabled entry day filtering
"""

import subprocess
import sys
import os

def main():
    """Main function to download AMZN 2022 data"""
    print("🔄 Downloading AMZN data for 2022 (with disabled entry day filtering)...")
    
    # Path to the aggregate.py script
    script_path = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/src/backtesting/data/aggregate.py"
    
    try:
        # Modify the script to download AMZN 2022 data
        print("Modifying aggregate.py to download AMZN 2022 data...")
        
        # Read the current script
        with open(script_path, 'r') as f:
            content = f.read()
        
        # Backup original content
        original_content = content
        
        # Modify for AMZN 2022 with disabled entry day filtering
        content = content.replace("TEST_YEAR = 2025", "TEST_YEAR = 2022")
        content = content.replace("TICKERS_TO_FILTER = ['AMZN']", "TICKERS_TO_FILTER = ['AMZN']")
        
        # Disable entry day filtering by setting ranges to None
        content = content.replace("MONTHLY_ENTRY_DAYS_RANGE = (28, 32)", "MONTHLY_ENTRY_DAYS_RANGE = None")
        content = content.replace("WEEKLY_ENTRY_DAYS_RANGE = 5", "WEEKLY_ENTRY_DAYS_RANGE = None")
        
        # Write modified content
        with open(script_path, 'w') as f:
            f.write(content)
        
        print("Running aggregate.py to download AMZN 2022 data...")
        
        # Run the script
        result = subprocess.run([
            sys.executable, script_path, '--year', '2022'
        ], capture_output=True, text=True, cwd="/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
        
        print("Script output:")
        print(result.stdout)
        
        if result.stderr:
            print("Script errors:")
            print(result.stderr)
        
        # Restore original content
        with open(script_path, 'w') as f:
            f.write(original_content)
        
        print("✅ AMZN 2022 data download completed!")
        print("📊 Data should be available in: /Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/AMZN/")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        # Restore original content in case of error
        try:
            with open(script_path, 'w') as f:
                f.write(original_content)
        except:
            pass

if __name__ == "__main__":
    main()
