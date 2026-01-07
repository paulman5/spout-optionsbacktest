"""
Master script to process all TSLA years from S3 and backtest.

1. Aggregates all years from S3
2. Merges with stock prices
3. Adds all required columns
4. Runs backtest on combined data
"""

import subprocess
import sys
from pathlib import Path

def main():
    print("=" * 80)
    print("TSLA MULTI-YEAR PROCESSING AND BACKTESTING")
    print("=" * 80)
    
    # Step 1: Aggregate all years
    print("\n📦 Step 1: Aggregating all years from S3...")
    print("-" * 80)
    
    aggregate_script = Path(__file__).parent / "data" / "aggregate_all_years_tsla.py"
    
    result = subprocess.run(
        [sys.executable, str(aggregate_script)],
        cwd=Path(__file__).parent.parent.parent,
        capture_output=False
    )
    
    if result.returncode != 0:
        print("❌ Aggregation failed!")
        return 1
    
    # Step 2: Run backtest
    print("\n📊 Step 2: Running backtest on combined data...")
    print("-" * 80)
    
    combined_file = Path("data/TSLA/monthly/all_years/options_day_aggs_all_years_monthly_with_prices_pessimistic.csv")
    output_file = Path("data/TSLA/monthly/all_years/backtest_results/backtest_results_consolidated.csv")
    
    backtest_script = Path(__file__).parent / "backtest_otm_ranges.py"
    
    result = subprocess.run(
        [
            sys.executable, str(backtest_script),
            "--data-file", str(combined_file),
            "--symbol", "TSLA",
            "--ranges", "10-15", "15-20", "20-25", "25-30", "30-35", "35-40",
            "--consolidated-output", str(output_file)
        ],
        cwd=Path(__file__).parent.parent.parent,
        capture_output=False
    )
    
    if result.returncode != 0:
        print("❌ Backtest failed!")
        return 1
    
    print("\n" + "=" * 80)
    print("✅ ALL PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"\n📁 Output files:")
    print(f"   Combined data: {combined_file}")
    print(f"   Backtest results: {output_file}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())









