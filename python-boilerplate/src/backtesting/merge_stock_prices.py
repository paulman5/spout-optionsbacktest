"""
Merge historical stock prices with options data.

This script loads options data and merges it with historical stock prices
from a CSV file, using pessimistic scenario (HIGH prices).
"""

import sys
import pandas as pd
from pathlib import Path
import importlib.util

# Import monthly.py functions
monthly_path = Path(__file__).parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

load_options_data = monthly.load_options_data
add_underlying_prices_from_csv = monthly.add_underlying_prices_from_csv


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge stock prices with options data")
    parser.add_argument("--options-file", required=True, help="Path to options CSV file")
    parser.add_argument("--stock-file", required=True, help="Path to historical stock prices CSV")
    parser.add_argument("--symbol", default="TSLA", help="Underlying symbol")
    parser.add_argument("--output-file", required=True, help="Output CSV file path")
    parser.add_argument("--pessimistic", action="store_true", default=True,
                       help="Use pessimistic scenario (HIGH prices)")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("MERGING STOCK PRICES WITH OPTIONS DATA")
    print("=" * 80)
    print(f"  Options file: {args.options_file}")
    print(f"  Stock file: {args.stock_file}")
    print(f"  Symbol: {args.symbol}")
    print(f"  Output file: {args.output_file}")
    print(f"  Pessimistic: {args.pessimistic}")
    
    # Load options data
    print("\n📊 Loading options data...")
    df = load_options_data(args.options_file)
    print(f"   Loaded {len(df):,} options")
    
    # Add underlying prices
    print("\n📈 Adding underlying stock prices...")
    df = add_underlying_prices_from_csv(
        df,
        args.stock_file,
        symbol=args.symbol,
        use_pessimistic=args.pessimistic
    )
    
    # Save merged data
    print(f"\n💾 Saving merged data to {args.output_file}...")
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output_file, index=False)
    
    print(f"✅ Saved {len(df):,} rows to {args.output_file}")


if __name__ == "__main__":
    main()








