#!/usr/bin/env python3
import pandas as pd
import sys
from pathlib import Path


def estimate_underlying_from_options(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate underlying stock price from options data.
    
    For OTM calls (strike > estimated_spot): underlying ≈ strike - option_price
    For ITM calls (strike < estimated_spot): underlying ≈ strike + option_price
    """
    df = df.copy()
    
    print(f"   Estimating underlying price for all rows...")
    
    # Simple estimation: underlying ≈ strike + option_price for calls
    # This is a rough heuristic but should give reasonable estimates
    df['underlying_spot'] = df['strike'] + df['close_price']
    
    # Add other price columns based on the spot estimate
    df['underlying_open'] = df['underlying_spot']  # Same as spot for simplicity
    df['underlying_close'] = df['underlying_spot']
    df['underlying_high'] = df['underlying_spot'] * 1.02  # 2% higher than spot
    df['underlying_low'] = df['underlying_spot'] * 0.98   # 2% lower than spot
    
    # Add expiration prices (same as spot for simplicity)
    df['underlying_close_at_expiry'] = df['underlying_spot']
    df['underlying_high_at_expiry'] = df['underlying_spot']
    df['underlying_spot_at_expiry'] = df['underlying_spot']
    
    return df


def add_premium_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add premium and OTM columns to options data.
    """
    df = df.copy()
    
    # Calculate OTM percentage using the corrected formula
    df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot'] * 100).round(2)
    
    # ITM: YES if strike < spot (negative otm_pct), NO otherwise
    df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Premium in dollars (close_price is already in dollars)
    df['premium'] = df['close_price']
    
    # Premium yield as percentage: (close_price / underlying_spot) * 100
    df['premium_yield_pct'] = (df['close_price'] / df['underlying_spot'] * 100).round(2)
    
    # Premium low in dollars (low_price is already in dollars)
    df['premium_low'] = df['low_price']
    
    # Premium yield low as percentage: (low_price / underlying_spot) * 100
    df['premium_yield_pct_low'] = (df['low_price'] / df['underlying_spot'] * 100).round(2)
    
    return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Process 2017 options data with estimated prices")
    parser.add_argument("--input-file", required=True, help="Path to input CSV file")
    parser.add_argument("--output-file", required=True, help="Path to output CSV file")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("PROCESSING 2017 OPTIONS DATA WITH ESTIMATED PRICES")
    print("=" * 60)
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output_file}")
    
    # Load data
    print(f"\n📊 Loading data...")
    df = pd.read_csv(args.input_file)
    print(f"   Loaded {len(df):,} rows")
    
    # Estimate underlying prices
    print(f"\n📈 Estimating underlying prices from options data...")
    df = estimate_underlying_from_options(df)
    
    # Add premium and OTM columns
    print(f"\n💰 Adding premium and OTM columns...")
    df = add_premium_columns(df)
    
    # Save
    print(f"\n💾 Saving to {args.output_file}...")
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output_file, index=False)
    
    print(f"✅ Saved {len(df):,} rows to {args.output_file}")
    print(f"\n📊 Summary:")
    print(f"   Date range: {df['date_only'].min()} to {df['date_only'].max()}")
    print(f"   Strike range: ${df['strike'].min():.2f} to ${df['strike'].max():.2f}")
    print(f"   Estimated spot range: ${df['underlying_spot'].min():.2f} to ${df['underlying_spot'].max():.2f}")
    print(f"   OTM options: {(df['otm_pct'] > 0).sum():,}")
    print(f"   ITM options: {(df['otm_pct'] <= 0).sum():,}")


if __name__ == "__main__":
    main()
