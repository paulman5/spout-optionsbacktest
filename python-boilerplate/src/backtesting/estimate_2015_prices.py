"""
Estimate 2015 stock prices from options data and fill missing values.

For 2015, we'll estimate underlying prices from options data where stock prices are missing.
"""

import pandas as pd
import sys
from pathlib import Path


def estimate_underlying_from_options(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate underlying stock price from options data.
    
    For OTM calls: underlying ≈ strike - (some function of option price)
    For ITM calls: underlying ≈ strike + option_price
    We'll use a simple heuristic based on the option price and strike.
    """
    df = df.copy()
    
    # For rows with missing underlying_spot, estimate it
    missing_mask = df['underlying_spot'].isna()
    
    if missing_mask.sum() == 0:
        return df
    
    print(f"   Estimating underlying price for {missing_mask.sum():,} rows with missing data...")
    
    # Simple estimation: for OTM calls (strike > estimated_spot), 
    # underlying ≈ strike - option_price (rough approximation)
    # For ITM, underlying ≈ strike + option_price
    
    # Start with a simple estimate: underlying ≈ strike - close_price for OTM
    # But we need to iterate or use a better method
    
    # Better approach: use the median ratio of strike/close_price for similar options
    # Or use the fact that for OTM calls, spot is typically below strike
    
    # For now, use a simple heuristic:
    # If close_price is small relative to strike, assume OTM: spot ≈ strike - close_price * 2
    # If close_price is large, assume closer to ATM: spot ≈ strike - close_price
    
    estimated_spot = df.loc[missing_mask, 'strike'] - df.loc[missing_mask, 'close_price'] * 1.5
    
    # Ensure reasonable bounds
    estimated_spot = estimated_spot.clip(lower=df.loc[missing_mask, 'close_price'], 
                                         upper=df.loc[missing_mask, 'strike'] * 1.2)
    
    df.loc[missing_mask, 'underlying_spot'] = estimated_spot
    
    # Fill other underlying columns with the same estimate
    for col in ['underlying_open', 'underlying_close', 'underlying_high', 'underlying_low']:
        if col in df.columns:
            df.loc[missing_mask, col] = estimated_spot
    
    # For expiration prices, use the same estimate (or we could improve this)
    exp_missing = df['underlying_spot_at_expiry'].isna()
    if exp_missing.sum() > 0:
        # Use entry spot as estimate for expiration (not ideal, but better than NaN)
        df.loc[exp_missing, 'underlying_spot_at_expiry'] = df.loc[exp_missing, 'underlying_spot']
        for col in ['underlying_close_at_expiry', 'underlying_high_at_expiry']:
            if col in df.columns:
                df.loc[exp_missing, col] = df.loc[exp_missing, 'underlying_spot_at_expiry']
    
    return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Estimate missing 2015 stock prices from options data")
    parser.add_argument("--input-file", required=True, help="Path to input CSV file")
    parser.add_argument("--output-file", required=True, help="Path to output CSV file")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("ESTIMATING MISSING 2015 STOCK PRICES")
    print("=" * 80)
    print(f"  Input file: {args.input_file}")
    print(f"  Output file: {args.output_file}")
    
    # Load data
    print("\n📊 Loading data...")
    df = pd.read_csv(args.input_file)
    print(f"   Loaded {len(df):,} rows")
    
    missing_before = df['underlying_spot'].isna().sum()
    print(f"   Rows with missing underlying_spot: {missing_before:,}")
    
    # Estimate missing prices
    print("\n📈 Estimating missing stock prices from options data...")
    df = estimate_underlying_from_options(df)
    
    missing_after = df['underlying_spot'].isna().sum()
    print(f"   Rows with missing underlying_spot after: {missing_after:,}")
    
    # Recalculate premium columns with estimated prices
    print("\n🔄 Recalculating premium and OTM columns...")
    
    # Calculate OTM percentage - strike divided by underlying spot
    df['otm_pct'] = ((df['strike'] / df['underlying_spot']) * 100).round(2)
    
    # Recalculate ITM
    df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Recalculate premium yields
    df['premium_yield_pct'] = (df['premium'] / df['underlying_spot'] * 100).round(2)
    df['premium_yield_pct_low'] = (df['premium_low'] / df['underlying_spot'] * 100).round(2)
    
    # Save
    print(f"\n💾 Saving to {args.output_file}...")
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output_file, index=False)
    
    print(f"✅ Saved {len(df):,} rows to {args.output_file}")
    print(f"\n📊 Summary:")
    print(f"   OTM options: {(df['otm_pct'] > 0).sum():,}")
    print(f"   ITM options: {(df['otm_pct'] < 0).sum():,}")
    print(f"   Average premium yield: {df['premium_yield_pct'].mean():.2f}%")
    print(f"   Average premium yield (low): {df['premium_yield_pct_low'].mean():.2f}%")


if __name__ == "__main__":
    main()

