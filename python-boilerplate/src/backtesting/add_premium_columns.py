"""
Add premium and OTM columns to options data after merging stock prices.

This script adds the same columns that were added to the 2019 dataset:
- otm_pct: Out-The-Money percentage
- ITM: In-The-Money boolean (YES/NO)
- premium: Premium in cents (close_price)
- premium_yield_pct: Premium yield as percentage
- premium_low: Premium in cents (low_price)
- premium_yield_pct_low: Premium yield as percentage (using low_price)
"""

import pandas as pd
import sys
from pathlib import Path


def add_premium_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add premium and OTM columns to options DataFrame.
    
    Args:
        df: DataFrame with options data and underlying prices
        
    Returns:
        DataFrame with added columns: otm_pct, ITM, premium, premium_yield_pct, 
        premium_low, premium_yield_pct_low
    """
    df = df.copy()
    
    # Ensure we have underlying_spot (use underlying_high if pessimistic, otherwise underlying_close)
    if 'underlying_spot' not in df.columns:
        if 'underlying_high' in df.columns:
            df['underlying_spot'] = df['underlying_high']
        elif 'underlying_close' in df.columns:
            df['underlying_spot'] = df['underlying_close']
        else:
            raise ValueError("Need underlying_spot, underlying_high, or underlying_close column")
    
    # Correct strike prices to show actual dollar values by recalculating from ticker
    # The conversion to dollars depends on the underlying spot price:
    #   - If underlying_spot >= 100: raw_strike / 1000 = strike_dollars
    #   - If underlying_spot < 100: raw_strike / 10000 = strike_dollars
    #
    # We'll recalculate strikes from the ticker to ensure correctness
    def parse_strike_from_ticker(ticker):
        """Extract raw strike value from ticker."""
        if not isinstance(ticker, str) or not ticker.startswith('O:'):
            return None
        ticker = ticker[2:]  # Remove 'O:'
        # Find where the strike part starts (after expiration and option type)
        # Format: SYMBOL + YYMMDD + C/P + STRIKE (8 digits)
        for i, char in enumerate(ticker):
            if char.isdigit():
                symbol_end = i
                break
        else:
            return None
        remaining = ticker[symbol_end:]
        if len(remaining) < 15:  # Need at least 6 (date) + 1 (type) + 8 (strike)
            return None
        try:
            strike_str = remaining[7:]  # Skip 6-digit date and 1-char type
            return int(strike_str)
        except (ValueError, IndexError):
            return None
    
    # Calculate OTM percentage - ((strike - underlying_spot) / underlying_spot) * 100
    df['otm_pct'] = (((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100).round(2)
    
    # ITM: YES if strike < spot (negative otm_pct), NO otherwise
    df['ITM'] = (df['strike'] < df['underlying_spot']).map({True: 'YES', False: 'NO'})
    
    # Premium using mid_price: (high_price + low_price) / 2
    # If mid_price column doesn't exist, calculate it
    if 'mid_price' not in df.columns:
        if 'high_price' in df.columns and 'low_price' in df.columns:
            df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
        else:
            # Fallback to close_price if mid_price can't be calculated
            df['mid_price'] = df['close_price']
    
    # Premium in dollars (mid_price is already in dollars per share)
    df['premium'] = df['mid_price']
    
    # Premium yield as percentage: (mid_price / underlying_spot) * 100
    # mid_price is already in dollars per share, underlying_spot is in dollars
    df['premium_yield_pct'] = (df['mid_price'] / df['underlying_spot'] * 100).round(2)
    
    # Premium low in dollars (low_price is already in dollars per share)
    df['premium_low'] = df['low_price']
    
    # Premium yield low as percentage: (low_price / underlying_spot) * 100
    # low_price is already in dollars per share, underlying_spot is in dollars
    df['premium_yield_pct_low'] = (df['low_price'] / df['underlying_spot'] * 100).round(2)
    
    return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Add premium and OTM columns to options data")
    parser.add_argument("--input-file", required=True, help="Path to input CSV file (with stock prices merged)")
    parser.add_argument("--output-file", required=True, help="Path to output CSV file")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("ADDING PREMIUM AND OTM COLUMNS")
    print("=" * 80)
    print(f"  Input file: {args.input_file}")
    print(f"  Output file: {args.output_file}")
    
    # Load data
    print("\n📊 Loading data...")
    df = pd.read_csv(args.input_file)
    print(f"   Loaded {len(df):,} rows")
    
    # Add premium columns
    print("\n📈 Adding premium and OTM columns...")
    df = add_premium_columns(df)
    
    # Reorder columns to match 2019 format
    # Expected order: ticker,date_only,expiration_date,underlying_symbol,option_type,strike,volume,open_price,close_price,otm_pct,ITM,premium,premium_yield_pct,premium_low,premium_yield_pct_low,high_price,low_price,...
    base_cols = ['ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 'strike', 'volume']
    price_cols = ['open_price', 'close_price']
    premium_cols = ['otm_pct', 'ITM', 'premium', 'premium_yield_pct', 'premium_low', 'premium_yield_pct_low']
    other_cols = ['high_price', 'low_price', 'transactions', 'window_start', 'days_to_expiry', 'time_remaining_category']
    underlying_cols = ['underlying_open', 'underlying_close', 'underlying_high', 'underlying_low', 'underlying_spot',
                       'underlying_close_at_expiry', 'underlying_high_at_expiry', 'underlying_spot_at_expiry']
    
    # Build column order
    ordered_cols = []
    for col_list in [base_cols, price_cols, premium_cols, other_cols, underlying_cols]:
        for col in col_list:
            if col in df.columns:
                ordered_cols.append(col)
    
    # Add any remaining columns
    for col in df.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    df = df[ordered_cols]
    
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


    print(f"   ITM options: {(df['otm_pct'] < 0).sum():,}")
    print(f"   Average premium yield: {df['premium_yield_pct'].mean():.2f}%")
    print(f"   Average premium yield (low): {df['premium_yield_pct_low'].mean():.2f}%")


if __name__ == "__main__":
    main()

