"""
Backtest covered call strategy by OTM percentage ranges.

Tests options starting from 10% OTM in specific ranges (e.g., 10-15%, 15-20%)
and calculates premium yields throughout the year using pessimistic scenario.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import importlib.util

# Import monthly.py functions
monthly_path = Path(__file__).parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

calculate_pnl = monthly.calculate_pnl
get_underlying_price_at_expiration = monthly.get_underlying_price_at_expiration


def backtest_otm_range(
    data_file: str,
    symbol: str,
    otm_min: float,
    otm_max: float,
    min_volume: int = 0,
    use_low_premium: bool = False
) -> pd.DataFrame:
    """
    Backtest covered calls for a specific OTM percentage range.
    
    Args:
        data_file: Path to CSV file with options data
        symbol: Underlying symbol (e.g., 'TSLA')
        otm_min: Minimum OTM percentage (e.g., 10.0 for 10%)
        otm_max: Maximum OTM percentage (e.g., 15.0 for 15%)
        min_volume: Minimum volume filter (default: 0, no filter)
        use_low_premium: If True, use low_price for premium (pessimistic), else use close_price
    
    Returns:
        DataFrame with backtest results
    """
    print(f"\n{'='*80}")
    print(f"BACKTESTING {otm_min}%-{otm_max}% OTM RANGE")
    print(f"{'='*80}")
    
    # Load data
    df = pd.read_csv(data_file)
    
    # Filter to calls only
    df = df[df['option_type'] == 'C'].copy()
    
    # Filter to OTM range (10% and above)
    df = df[(df['otm_pct'] >= otm_min) & (df['otm_pct'] <= otm_max)].copy()
    
    # Filter by volume if specified
    if min_volume > 0:
        df = df[df['volume'] >= min_volume].copy()
    
    print(f"  Options in {otm_min}%-{otm_max}% OTM range: {len(df):,}")
    
    if len(df) == 0:
        print(f"  ⚠️  No options found in this range")
        return pd.DataFrame()
    
    # Convert dates
    df['date_only'] = pd.to_datetime(df['date_only'])
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    results = []
    
    # Group by expiration date to process each expiration
    for exp_date, exp_group in df.groupby('expiration_date'):
        exp_date_str = exp_date.strftime('%Y-%m-%d') if hasattr(exp_date, 'strftime') else str(exp_date)
        
        for _, row in exp_group.iterrows():
            entry_date = row['date_only']
            strike = float(row['strike'])
            
            # Use low_price or close_price for premium (pessimistic vs normal)
            if use_low_premium:
                premium_collected = float(row['low_price']) / 100.0  # Most pessimistic
                premium_yield = float(row['premium_yield_pct_low'])
            else:
                premium_collected = float(row['close_price']) / 100.0
                premium_yield = float(row['premium_yield_pct'])
            
            volume = int(row['volume'])
            otm_pct = float(row['otm_pct'])
            
            # Get underlying prices
            entry_underlying = float(row['underlying_spot'])  # High price (pessimistic)
            
            if 'underlying_spot_at_expiry' in row and pd.notna(row['underlying_spot_at_expiry']):
                expiration_price = float(row['underlying_spot_at_expiry'])  # High price (pessimistic)
            else:
                expiration_price = get_underlying_price_at_expiration(df, symbol, exp_date_str)
                if expiration_price is None:
                    continue
            
            # Calculate P&L
            pnl_per_share, assigned = calculate_pnl(
                entry_underlying,
                strike,
                premium_collected,
                expiration_price
            )
            
            # Calculate returns
            pnl_yield = pnl_per_share / entry_underlying if entry_underlying > 0 else 0
            
            results.append({
                'entry_date': entry_date,
                'expiration_date': exp_date,
                'ticker': row['ticker'],
                'strike': strike,
                'otm_pct': otm_pct,
                'entry_underlying': entry_underlying,
                'expiration_underlying': expiration_price,
                'premium_collected': premium_collected,
                'premium_yield_pct': premium_yield,
                'premium_close': float(row['close_price']) / 100.0,
                'premium_low': float(row['low_price']) / 100.0,
                'premium_yield_close': float(row['premium_yield_pct']),
                'premium_yield_low': float(row['premium_yield_pct_low']),
                'pnl_per_share': pnl_per_share,
                'pnl_per_contract': pnl_per_share * 100,
                'pnl_yield': pnl_yield,
                'assigned': assigned,
                'volume': volume,
                'days_to_expiry': row['days_to_expiry'],
            })
    
    results_df = pd.DataFrame(results)
    
    if len(results_df) == 0:
        print(f"  ⚠️  No valid trades found")
        return pd.DataFrame()
    
    # Calculate summary statistics
    print(f"\n  Results Summary:")
    print(f"    Total trades: {len(results_df):,}")
    print(f"    Assigned: {results_df['assigned'].sum():,} ({results_df['assigned'].mean()*100:.1f}%)")
    print(f"    Not Assigned: {(~results_df['assigned']).sum():,} ({(~results_df['assigned']).mean()*100:.1f}%)")
    print(f"    Total P&L: ${results_df['pnl_per_contract'].sum():,.2f}")
    print(f"    Average P&L per trade: ${results_df['pnl_per_contract'].mean():.2f}")
    print(f"    Average premium yield (close): {results_df['premium_yield_close'].mean():.2f}%")
    print(f"    Average premium yield (low): {results_df['premium_yield_low'].mean():.2f}%")
    print(f"    Win rate: {(results_df['pnl_per_contract'] > 0).mean()*100:.1f}%")
    
    return results_df


def backtest_multiple_ranges(
    data_file: str,
    symbol: str,
    ranges: list,
    min_volume: int = 0,
    use_low_premium: bool = False,
    output_dir: str = None,
    consolidated_output: str = None
):
    """
    Backtest multiple OTM percentage ranges.
    
    Args:
        data_file: Path to CSV file with options data
        symbol: Underlying symbol
        ranges: List of tuples [(min1, max1), (min2, max2), ...] for OTM ranges
        min_volume: Minimum volume filter
        use_low_premium: Use low_price for premium (pessimistic)
        output_dir: Directory to save results (optional)
        consolidated_output: Path to single consolidated output file (optional)
    """
    print(f"\n{'='*80}")
    print(f"BACKTESTING MULTIPLE OTM RANGES")
    print(f"{'='*80}")
    print(f"  Data file: {data_file}")
    print(f"  Symbol: {symbol}")
    print(f"  Ranges: {ranges}")
    print(f"  Min volume: {min_volume}")
    print(f"  Use low premium: {use_low_premium}")
    
    all_results_close = []
    all_results_low = []
    summary = []
    
    # Run backtests for both close and low premium scenarios
    for otm_min, otm_max in ranges:
        # Close premium scenario
        results_close = backtest_otm_range(
            data_file=data_file,
            symbol=symbol,
            otm_min=otm_min,
            otm_max=otm_max,
            min_volume=min_volume,
            use_low_premium=False
        )
        
        # Low premium scenario
        results_low = backtest_otm_range(
            data_file=data_file,
            symbol=symbol,
            otm_min=otm_min,
            otm_max=otm_max,
            min_volume=min_volume,
            use_low_premium=True
        )
        
        if len(results_close) > 0:
            results_close['otm_range'] = f"{otm_min}%-{otm_max}%"
            all_results_close.append(results_close)
            
        if len(results_low) > 0:
            results_low['otm_range'] = f"{otm_min}%-{otm_max}%"
            all_results_low.append(results_low)
            
            summary.append({
                'otm_range': f"{otm_min}%-{otm_max}%",
                'trades': len(results_close),
                'assigned_pct': results_close['assigned'].mean() * 100,
                'total_pnl_close': results_close['pnl_per_contract'].sum(),
                'avg_pnl_close': results_close['pnl_per_contract'].mean(),
                'total_pnl_low': results_low['pnl_per_contract'].sum(),
                'avg_pnl_low': results_low['pnl_per_contract'].mean(),
                'avg_premium_yield_close': results_close['premium_yield_close'].mean(),
                'avg_premium_yield_low': results_close['premium_yield_low'].mean(),
                'win_rate': (results_close['pnl_per_contract'] > 0).mean() * 100,
            })
    
    # Combine all results
    if all_results_close and all_results_low:
        combined_close = pd.concat(all_results_close, ignore_index=True)
        combined_low = pd.concat(all_results_low, ignore_index=True)
        
        # Merge close and low premium results into one consolidated file
        consolidated = combined_close.copy()
        
        # Add low premium P&L columns
        low_pnl = combined_low[['ticker', 'entry_date', 'pnl_per_share', 'pnl_per_contract', 'pnl_yield']].copy()
        low_pnl.columns = ['ticker', 'entry_date', 'pnl_per_share_low', 'pnl_per_contract_low', 'pnl_yield_low']
        
        consolidated = consolidated.merge(low_pnl, on=['ticker', 'entry_date'], how='left')
        
        # Reorder columns for better readability
        cols = [
            'entry_date', 'expiration_date', 'otm_range', 'otm_pct',
            'strike', 'entry_underlying', 'expiration_underlying',  # Strike, entry price, and expiration price together
            'premium_yield_close', 'premium_yield_low',
            'pnl_per_contract', 'pnl_per_contract_low',
            'assigned', 'volume'
        ]
        
        # Ensure all columns exist
        consolidated = consolidated[[c for c in cols if c in consolidated.columns]]
        consolidated = consolidated.sort_values(['entry_date', 'otm_range', 'otm_pct'])
        
        # Save consolidated file
        if consolidated_output:
            consolidated.to_csv(consolidated_output, index=False)
            print(f"\n✅ Saved consolidated backtest results to: {consolidated_output}")
            print(f"   Total trades: {len(consolidated):,}")
        
        # Save individual files if output_dir specified
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = output_dir / f"backtest_otm_ranges_close_premium.csv"
            combined_close.to_csv(output_file, index=False)
            
            output_file = output_dir / f"backtest_otm_ranges_low_premium.csv"
            combined_low.to_csv(output_file, index=False)
            
            # Save summary
            summary_df = pd.DataFrame(summary)
            summary_file = output_dir / f"backtest_otm_ranges_summary.csv"
            summary_df.to_csv(summary_file, index=False)
            print(f"✅ Saved summary to: {summary_file}")
            
            print(f"\n{'='*80}")
            print("SUMMARY BY OTM RANGE:")
            print(f"{'='*80}")
            print(summary_df.to_string(index=False))
        
        return consolidated, pd.DataFrame(summary) if summary else pd.DataFrame()
    
    return pd.DataFrame(), pd.DataFrame()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Backtest covered calls by OTM ranges")
    parser.add_argument("--data-file", required=True, help="Path to options CSV file")
    parser.add_argument("--symbol", default="TSLA", help="Underlying symbol")
    parser.add_argument("--ranges", nargs="+", default=["10-15", "15-20", "20-25", "25-30", "30-35", "35-40"],
                       help="OTM ranges to test (e.g., '10-15' '15-20')")
    parser.add_argument("--min-volume", type=int, default=0, help="Minimum volume filter")
    parser.add_argument("--use-low-premium", action="store_true", 
                       help="Use low_price for premium (most pessimistic)")
    parser.add_argument("--output-dir", help="Directory to save results")
    parser.add_argument("--consolidated-output", default="data/TSLA/monthly/backtest_results/backtest_results_consolidated.csv",
                       help="Path to single consolidated output file")
    
    args = parser.parse_args()
    
    # Parse ranges
    ranges = []
    for range_str in args.ranges:
        try:
            min_val, max_val = map(float, range_str.split('-'))
            ranges.append((min_val, max_val))
        except:
            print(f"⚠️  Invalid range format: {range_str}, skipping")
    
    if not ranges:
        ranges = [(10, 15), (15, 20), (20, 25), (25, 30), (30, 35), (35, 40)]
    
    # Run backtests
    results, summary = backtest_multiple_ranges(
        data_file=args.data_file,
        symbol=args.symbol,
        ranges=ranges,
        min_volume=args.min_volume,
        use_low_premium=args.use_low_premium,
        output_dir=args.output_dir,
        consolidated_output=args.consolidated_output
    )


if __name__ == "__main__":
    main()











