"""
Backtesting script for covered call options strategy using historical data.

This script mimics the structure of apy.py but works with historical CSV data
instead of REST API calls.
"""

import os
import sys
import math
import argparse
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


# ============================================================================
# STEP 1: Helper functions (copied from apy.py, work with any data)
# ============================================================================

def norm_cdf(x: float) -> float:
    """Normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))

def midpoint(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    """Calculate midpoint between bid and ask."""
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    return 0.5 * (bid + ask)

def pop_estimate(S0: float, breakeven: float, iv: Optional[float], t_years: float) -> Optional[float]:
    """
    Estimate probability of profit using Black-Scholes approximation.
    
    Args:
        S0: Current spot price
        breakeven: Breakeven price (spot - premium for covered calls)
        iv: Implied volatility (None if not available)
        t_years: Time to expiry in years
    """
    if iv is None or iv <= 0 or breakeven <= 0 or t_years <= 0:
        return None
    d2 = (math.log(S0 / breakeven) - 0.5 * (iv ** 2) * t_years) / (iv * math.sqrt(t_years))
    return norm_cdf(d2)

def in_range(value, bounds):
    """Check if value is within bounds (lo, hi). None means unbounded."""
    if bounds is None:
        return True
    if value is None:
        return False
    lo, hi = bounds
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


# ============================================================================
# STEP 2: Data loading functions (adapted for CSV instead of REST API)
# ============================================================================

def load_options_data(csv_path: str) -> pd.DataFrame:
    """
    Load options data from CSV file.
    
    Returns DataFrame with columns matching what screen_candidates expects.
    """
    df = pd.read_csv(csv_path)
    
    # Convert date columns
    df['date_only'] = pd.to_datetime(df['date_only'])
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    # Ensure days_to_expiry is numeric
    if 'days_to_expiry' not in df.columns:
        df['days_to_expiry'] = (df['expiration_date'] - df['date_only']).dt.days
    
    return df

def load_historical_stock_prices(csv_path: str) -> pd.DataFrame:
    """
    Load historical stock prices from a CSV file.
    
    Expected CSV format:
    - Date column in MM/DD/YYYY format
    - Close/Last, Open, High, Low columns with $ prefix
    - Volume column
    
    Args:
        csv_path: Path to the historical stock prices CSV file
    
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    df = pd.read_csv(csv_path)
    
    # Parse date column (assuming MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date
    
    # Extract numeric values from price columns (remove $ and commas)
    def clean_price(value):
        if pd.isna(value):
            return None
        # Convert to string, remove $ and commas, then convert to float
        str_val = str(value).replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except ValueError:
            return None
    
    # Clean price columns
    df['close'] = df['Close/Last'].apply(clean_price)
    df['open'] = df['Open'].apply(clean_price)
    df['high'] = df['High'].apply(clean_price)
    df['low'] = df['Low'].apply(clean_price)
    
    # Volume - remove commas if present
    def clean_volume(value):
        if pd.isna(value):
            return None
        str_val = str(value).replace(',', '').strip()
        try:
            return int(float(str_val))  # Convert to int
        except ValueError:
            return None
    
    df['volume'] = df['Volume'].apply(clean_volume)
    
    # Select and return only the columns we need
    result = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
    
    # Sort by date (ascending - oldest first)
    result = result.sort_values('date').reset_index(drop=True)
    
    return result

def add_underlying_prices_from_csv(
    options_df: pd.DataFrame,
    stock_csv_path: str,
    symbol: str = None,
    use_pessimistic: bool = False
) -> pd.DataFrame:
    """
    Add underlying stock prices to options DataFrame by loading from a CSV file.
    
    This is an alternative to add_underlying_prices() that uses a local CSV file
    instead of fetching from yfinance.
    
    Fetches actual stock prices for:
    - Trading dates (date_only): underlying_open, underlying_close, underlying_high, underlying_low
    - Expiration dates (expiration_date): underlying_close_at_expiry (or underlying_high_at_expiry if pessimistic)
    
    Args:
        options_df: Options DataFrame with 'date_only', 'expiration_date', and 'underlying_symbol' columns
        stock_csv_path: Path to CSV file with historical stock prices
        symbol: Underlying symbol (optional, for validation/logging)
        use_pessimistic: If True, uses HIGH prices (most pessimistic scenario for covered calls)
                        - Entry: uses underlying_high instead of underlying_close
                        - Expiration: uses underlying_high_at_expiry instead of underlying_close_at_expiry
    
    Returns:
        DataFrame with added underlying price columns:
        - underlying_open: Stock open price on trading date
        - underlying_close: Stock close price on trading date  
        - underlying_high: Stock high price on trading date
        - underlying_low: Stock low price on trading date
        - underlying_close_at_expiry: Stock close price on expiration date (or high if pessimistic)
        - underlying_high_at_expiry: Stock high price on expiration date
        - underlying_spot: The price to use for calculations (high if pessimistic, close otherwise)
        - underlying_spot_at_expiry: The price to use at expiration (high if pessimistic, close otherwise)
    """
    options_df = options_df.copy()
    
    # Load historical stock prices
    print(f"📈 Loading stock prices from {stock_csv_path}...")
    stock_prices = load_historical_stock_prices(stock_csv_path)
    
    if stock_prices.empty:
        raise ValueError(f"No stock price data found in {stock_csv_path}")
    
    print(f"   Loaded {len(stock_prices):,} days of stock price data")
    print(f"   Date range: {stock_prices['date'].min()} to {stock_prices['date'].max()}")
    
    # Validate symbol if provided
    if symbol and 'underlying_symbol' in options_df.columns:
        symbols_in_data = options_df['underlying_symbol'].unique()
        if symbol not in symbols_in_data:
            print(f"⚠️  Warning: Symbol {symbol} not found in options data. Found: {symbols_in_data}")
    
    # Convert date_only to date for joining
    options_df['date_only_date'] = pd.to_datetime(options_df['date_only']).dt.date
    
    # Join stock prices on trading date
    options_df = options_df.merge(
        stock_prices[['date', 'open', 'close', 'high', 'low']],
        left_on='date_only_date',
        right_on='date',
        how='left'
    )
    
    # Rename columns
    options_df = options_df.rename(columns={
        'open': 'underlying_open',
        'close': 'underlying_close',
        'high': 'underlying_high',
        'low': 'underlying_low'
    })
    
    # Add pessimistic spot price (high for entry if pessimistic, otherwise close)
    if use_pessimistic:
        options_df['underlying_spot'] = options_df['underlying_high']
        print("   Using HIGH prices for entry (pessimistic scenario)")
    else:
        options_df['underlying_spot'] = options_df['underlying_close']
        print("   Using CLOSE prices for entry (normal scenario)")
    
    # Drop temporary date column
    options_df = options_df.drop(columns=['date_only_date', 'date'], errors='ignore')
    
    # Add expiration day prices
    if 'expiration_date' in options_df.columns:
        options_df['expiration_date_date'] = pd.to_datetime(options_df['expiration_date']).dt.date
        exp_prices = stock_prices[['date', 'close', 'high']].rename(columns={
            'close': 'underlying_close_at_expiry',
            'high': 'underlying_high_at_expiry'
        })
        options_df = options_df.merge(
            exp_prices,
            left_on='expiration_date_date',
            right_on='date',
            how='left'
        )
        
        # Add pessimistic spot price at expiry (high if pessimistic, otherwise close)
        if use_pessimistic:
            options_df['underlying_spot_at_expiry'] = options_df['underlying_high_at_expiry']
            print("   Using HIGH prices for expiration (pessimistic scenario)")
        else:
            options_df['underlying_spot_at_expiry'] = options_df['underlying_close_at_expiry']
            print("   Using CLOSE prices for expiration (normal scenario)")
        
        options_df = options_df.drop(columns=['expiration_date_date', 'date'], errors='ignore')
    
    # Check for missing prices (check the spot price we're actually using)
    missing_trading = options_df['underlying_spot'].isna().sum()
    missing_expiry = options_df['underlying_spot_at_expiry'].isna().sum() if 'underlying_spot_at_expiry' in options_df.columns else 0
    
    if missing_trading > 0:
        price_type = "underlying_high" if use_pessimistic else "underlying_close"
        print(f"⚠️  Warning: {missing_trading} rows missing {price_type} (trading date)")
        # Show some examples of missing dates
        missing_dates = options_df[options_df['underlying_spot'].isna()]['date_only'].unique()[:5]
        print(f"   Example missing dates: {missing_dates}")
    
    if missing_expiry > 0:
        price_type = "underlying_high_at_expiry" if use_pessimistic else "underlying_close_at_expiry"
        print(f"⚠️  Warning: {missing_expiry} rows missing {price_type} (expiration date)")
        # Show some examples of missing dates
        missing_exp_dates = options_df[options_df['underlying_spot_at_expiry'].isna()]['expiration_date'].unique()[:5]
        print(f"   Example missing expiration dates: {missing_exp_dates}")
    
    if missing_trading == 0 and missing_expiry == 0:
        print("✅ All rows have underlying price data")
    
    return options_df

def fetch_underlying_prices(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical stock prices for the underlying symbol.
    
    Args:
        symbol: Stock symbol (e.g., 'TSLA')
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
    
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError(
            "yfinance is required to fetch stock prices. Install it with: pip install yfinance"
        )
    
    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=start_date, end=end_date)
    
    if hist.empty:
        raise ValueError(f"No price data found for {symbol} between {start_date} and {end_date}")
    
    # Reset index to make Date a column
    hist = hist.reset_index()
    
    # Rename columns to match our needs
    hist['date'] = pd.to_datetime(hist['Date']).dt.date
    hist = hist[['date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    hist.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    
    return hist

def add_underlying_prices(df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
    """
    Add underlying stock prices to options DataFrame by joining on date.
    
    Fetches actual stock prices for:
    - Trading dates (date_only): underlying_open, underlying_close, underlying_high, underlying_low
    - Expiration dates (expiration_date): underlying_close_at_expiry
    
    Args:
        df: Options DataFrame with 'date_only', 'expiration_date', and 'underlying_symbol' columns
        symbol: Underlying symbol (if None, uses 'underlying_symbol' column from data)
                If multiple symbols exist, fetches prices for each symbol separately.
    
    Returns:
        DataFrame with added underlying price columns:
        - underlying_open: Stock open price on trading date
        - underlying_close: Stock close price on trading date  
        - underlying_high: Stock high price on trading date
        - underlying_low: Stock low price on trading date
        - underlying_close_at_expiry: Stock close price on expiration date
    """
    df = df.copy()
    
    # Determine which symbols to fetch
    if symbol is None:
        if 'underlying_symbol' not in df.columns:
            raise ValueError("Either provide 'symbol' parameter or ensure 'underlying_symbol' column exists")
        symbols = df['underlying_symbol'].unique()
        if len(symbols) > 1:
            print(f"⚠️  Multiple symbols found: {symbols}. Fetching prices for each separately.")
    else:
        symbols = [symbol]
    
    # Get date range from options data (across all symbols)
    min_date = pd.to_datetime(df['date_only']).min().date()
    max_date = pd.to_datetime(df['date_only']).max().date()
    
    # Also include expiration dates in range
    if 'expiration_date' in df.columns:
        exp_min = pd.to_datetime(df['expiration_date']).min().date()
        exp_max = pd.to_datetime(df['expiration_date']).max().date()
        min_date = min(min_date, exp_min)
        max_date = max(max_date, exp_max)
    
    # Fetch stock prices for each symbol and merge
    all_results = []
    
    for sym in symbols:
        sym_df = df[df['underlying_symbol'] == sym].copy() if len(symbols) > 1 else df
        
        # Fetch stock prices for this symbol
        start_date = min_date.strftime('%Y-%m-%d')
        end_date = (max_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"📈 Fetching stock prices for {sym} from {start_date} to {end_date}...")
        stock_prices = fetch_underlying_prices(sym, start_date, end_date)
        
        # Convert date_only to date for joining
        sym_df['date_only_date'] = pd.to_datetime(sym_df['date_only']).dt.date
        
        # Join stock prices on trading date
        sym_df = sym_df.merge(
            stock_prices[['date', 'open', 'close', 'high', 'low']],
            left_on='date_only_date',
            right_on='date',
            how='left'
        )
        
        # Rename columns
        sym_df = sym_df.rename(columns={
            'open': 'underlying_open',
            'close': 'underlying_close',
            'high': 'underlying_high',
            'low': 'underlying_low'
        })
        
        # Drop temporary date column
        sym_df = sym_df.drop(columns=['date_only_date', 'date'], errors='ignore')
        
        # Add expiration day prices
        if 'expiration_date' in sym_df.columns:
            sym_df['expiration_date_date'] = pd.to_datetime(sym_df['expiration_date']).dt.date
            exp_prices = stock_prices[['date', 'close']].rename(columns={'close': 'underlying_close_at_expiry'})
            sym_df = sym_df.merge(
                exp_prices,
                left_on='expiration_date_date',
                right_on='date',
                how='left'
            )
            sym_df = sym_df.drop(columns=['expiration_date_date', 'date'], errors='ignore')
        
        all_results.append(sym_df)
    
    # Combine results if multiple symbols
    if len(all_results) > 1:
        result_df = pd.concat(all_results, ignore_index=True)
    else:
        result_df = all_results[0]
    
    # Check for missing prices
    missing_trading = result_df['underlying_close'].isna().sum()
    missing_expiry = result_df['underlying_close_at_expiry'].isna().sum() if 'underlying_close_at_expiry' in result_df.columns else 0
    
    if missing_trading > 0:
        print(f"⚠️  Warning: {missing_trading} rows missing underlying_close (trading date)")
    if missing_expiry > 0:
        print(f"⚠️  Warning: {missing_expiry} rows missing underlying_close_at_expiry (expiration date)")
    
    return result_df

def get_spot_price(row: pd.Series) -> float:
    """
    Get underlying spot price from row data.
    
    First tries to use actual underlying_spot if available (from add_underlying_prices_from_csv),
    then underlying_close, otherwise falls back to estimation.
    
    For OTM calls: spot ≈ strike - option_price (rough approximation)
    For ATM/ITM: spot ≈ strike + option_price (for deep ITM)
    """
    # Prefer underlying_spot (which may be high if pessimistic scenario)
    if 'underlying_spot' in row and pd.notna(row['underlying_spot']):
        return float(row['underlying_spot'])
    
    # Fall back to underlying_close
    if 'underlying_close' in row and pd.notna(row['underlying_close']):
        return float(row['underlying_close'])
    
    # Fall back to estimation
    strike = float(row['strike'])
    option_price = float(row['close_price'])
    
    # Simple approximation: assume spot is slightly above strike for OTM calls
    # For covered calls, we typically sell OTM, so spot < strike
    # Estimate: spot = strike - (some function of option price)
    # A rough heuristic: spot ≈ strike - option_price for OTM calls
    estimated_spot = strike - option_price * 0.5  # Adjust multiplier as needed
    
    # Ensure reasonable bounds (spot shouldn't be negative or way above strike)
    estimated_spot = max(option_price, estimated_spot)  # At least as high as option price
    estimated_spot = min(strike * 1.5, estimated_spot)  # Not way above strike
    
    return estimated_spot

def estimate_spot_price(row: pd.Series) -> float:
    """
    Deprecated: Use get_spot_price instead.
    Kept for backward compatibility.
    """
    return get_spot_price(row)


# ============================================================================
# STEP 3: Adapted screen_candidates for DataFrame (instead of REST API chain)
# ============================================================================

def screen_candidates_from_df(
    df: pd.DataFrame,
    min_otm_pct=0.00,
    max_otm_pct=0.03,
    delta_lo=0.20,  # Note: We don't have delta, so this filter will be skipped
    delta_hi=0.40,  # Note: We don't have delta, so this filter will be skipped
    min_bid=0.05,   # Using close_price instead
    min_oi=1,       # Note: We don't have open_interest, so this filter will be skipped
    min_volume=0,
    max_spread_to_mid=0.75,  # Note: We don't have bid/ask, so this filter will be skipped
    min_premium_yield=0.0,
    iv_range=None,  # Note: We don't have IV, so this filter will be skipped
    days_limit=None,  # Using days_to_expiry instead of minutes
    capital_limit=None,
    rank_metric="premium_yield",
    use_estimated_spot=True  # Whether to estimate spot or require it in data
) -> List[Dict]:
    """
    Screen options candidates from a DataFrame (adapted from apy.py screen_candidates).
    
    This function mimics the logic of screen_candidates() but works with our CSV data structure.
    Some filters are skipped if the required data isn't available (delta, IV, open_interest, bid/ask).
    """
    # Filter to calls only
    df = df[df['option_type'] == 'C'].copy()
    
    if len(df) == 0:
        return []
    
    # Get spot price for each row (uses actual underlying_close if available, otherwise estimates)
    if 'spot' not in df.columns:
        df['spot'] = df.apply(get_spot_price, axis=1)
    
    # Calculate strike bounds based on OTM percentage
    df['strike_lo'] = df['spot'] * (1 + min_otm_pct)
    df['strike_hi'] = df['spot'] * (1 + max_otm_pct) if max_otm_pct else float('inf')
    
    # Filter by strike range
    df = df[(df['strike'] >= df['strike_lo']) & (df['strike'] <= df['strike_hi'])].copy()
    
    # Filter by days to expiry if specified
    if days_limit is not None:
        df = df[df['days_to_expiry'] <= days_limit].copy()
    
    # Filter by minimum premium (using close_price as proxy for bid)
    df = df[df['close_price'] >= min_bid].copy()
    
    # Filter by volume
    df = df[df['volume'] >= min_volume].copy()
    
    # Calculate premium yield and filter
    df['premium_yield'] = df['close_price'] / df['spot']
    df = df[df['premium_yield'] >= min_premium_yield].copy()
    
    # Filter by capital limit if specified
    if capital_limit is not None:
        df['capital_required'] = df['spot'] * 100.0
        df = df[df['capital_required'] <= capital_limit].copy()
    
    if len(df) == 0:
        return []
    
    # Calculate additional metrics
    df['breakeven'] = df['spot'] - df['close_price']
    df['max_profit'] = (df['strike'] - df['spot']) + df['close_price']
    
    # Convert to list of dicts (matching apy.py format)
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "ticker": str(row['ticker']),
            "expiration": row['expiration_date'].strftime('%Y-%m-%d') if hasattr(row['expiration_date'], 'strftime') else str(row['expiration_date']),
            "strike": float(row['strike']),
            "delta": None,  # Not available in our data
            "bid": None,    # Not available in our data
            "ask": None,    # Not available in our data
            "mid": float(row['close_price']),  # Using close_price as proxy
            "spread_pct": None,  # Not available without bid/ask
            "open_interest": None,  # Not available in our data
            "volume": int(row['volume']),
            "iv": None,  # Not available in our data
            "spot": float(row['spot']),
            "premium_yield": float(row['premium_yield']),
            "breakeven": float(row['breakeven']),
            "max_profit": float(row['max_profit']),
            "pop_est": None,  # Can't calculate without IV
            "days_to_expiry": float(row['days_to_expiry']),
            "capital_required": float(row['spot'] * 100.0),
        })
    
    # Sort by rank_metric
    if rank_metric == "premium_yield":
        rows.sort(key=lambda r: r["premium_yield"], reverse=True)
    elif rank_metric == "max_profit":
        rows.sort(key=lambda r: r["max_profit"], reverse=True)
    elif rank_metric == "pop_est":
        rows.sort(key=lambda r: (r["pop_est"] or 0), reverse=True)
    else:
        rows.sort(key=lambda r: r["premium_yield"], reverse=True)
    
    return rows


# ============================================================================
# STEP 4: P&L marking and CSV saving (copied from apy.py)
# ============================================================================

def save_csv(rows: List[Dict], outdir: str, symbol: str, expiration_date: str) -> str:
    """Save screened candidates to CSV (matching apy.py format)."""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    path = Path(outdir) / f"{symbol.lower()}_{expiration_date}_0dte_calls.csv"
    df.to_csv(path, index=False)
    return str(path)

def mark_realized_pnl(csv_path: str, underlying_close: float) -> str:
    """
    Compute realized P&L for screened candidates (copied from apy.py).
    
    This function takes a CSV of screened candidates and adds P&L columns
    based on the underlying price at expiration.
    """
    df = pd.read_csv(csv_path)
    S_close = float(underlying_close)
    per_share = []
    assigned = []
    for _, r in df.iterrows():
        K = float(r["strike"])
        S0 = float(r["spot"])  # Price when you sold the call
        c = float(r["mid"])    # Premium collected
        if S_close <= K:
            # Not assigned: keep the premium collected
            p = c
            assigned.append(False)
        else:
            # Assigned: premium + (strike - spot price when call was sold)
            # This represents the gain from selling at strike vs market price
            p = c + (K - S0)
            assigned.append(True)
        per_share.append(p)
    df["assigned"] = assigned
    df["pnl_per_share"] = per_share
    df["pnl_per_contract"] = df["pnl_per_share"] * 100.0
    out = csv_path.replace(".csv", "_marked.csv")
    df.to_csv(out, index=False)
    return out

def calculate_pnl(
    entry_price: float,
    strike: float,
    premium_collected: float,
    expiration_price: float
) -> Tuple[float, bool]:
    """
    Calculate P&L for a covered call strategy.
    
    Args:
        entry_price: Underlying price when call was sold (entry day) in dollars
        strike: Strike price of the call (in cents, e.g., 2300.0 = $23.00)
        premium_collected: Premium received for selling the call
        expiration_price: Underlying price at expiration in dollars
    
    Returns:
        Tuple of (pnl_per_share, was_assigned)
    """
    # Convert strike from cents to dollars (2300.0 -> 23.00)
    strike_dollars = strike / 100.0
    
    if expiration_price <= strike_dollars:
        # Not assigned: keep the premium
        pnl = premium_collected
        assigned = False
    else:
        # Assigned: premium + (strike - entry_price)
        # This is the gain from selling at strike vs entry price
        pnl = premium_collected + (strike_dollars - entry_price)
        assigned = True
    
    return pnl, assigned

def get_underlying_price_at_expiration(
    df: pd.DataFrame,
    symbol: str,
    expiration_date: str
) -> Optional[float]:
    """
    Get underlying stock price at expiration.
    
    First tries to use actual underlying_spot_at_expiry if available (pessimistic/normal),
    then underlying_close_at_expiry, otherwise falls back to looking up expiration day data.
    """
    # First, check if we have the underlying_spot_at_expiry column (from add_underlying_prices_from_csv)
    if 'underlying_spot_at_expiry' in df.columns:
        exp_data = df[df['expiration_date'] == expiration_date]
        if len(exp_data) > 0 and pd.notna(exp_data['underlying_spot_at_expiry'].iloc[0]):
            return float(exp_data['underlying_spot_at_expiry'].iloc[0])
    
    # Check for underlying_close_at_expiry column
    if 'underlying_close_at_expiry' in df.columns:
        exp_data = df[df['expiration_date'] == expiration_date]
        if len(exp_data) > 0 and pd.notna(exp_data['underlying_close_at_expiry'].iloc[0]):
            return float(exp_data['underlying_close_at_expiry'].iloc[0])
    
    # Fall back to looking for data on expiration day
    exp_data = df[
        (df['expiration_date'] == expiration_date) &
        (df['date_only'] == expiration_date)
    ]
    
    if len(exp_data) == 0:
        return None
    
    # If we have underlying_spot for expiration day, use it
    if 'underlying_spot' in exp_data.columns and pd.notna(exp_data['underlying_spot'].iloc[0]):
        return float(exp_data['underlying_spot'].iloc[0])
    
    # If we have underlying_close for expiration day, use it
    if 'underlying_close' in exp_data.columns and pd.notna(exp_data['underlying_close'].iloc[0]):
        return float(exp_data['underlying_close'].iloc[0])
    
    # Last resort: estimate from option price (not accurate)
    # This is a fallback - should ideally have actual stock price data
    return None

def backtest_covered_calls(
    data_file: str,
    symbol: str,
    option_type: str = "call",
    min_volume: int = 0,
    min_premium: float = 0.0,
    max_strike_pct_otm: float = 0.05
) -> pd.DataFrame:
    """
    Backtest covered call strategy on historical options data.
    
    Args:
        data_file: Path to CSV file with options data
        symbol: Underlying symbol (e.g., 'TSLA')
        option_type: 'call' or 'put' (default: 'call')
        min_volume: Minimum volume filter
        min_premium: Minimum premium to collect
        max_strike_pct_otm: Maximum % OTM for strike selection
    
    Returns:
        DataFrame with backtest results
    """
    print(f"📊 Loading data from {data_file}...")
    df = pd.read_csv(data_file)
    
    # Filter to calls only
    if option_type.lower() == "call":
        df = df[df['option_type'] == 'C'].copy()
    else:
        df = df[df['option_type'] == 'P'].copy()
    
    print(f"   Loaded {len(df):,} rows")
    
    # Convert dates
    df['date_only'] = pd.to_datetime(df['date_only']).dt.date
    df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
    
    # Filter by volume and premium
    df = df[df['volume'] >= min_volume].copy()
    df = df[df['close_price'] >= min_premium].copy()
    
    print(f"   After filters: {len(df):,} rows")
    
    # Calculate entry underlying price (approximate from option price)
    # For covered calls, we need the underlying price at entry
    # We'll estimate: underlying ≈ strike + option_price (for ATM/OTM calls)
    # Or use a simpler approach: assume we know the underlying price
    
    results = []
    
    # Group by expiration date to process each expiration
    for exp_date, exp_group in df.groupby('expiration_date'):
        exp_date_str = exp_date.strftime('%Y-%m-%d') if isinstance(exp_date, date) else str(exp_date)
        
        # Get entry data (should be one row per contract for weekly, multiple for monthly)
        for _, row in exp_group.iterrows():
            entry_date = row['date_only']
            strike = float(row['strike'])
            # Option prices are stored in cents, convert to dollars (103.0 -> $1.03)
            premium_collected = float(row['close_price']) / 100.0  # Premium at entry in dollars
            volume = int(row['volume'])
            
            # Use actual underlying price at entry if available (from merged data)
            if 'underlying_spot' in row and pd.notna(row['underlying_spot']):
                entry_underlying = float(row['underlying_spot'])
            elif 'underlying_close' in row and pd.notna(row['underlying_close']):
                entry_underlying = float(row['underlying_close'])
            else:
                # Fallback: estimate underlying price at entry
                entry_underlying = strike + premium_collected  # Rough estimate
                print(f"⚠️  Warning: No underlying price for {row.get('ticker', 'unknown')} on {entry_date}, using estimate")
            
            # Get underlying price at expiration (use actual if available)
            if 'underlying_spot_at_expiry' in row and pd.notna(row['underlying_spot_at_expiry']):
                expiration_price = float(row['underlying_spot_at_expiry'])
            elif 'underlying_close_at_expiry' in row and pd.notna(row['underlying_close_at_expiry']):
                expiration_price = float(row['underlying_close_at_expiry'])
            else:
                # Fallback: try to get from expiration day data
                expiration_price = get_underlying_price_at_expiration(df, symbol, exp_date_str)
            
            if expiration_price is None:
                # If we don't have expiration day data, skip this trade
                continue
            
            # Calculate P&L
            pnl_per_share, assigned = calculate_pnl(
                entry_underlying,
                strike,
                premium_collected,
                expiration_price
            )
            
            # Calculate returns
            premium_yield = premium_collected / entry_underlying if entry_underlying > 0 else 0
            pnl_yield = pnl_per_share / entry_underlying if entry_underlying > 0 else 0
            
            results.append({
                'entry_date': entry_date,
                'expiration_date': exp_date,
                'ticker': row['ticker'],
                'strike': strike,
                'entry_underlying': entry_underlying,
                'expiration_underlying': expiration_price,
                'premium_collected': premium_collected,
                'premium_yield': premium_yield,
                'pnl_per_share': pnl_per_share,
                'pnl_per_contract': pnl_per_share * 100,
                'pnl_yield': pnl_yield,
                'assigned': assigned,
                'volume': volume,
                'days_to_expiry': row['days_to_expiry'],
            })
    
    results_df = pd.DataFrame(results)
    
    if len(results_df) == 0:
        print("⚠️  No valid trades found (missing expiration prices)")
        return pd.DataFrame()
    
    # Calculate summary statistics
    print(f"\n✅ Backtest complete: {len(results_df):,} trades")
    print(f"   Assigned: {results_df['assigned'].sum():,} ({results_df['assigned'].mean()*100:.1f}%)")
    print(f"   Not Assigned: {(~results_df['assigned']).sum():,} ({(~results_df['assigned']).mean()*100:.1f}%)")
    print(f"   Total P&L: ${results_df['pnl_per_contract'].sum():,.2f}")
    print(f"   Average P&L per trade: ${results_df['pnl_per_contract'].mean():.2f}")
    print(f"   Average premium yield: {results_df['premium_yield'].mean()*100:.2f}%")
    print(f"   Win rate: {(results_df['pnl_per_contract'] > 0).mean()*100:.1f}%")
    
    return results_df

def main():
    parser = argparse.ArgumentParser(description="Backtest covered call strategy on historical data")
    parser.add_argument("--data-file", required=True, help="Path to options data CSV file")
    parser.add_argument("--symbol", default="TSLA", help="Underlying symbol")
    parser.add_argument("--output", help="Output CSV file for results")
    parser.add_argument("--min-volume", type=int, default=0, help="Minimum volume filter")
    parser.add_argument("--min-premium", type=float, default=0.0, help="Minimum premium to collect")
    parser.add_argument("--option-type", choices=["call", "put"], default="call", help="Option type")
    
    args = parser.parse_args()
    
    results = backtest_covered_calls(
        data_file=args.data_file,
        symbol=args.symbol,
        option_type=args.option_type,
        min_volume=args.min_volume,
        min_premium=args.min_premium
    )
    
    if len(results) > 0:
        if args.output:
            results.to_csv(args.output, index=False)
            print(f"\n💾 Results saved to {args.output}")
        else:
            print(f"\n📊 Results preview:")
            print(results.head(10).to_string())
    else:
        print("❌ No results to save")

if __name__ == "__main__":
    main()

