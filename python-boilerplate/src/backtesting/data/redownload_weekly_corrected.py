#!/usr/bin/env python3
"""
Redownload and reorganize weekly options data with correct filtering:
- Thursday entries only (days_to_expiry=8 for weekly options)
- Process only stocks used in ITM hit rate calculations
- Organize into data/TICKER/weekly/ folders
- Match with historical stock prices automatically
"""

import os
import sys
import duckdb
import time
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
import importlib.util

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Stocks to process (from ITM hit rate calculations)
STOCKS_TO_PROCESS = [
    'AAPL', 'AMZN', 'GOOG', 'HOOD', 'IWM', 'JPM', 'META', 
    'MSFT', 'NVDA', 'QQQ', 'TSLA', 'XLE', 'XLF', 'XLK'
]

# Years to process
YEARS_TO_PROCESS = list(range(2016, 2026))  # 2016-2025

print("=" * 80)
print("REDOWNLOADING WEEKLY OPTIONS DATA (THURSDAY ENTRIES, 8 DAYS TO EXPIRY)")
print("=" * 80)
print(f"Stocks to process: {', '.join(STOCKS_TO_PROCESS)}")
print(f"Years: {YEARS_TO_PROCESS[0]}-{YEARS_TO_PROCESS[-1]}")
print()

# ------------------------------------------------------------
# 1. Load environment variables
# ------------------------------------------------------------

print("📋 Loading environment variables...", flush=True)
load_dotenv()

S3_ACCESS_KEY = os.getenv("MASSIVE_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("MASSIVE_API_KEY")
S3_ENDPOINT   = os.getenv("MASSIVE_S3_ENDPOINT")
S3_BUCKET     = os.getenv("MASSIVE_S3_BUCKET")
S3_REGION     = os.getenv("MASSIVE_S3_REGION", "us-east-1")

missing = [
    name for name, value in {
        "MASSIVE_S3_ACCESS_KEY": S3_ACCESS_KEY,
        "MASSIVE_S3_SECRET_KEY": S3_SECRET_KEY,
        "MASSIVE_S3_ENDPOINT": S3_ENDPOINT,
        "MASSIVE_S3_BUCKET": S3_BUCKET,
    }.items()
    if not value
]

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

print("✅ Environment variables loaded", flush=True)

# ------------------------------------------------------------
# 2. Connect to DuckDB
# ------------------------------------------------------------

print("\n🔌 Connecting to DuckDB...", flush=True)
DB_PATH = Path(__file__).parent / "options.duckdb"

try:
    con = duckdb.connect(str(DB_PATH))
    print(f"✅ DuckDB connected", flush=True)
except Exception as e:
    print(f"❌ Error connecting to DuckDB: {e}", flush=True)
    raise

# ------------------------------------------------------------
# 3. Enable S3 support
# ------------------------------------------------------------

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

endpoint_clean = S3_ENDPOINT.replace('https://', '').replace('http://', '')
con.execute(f"""
SET s3_endpoint='{endpoint_clean}';
SET s3_access_key_id='{S3_ACCESS_KEY}';
SET s3_secret_access_key='{S3_SECRET_KEY}';
SET s3_region='{S3_REGION}';
SET s3_use_ssl=true;
SET s3_url_style='path';
""")

print("✅ DuckDB S3 configuration set", flush=True)

# ------------------------------------------------------------
# 4. Import monthly.py functions for stock price matching
# ------------------------------------------------------------

monthly_path = Path(__file__).parent.parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

# Import add_premium_columns
premium_path = Path(__file__).parent.parent / "add_premium_columns.py"
spec_premium = importlib.util.spec_from_file_location("add_premium_columns", premium_path)
premium_module = importlib.util.module_from_spec(spec_premium)
spec_premium.loader.exec_module(premium_module)

load_historical_stock_prices = monthly.load_historical_stock_prices
add_underlying_prices_from_csv = monthly.add_underlying_prices_from_csv
add_premium_columns = premium_module.add_premium_columns

# ------------------------------------------------------------
# 5. Helper function to parse option ticker
# ------------------------------------------------------------

def parse_option_ticker(ticker: str):
    """Parse option ticker to extract symbol, expiration, type, and strike."""
    if not ticker.startswith('O:'):
        return None
    ticker = ticker[2:]
    symbol_end = 0
    for i, char in enumerate(ticker):
        if char.isdigit():
            symbol_end = i
            break
    if symbol_end == 0:
        return None
    symbol = ticker[:symbol_end]
    remaining = ticker[symbol_end:]
    if len(remaining) < 7:
        return None
    expiration_str = remaining[:6]
    option_type = remaining[6]
    strike_str = remaining[7:]
    try:
        year = 2000 + int(expiration_str[:2])
        month = int(expiration_str[2:4])
        day = int(expiration_str[4:6])
        expiration_date = date(year, month, day)
        strike = float(strike_str) / 100.0
        return {
            'symbol': symbol,
            'expiration_date': expiration_date,
            'option_type': option_type,
            'strike': strike
        }
    except (ValueError, IndexError):
        return None

# ------------------------------------------------------------
# 6. Process each year
# ------------------------------------------------------------

script_start_time = time.time()

for TEST_YEAR in YEARS_TO_PROCESS:
    year_start_time = time.time()
    print("\n" + "=" * 80)
    print(f"PROCESSING YEAR: {TEST_YEAR}")
    print("=" * 80)
    
    # Check if files already exist for all stocks
    all_exist = True
    for ticker in STOCKS_TO_PROCESS:
        output_file = Path(f"../../data/{ticker}/weekly/{TEST_YEAR}_options_pessimistic.csv")
        if not output_file.exists():
            all_exist = False
            break
    
    if all_exist:
        print(f"⏭️  Skipping {TEST_YEAR} - all files already exist")
        continue
    
    # ------------------------------------------------------------
    # 6.1. Aggregate data from S3
    # ------------------------------------------------------------
    
    table_name = f"options_day_aggs_{TEST_YEAR}"
    
    print(f"\n📥 Step 1: Aggregating data from S3 for {TEST_YEAR}...")
    
    # Check if table already exists
    table_exists = False
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        if result and result[0] > 0:
            table_exists = True
            print(f"   ✅ Table {table_name} already exists with {result[0]:,} rows")
    except:
        pass
    
    if not table_exists:
        print(f"   🔍 Finding files for {TEST_YEAR}...")
        all_files = con.execute(f"""
        SELECT file 
        FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/*/*.csv.gz')
        ORDER BY file;
        """).fetchall()
        
        if not all_files:
            print(f"   ⚠️  No files found for {TEST_YEAR}, skipping...")
            continue
        
        print(f"   ✅ Found {len(all_files)} files")
        
        # Build WHERE clause for ticker filtering
        ticker_conditions = " OR ".join([f"ticker LIKE 'O:{ticker}%'" for ticker in STOCKS_TO_PROCESS])
        where_clause = f"WHERE ({ticker_conditions})"
        
        print(f"   ⏳ Reading and aggregating files (filtering for {len(STOCKS_TO_PROCESS)} tickers)...")
        start_time = time.time()
        
        file_paths = [f"'{row[0]}'" for row in all_files]
        file_list = ',\n        '.join(file_paths)
        
        con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *
        FROM read_csv_auto([
            {file_list}
        ], compression='gzip')
        {where_clause};
        """)
        
        elapsed = time.time() - start_time
        print(f"   ✅ Finished reading files (took {elapsed:.1f} seconds / {elapsed/60:.1f} minutes)")
    
    # ------------------------------------------------------------
    # 6.2. Process and filter data
    # ------------------------------------------------------------
    
    print(f"\n🔧 Step 2: Processing and filtering data...")
    
    # Load data
    df = con.execute(f"SELECT * FROM {table_name}").df()
    
    if len(df) == 0:
        print(f"   ⚠️  No data for {TEST_YEAR}, skipping...")
        continue
    
    print(f"   Loaded {len(df):,} rows")
    
    # Parse tickers
    print(f"   Parsing option tickers...")
    parsed = df['ticker'].apply(parse_option_ticker)
    df['underlying_symbol'] = parsed.apply(lambda x: x['symbol'] if x else None)
    df['expiration_date'] = parsed.apply(lambda x: x['expiration_date'] if x else None)
    df['option_type'] = parsed.apply(lambda x: x['option_type'] if x else None)
    df['strike'] = parsed.apply(lambda x: x['strike'] if x else None)
    
    # Convert dates
    df['date_only'] = pd.to_datetime(df['window_start'] / 1_000_000_000, unit='s').dt.date
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    # Calculate days_to_expiry
    df['days_to_expiry'] = (df['expiration_date'] - pd.to_datetime(df['date_only'])).dt.days
    
    # Categorize expiration type
    def categorize_expiration_preliminary(row):
        exp_date = row['expiration_date']
        first_day = exp_date.replace(day=1)
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day.replace(day=1 + days_until_friday)
        third_friday = first_friday.replace(day=first_friday.day + 14)
        
        if exp_date.date() == third_friday.date():
            return 'Monthly'
        thursday_before = third_friday - pd.Timedelta(days=1)
        if exp_date.date() == thursday_before.date() and exp_date.dayofweek == 3:
            return 'Monthly'
        if exp_date.dayofweek == 4:
            return 'Weekly_Candidate'
        return 'Other'
    
    df['expiration_category'] = df.apply(categorize_expiration_preliminary, axis=1)
    
    # Distinguish Weekly from Long-term
    weekly_candidates = df[df['expiration_category'] == 'Weekly_Candidate'].copy()
    if len(weekly_candidates) > 0:
        max_dte_by_exp = weekly_candidates.groupby('expiration_date')['days_to_expiry'].max().reset_index()
        max_dte_by_exp.columns = ['expiration_date', 'max_days_to_expiry']
        
        WEEKLY_MAX_DTE_THRESHOLD = 120
        weekly_exp_dates = set(max_dte_by_exp[max_dte_by_exp['max_days_to_expiry'] < WEEKLY_MAX_DTE_THRESHOLD]['expiration_date'])
        longterm_exp_dates = set(max_dte_by_exp[max_dte_by_exp['max_days_to_expiry'] >= WEEKLY_MAX_DTE_THRESHOLD]['expiration_date'])
        
        df.loc[df['expiration_date'].isin(weekly_exp_dates), 'expiration_category'] = 'Weekly'
        df.loc[df['expiration_date'].isin(longterm_exp_dates), 'expiration_category'] = 'Long-term'
    
    # Filter to Weekly only
    df_weekly = df[df['expiration_category'] == 'Weekly'].copy()
    
    if len(df_weekly) == 0:
        print(f"   ⚠️  No weekly options for {TEST_YEAR}, skipping...")
        continue
    
    print(f"   Found {len(df_weekly):,} weekly options")
    
    # Filter to Thursday entries with days_to_expiry=8
    print(f"   Filtering to Thursday entries (days_to_expiry=8)...")
    df_weekly['date_only_dt'] = pd.to_datetime(df_weekly['date_only'])
    df_weekly['day_of_week'] = df_weekly['date_only_dt'].dt.dayofweek  # Monday=0, Thursday=3
    
    # Filter to ONLY Thursday entries (days_to_expiry=8)
    weekly_entry = df_weekly[
        (df_weekly['days_to_expiry'] == 8) &   # 8 days to expiry (Thursday to next Friday)
        (df_weekly['day_of_week'] == 3)        # Must be a Thursday (day_of_week=3)
    ].copy()
    
    weekly_entry = weekly_entry.drop(columns=['date_only_dt', 'day_of_week'])
    
    print(f"   ✅ Filtered to {len(weekly_entry):,} Thursday entries (days_to_expiry=8)")
    
    if len(weekly_entry) == 0:
        print(f"   ⚠️  No Thursday entries found for {TEST_YEAR}, skipping...")
        continue
    
    # ------------------------------------------------------------
    # 6.3. Split by ticker and save
    # ------------------------------------------------------------
    
    print(f"\n💾 Step 3: Splitting by ticker and saving...")
    
    for ticker in STOCKS_TO_PROCESS:
        ticker_data = weekly_entry[weekly_entry['underlying_symbol'] == ticker].copy()
        
        if len(ticker_data) == 0:
            continue
        
        # Prepare output directory (relative to python-boilerplate root)
        base_path = Path(__file__).parent.parent.parent.parent  # Go up to python-boilerplate
        output_dir = base_path / "data" / ticker / "weekly"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{TEST_YEAR}_options_pessimistic.csv"
        
        # Select essential columns
        essential_columns = [
            'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
            'strike', 'volume', 'open_price', 'close_price', 'high_price', 'low_price', 
            'transactions', 'window_start', 'days_to_expiry'
        ]
        
        ticker_data_clean = ticker_data[essential_columns].copy()
        
        # Match with historical stock prices
        stock_file = base_path / "data" / ticker / f"HistoricalData_{ticker}.csv"
        if not stock_file.exists():
            # Try alternative naming
            stock_files = list((base_path / "data" / ticker).glob("HistoricalData*.csv"))
            if stock_files:
                stock_file = stock_files[0]
            else:
                print(f"   ⚠️  No historical stock file found for {ticker}, skipping price matching")
                ticker_data_clean.to_csv(output_file, index=False)
                print(f"   ✅ Saved {output_file} ({len(ticker_data_clean):,} rows, no stock prices)")
                continue
        
        print(f"   📈 Matching {ticker} with historical stock prices...")
        try:
            # Add underlying prices
            ticker_data_with_prices = add_underlying_prices_from_csv(
                ticker_data_clean,
                str(stock_file),
                symbol=ticker,
                use_pessimistic=True
            )
            
            # Add premium columns
            ticker_data_final = add_premium_columns(ticker_data_with_prices)
            
            # Save
            ticker_data_final.to_csv(output_file, index=False)
            print(f"   ✅ Saved {output_file} ({len(ticker_data_final):,} rows)")
        except Exception as e:
            print(f"   ⚠️  Error processing {ticker}: {e}")
            # Save without prices as fallback
            ticker_data_clean.to_csv(output_file, index=False)
            print(f"   ✅ Saved {output_file} ({len(ticker_data_clean):,} rows, without stock prices)")
    
    year_elapsed = time.time() - year_start_time
    print(f"\n✅ Completed {TEST_YEAR} in {year_elapsed:.1f} seconds ({year_elapsed/60:.1f} minutes)")

total_elapsed = time.time() - script_start_time
print("\n" + "=" * 80)
print("🎉 ALL PROCESSING COMPLETE!")
print("=" * 80)
print(f"Total time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes / {total_elapsed/3600:.1f} hours)")

