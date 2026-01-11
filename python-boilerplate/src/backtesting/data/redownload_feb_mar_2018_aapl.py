#!/usr/bin/env python3
"""
Redownload February and March 2018 for AAPL with all call options:
- Thursday entries only (days_to_expiry=8)
- Include ALL call options (no ITM probability filter)
- DO NOT exclude third Fridays (include all Fridays)
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

TEST_YEAR = 2018
TICKER = 'AAPL'

print("=" * 80)
print(f"REDOWNLOADING {TICKER} FEBRUARY & MARCH {TEST_YEAR}")
print("(Thursday entries, days_to_expiry=8, ALL call options, include third Fridays)")
print("=" * 80)

# ------------------------------------------------------------
# 1. Load environment variables
# ------------------------------------------------------------

print("\n📋 Loading environment variables...", flush=True)
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
# Use a temporary database to avoid lock conflicts
DB_PATH = Path(__file__).parent / "options_temp_feb_mar.duckdb"

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
# 4. Import functions for stock price matching
# ------------------------------------------------------------

monthly_path = Path(__file__).parent.parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

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
# 6. Aggregate data from S3
# ------------------------------------------------------------

print(f"\n📥 Step 1: Aggregating data from S3 for {TEST_YEAR}...")

table_name = f"options_day_aggs_{TEST_YEAR}_feb_mar"

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
    print(f"   🔍 Finding files for {TEST_YEAR} (February and March only)...")
    
    # Get files for February and March only
    all_files = con.execute(f"""
    SELECT file 
    FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/02/*.csv.gz')
    ORDER BY file;
    """).fetchall()
    
    mar_files = con.execute(f"""
    SELECT file 
    FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/03/*.csv.gz')
    ORDER BY file;
    """).fetchall()
    
    all_files.extend(mar_files)
    
    if not all_files:
        raise RuntimeError(f"❌ No files found for {TEST_YEAR} February/March")
    
    print(f"   ✅ Found {len(all_files)} files")
    
    # Build WHERE clause for AAPL only
    where_clause = f"WHERE ticker LIKE 'O:{TICKER}%'"
    
    print(f"   ⏳ Reading and aggregating files (filtering for {TICKER})...")
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
    print(f"   ✅ Finished reading files (took {elapsed:.1f} seconds)")

# ------------------------------------------------------------
# 7. Process and filter data
# ------------------------------------------------------------

print(f"\n🔧 Step 2: Processing and filtering data...")

# Load data
df = con.execute(f"SELECT * FROM {table_name}").df()

if len(df) == 0:
    raise RuntimeError(f"❌ No data for {TEST_YEAR} February/March")

print(f"   Loaded {len(df):,} rows")

# Parse tickers
print(f"   Parsing option tickers...")
parsed = df['ticker'].apply(parse_option_ticker)
df['underlying_symbol'] = parsed.apply(lambda x: x['symbol'] if x else None)
df['expiration_date'] = parsed.apply(lambda x: x['expiration_date'] if x else None)
df['option_type'] = parsed.apply(lambda x: x['option_type'] if x else None)
df['strike'] = parsed.apply(lambda x: x['strike'] if x else None)

# Filter to CALL options only
df = df[df['option_type'] == 'C'].copy()
print(f"   Filtered to calls: {len(df):,} rows")

# Convert dates
df['date_only'] = pd.to_datetime(df['window_start'] / 1_000_000_000, unit='s').dt.date
df['expiration_date'] = pd.to_datetime(df['expiration_date'])

# Filter to February and March only
df = df[
    (df['date_only'] >= date(2018, 2, 1)) & 
    (df['date_only'] <= date(2018, 3, 31))
].copy()

print(f"   Filtered to Feb-Mar 2018: {len(df):,} rows")

# Calculate days_to_expiry
df['days_to_expiry'] = (df['expiration_date'] - pd.to_datetime(df['date_only'])).dt.days

# Filter to Thursday entries with days_to_expiry=8
print(f"   Filtering to Thursday entries (days_to_expiry=8)...")
df['date_only_dt'] = pd.to_datetime(df['date_only'])
df['day_of_week'] = df['date_only_dt'].dt.dayofweek  # Monday=0, Thursday=3

# Filter to ONLY Thursday entries (days_to_expiry=8)
weekly_entry = df[
    (df['days_to_expiry'] == 8) &   # 8 days to expiry (Thursday to next Friday)
    (df['day_of_week'] == 3)        # Must be a Thursday (day_of_week=3)
].copy()

weekly_entry = weekly_entry.drop(columns=['date_only_dt', 'day_of_week'])

print(f"   ✅ Filtered to {len(weekly_entry):,} Thursday entries (days_to_expiry=8)")

if len(weekly_entry) == 0:
    raise RuntimeError("❌ No Thursday entries found!")

# Rename columns to match expected names
weekly_entry = weekly_entry.rename(columns={
    'open': 'open_price',
    'close': 'close_price',
    'high': 'high_price',
    'low': 'low_price'
})

# Check expiration dates
unique_expirations = sorted(weekly_entry['expiration_date'].dt.date.unique())
print(f"\n   Expiration dates found: {len(unique_expirations)}")
for exp in unique_expirations:
    dt = datetime.combine(exp, datetime.min.time())
    day_name = dt.strftime('%A')
    print(f"     {exp} ({day_name})")

# ------------------------------------------------------------
# 8. Save data
# ------------------------------------------------------------

print(f"\n💾 Step 3: Saving data...")

# Prepare output directory
base_path = Path(__file__).parent.parent.parent.parent
output_dir = base_path / "data" / TICKER / "weekly"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"{TEST_YEAR}_options_pessimistic_feb_mar.csv"

# Select essential columns
essential_columns = [
    'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
    'strike', 'volume', 'open_price', 'close_price', 'high_price', 'low_price', 
    'transactions', 'window_start', 'days_to_expiry'
]

ticker_data_clean = weekly_entry[essential_columns].copy()

# Match with historical stock prices
stock_file = base_path / "data" / TICKER / f"HistoricalData_{TICKER}.csv"
if not stock_file.exists():
    stock_files = list((base_path / "data" / TICKER).glob("HistoricalData*.csv"))
    if stock_files:
        stock_file = stock_files[0]

if stock_file.exists():
    print(f"   📈 Matching with historical stock prices...")
    try:
        ticker_data_with_prices = add_underlying_prices_from_csv(
            ticker_data_clean,
            str(stock_file),
            symbol=TICKER,
            use_pessimistic=True
        )
        
        ticker_data_final = add_premium_columns(ticker_data_with_prices)
        
        ticker_data_final.to_csv(output_file, index=False)
        print(f"   ✅ Saved {output_file} ({len(ticker_data_final):,} rows)")
    except Exception as e:
        print(f"   ⚠️  Error processing: {e}")
        ticker_data_clean.to_csv(output_file, index=False)
        print(f"   ✅ Saved {output_file} ({len(ticker_data_clean):,} rows, without stock prices)")
else:
    ticker_data_clean.to_csv(output_file, index=False)
    print(f"   ✅ Saved {output_file} ({len(ticker_data_clean):,} rows, no stock prices)")

print("\n" + "=" * 80)
print("✅ COMPLETE!")
print("=" * 80)
print(f"Output file: {output_file}")
print(f"Total rows: {len(weekly_entry):,}")
print(f"Unique trading dates: {len(weekly_entry['date_only'].unique())}")
print(f"Unique expiration dates: {len(unique_expirations)}")

