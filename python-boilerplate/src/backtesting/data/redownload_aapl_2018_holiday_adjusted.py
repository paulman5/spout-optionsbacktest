#!/usr/bin/env python3
"""
Redownload full year 2018 for AAPL with all call options:
- Normal weeks: Monday entries (days_to_expiry=4, Monday to Friday)
- Friday holiday weeks: Monday entries (days_to_expiry=3, Monday to Thursday)
- Monday holiday weeks: Tuesday entries (days_to_expiry=3, Tuesday to Friday)
- Include ALL call options (no ITM probability filter)
"""

import os
import sys
import duckdb
import time
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv
import importlib.util

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

TEST_YEAR = 2018
TICKER = 'AAPL'

# Market holidays in 2018
MONDAY_HOLIDAYS_2018 = [
    date(2018, 1, 1),   # New Year's Day
    date(2018, 1, 15),  # Martin Luther King Jr. Day
    date(2018, 2, 19),  # Presidents' Day
    date(2018, 5, 28),  # Memorial Day
    date(2018, 9, 3),   # Labor Day
]

FRIDAY_HOLIDAYS_2018 = [
    date(2018, 3, 30),  # Good Friday
]

print("=" * 80)
print(f"REDOWNLOADING {TICKER} FULL YEAR {TEST_YEAR}")
print("(Monday-Friday normal, Monday-Thursday if Friday holiday, Tuesday-Friday if Monday holiday)")
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
DB_PATH = Path(__file__).parent / "options_temp_2018_monday.duckdb"  # Reuse existing table

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
        strike = float(strike_str) / 1000.0
        return {
            'symbol': symbol,
            'expiration_date': expiration_date,
            'option_type': option_type,
            'strike': strike
        }
    except (ValueError, IndexError):
        return None

# ------------------------------------------------------------
# 6. Load existing data
# ------------------------------------------------------------

print(f"\n📥 Step 1: Loading data from existing table...")

table_name = f"options_day_aggs_{TEST_YEAR}_monday"

# Load data
df = con.execute(f"SELECT * FROM {table_name}").df()

if len(df) == 0:
    raise RuntimeError(f"❌ No data for {TEST_YEAR}")

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

# Filter to 2018 only
df = df[
    (df['date_only'] >= date(2018, 1, 1)) & 
    (df['date_only'] <= date(2018, 12, 31))
].copy()

print(f"   Filtered to 2018: {len(df):,} rows")

# Calculate days_to_expiry
df['date_only_dt'] = pd.to_datetime(df['date_only'])
df['days_to_expiry'] = (df['expiration_date'] - df['date_only_dt']).dt.days
df['day_of_week'] = df['date_only_dt'].dt.dayofweek  # Monday=0, Tuesday=1, Friday=4

# ------------------------------------------------------------
# 7. Get normal Monday-Friday entries
# ------------------------------------------------------------

print(f"\n🔧 Step 2: Processing data...")

# Normal weeks: Monday entries with days_to_expiry=4 (Monday to Friday)
print(f"   Filtering to normal Monday-Friday entries (days_to_expiry=4)...")
monday_friday = df[
    (df['days_to_expiry'] == 4) &
    (df['day_of_week'] == 0)  # Monday
].copy()

# Check which ones are actually normal (Friday not a holiday)
monday_friday_normal = []
for _, row in monday_friday.iterrows():
    entry_date = row['date_only']
    expiration_date = row['expiration_date'].date()
    
    # Check if expiration Friday is a holiday
    if expiration_date not in FRIDAY_HOLIDAYS_2018:
        monday_friday_normal.append(row)

monday_friday_normal_df = pd.DataFrame(monday_friday_normal)
print(f"   ✅ Found {len(monday_friday_normal_df):,} normal Monday-Friday entries")

# ------------------------------------------------------------
# 8. Get Monday-Thursday entries (Friday holiday weeks)
# ------------------------------------------------------------

print(f"   Filtering to Monday-Thursday entries (days_to_expiry=3) for Friday holiday weeks...")
monday_thursday = df[
    (df['days_to_expiry'] == 3) &
    (df['day_of_week'] == 0)  # Monday
].copy()

# Filter to only those where Friday would be a holiday
monday_thursday_holiday = []
for _, row in monday_thursday.iterrows():
    entry_date = row['date_only']
    expiration_date = row['expiration_date'].date()
    
    # Check if the Friday of that week (Monday + 4 days) is a holiday
    friday_of_week = entry_date + timedelta(days=4)
    
    if friday_of_week in FRIDAY_HOLIDAYS_2018:
        monday_thursday_holiday.append(row)

monday_thursday_holiday_df = pd.DataFrame(monday_thursday_holiday)
print(f"   ✅ Found {len(monday_thursday_holiday_df):,} Monday-Thursday entries for Friday holiday weeks")

# ------------------------------------------------------------
# 9. Get Tuesday-Friday entries (Monday holiday weeks)
# ------------------------------------------------------------

print(f"   Filtering to Tuesday-Friday entries (days_to_expiry=3) for Monday holiday weeks...")
tuesday_friday = df[
    (df['days_to_expiry'] == 3) &
    (df['day_of_week'] == 1)  # Tuesday
].copy()

# Filter to only those where Monday was a holiday
tuesday_friday_holiday = []
for _, row in tuesday_friday.iterrows():
    entry_date = row['date_only']
    expiration_date = row['expiration_date'].date()
    
    # Check if the Monday before this Tuesday is a holiday
    monday_before = entry_date - timedelta(days=1)
    
    if monday_before in MONDAY_HOLIDAYS_2018:
        tuesday_friday_holiday.append(row)

tuesday_friday_holiday_df = pd.DataFrame(tuesday_friday_holiday)
print(f"   ✅ Found {len(tuesday_friday_holiday_df):,} Tuesday-Friday entries for Monday holiday weeks")

# ------------------------------------------------------------
# 10. Combine all entries
# ------------------------------------------------------------

print(f"\n📊 Step 3: Combining entries...")

# Combine all entry types
all_entries_list = []
if len(monday_friday_normal_df) > 0:
    all_entries_list.append(monday_friday_normal_df)
if len(monday_thursday_holiday_df) > 0:
    all_entries_list.append(monday_thursday_holiday_df)
if len(tuesday_friday_holiday_df) > 0:
    all_entries_list.append(tuesday_friday_holiday_df)

if all_entries_list:
    all_entries = pd.concat(all_entries_list, ignore_index=True)
else:
    all_entries = pd.DataFrame()

# Drop temporary columns
all_entries = all_entries.drop(columns=['date_only_dt', 'day_of_week'], errors='ignore')

print(f"   ✅ Total entries: {len(all_entries):,}")
print(f"      - Normal Monday-Friday: {len(monday_friday_normal_df):,}")
print(f"      - Monday-Thursday (Friday holiday): {len(monday_thursday_holiday_df):,}")
print(f"      - Tuesday-Friday (Monday holiday): {len(tuesday_friday_holiday_df):,}")

# Rename columns to match expected names
all_entries = all_entries.rename(columns={
    'open': 'open_price',
    'close': 'close_price',
    'high': 'high_price',
    'low': 'low_price'
})

# Check unique dates
unique_trading_dates = sorted(all_entries['date_only'].unique())
unique_expirations = sorted(all_entries['expiration_date'].dt.date.unique())
print(f"\n   Trading dates found: {len(unique_trading_dates)}")
print(f"   Expiration dates found: {len(unique_expirations)}")

# ------------------------------------------------------------
# 11. Save data
# ------------------------------------------------------------

print(f"\n💾 Step 4: Saving data...")

# Prepare output directory
base_path = Path(__file__).parent.parent.parent.parent
output_dir = base_path / "data" / TICKER / "weekly"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"{TEST_YEAR}_options_pessimistic_holiday_adjusted.csv"

# Select essential columns
essential_columns = [
    'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
    'strike', 'volume', 'open_price', 'close_price', 'high_price', 'low_price', 
    'transactions', 'window_start', 'days_to_expiry'
]

ticker_data_clean = all_entries[essential_columns].copy()

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
print(f"Total rows: {len(all_entries):,}")
print(f"  - Normal Monday-Friday: {len(monday_friday_normal_df):,}")
print(f"  - Monday-Thursday (Friday holiday): {len(monday_thursday_holiday_df):,}")
print(f"  - Tuesday-Friday (Monday holiday): {len(tuesday_friday_holiday_df):,}")
print(f"Unique trading dates: {len(unique_trading_dates)}")
print(f"Unique expiration dates: {len(unique_expirations)}")

