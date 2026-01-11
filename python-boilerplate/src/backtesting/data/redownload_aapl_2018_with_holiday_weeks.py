#!/usr/bin/env python3
"""
Redownload full year 2018 for AAPL with all call options:
- Monday entries (days_to_expiry=4, Monday to Friday) - normal weeks
- Tuesday entries (days_to_expiry=6, Tuesday to next Monday) - for holiday weeks where Friday is closed
- Include ALL call options (no ITM probability filter)
- DO NOT exclude any weeks
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

# Market holidays in 2018 (Fridays that are holidays)
FRIDAY_HOLIDAYS_2018 = [
    date(2018, 3, 30),  # Good Friday
]

print("=" * 80)
print(f"REDOWNLOADING {TICKER} FULL YEAR {TEST_YEAR}")
print("(Monday entries + Tuesday entries for holiday weeks)")
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
# 7. Get Monday entries (normal weeks)
# ------------------------------------------------------------

print(f"\n🔧 Step 2: Processing data...")

# Filter to Monday entries with days_to_expiry=4 (Monday to Friday)
print(f"   Filtering to Monday entries (days_to_expiry=4)...")
monday_entries = df[
    (df['days_to_expiry'] == 4) &   # 4 days to expiry (Monday entry, Friday expiration)
    (df['day_of_week'] == 0)        # Must be a Monday (day_of_week=0)
].copy()

print(f"   ✅ Found {len(monday_entries):,} Monday entries")

# ------------------------------------------------------------
# 8. Get Tuesday entries for holiday weeks
# ------------------------------------------------------------

print(f"   Checking for Tuesday entries (days_to_expiry=6) for holiday weeks...")

# Check what Tuesday entries exist
tuesday_df = df[df['day_of_week'] == 1].copy()
if len(tuesday_df) > 0:
    dte_counts = tuesday_df['days_to_expiry'].value_counts().sort_index()
    print(f"   Days to expiry distribution for Tuesdays:")
    for dte, count in dte_counts.head(10).items():
        print(f"     {int(dte)} days: {count:,} rows")
    if len(dte_counts) > 10:
        print(f"     ... and {len(dte_counts) - 10} more values")

# Filter to Tuesday entries that expire on Monday (regardless of days_to_expiry)
# First, add expiration day of week
df['expiration_day_of_week'] = df['expiration_date'].dt.dayofweek

# Get all Tuesday entries
tuesday_all = df[df['day_of_week'] == 1].copy()

# Filter to those that expire on Monday (day_of_week=0 for expiration)
tuesday_entries = tuesday_all[tuesday_all['expiration_day_of_week'] == 0].copy()

print(f"   ✅ Found {len(tuesday_entries):,} Tuesday entries that expire on Monday")

# Check days_to_expiry for Tuesday-Monday entries
if len(tuesday_entries) > 0:
    tuesday_entries['days_to_expiry_calc'] = (tuesday_entries['expiration_date'] - tuesday_entries['date_only_dt']).dt.days
    dte_counts = tuesday_entries['days_to_expiry_calc'].value_counts().sort_index()
    print(f"   Days to expiry for Tuesday-Monday entries:")
    for dte, count in dte_counts.head(5).items():
        print(f"     {int(dte)} days: {count:,} rows")

# Filter Tuesday entries to only those that correspond to holiday weeks
# (where the Monday of that week would have expired on a Friday holiday)
tuesday_entries_filtered = []
for _, row in tuesday_entries.iterrows():
    entry_date = row['date_only']
    expiration_date = row['expiration_date'].date()
    
    # Check if this Tuesday entry is for a week where Friday would be a holiday
    # The Monday before this Tuesday would expire on Friday
    monday_before = entry_date - timedelta(days=1)  # Monday before Tuesday
    
    # Check if the Friday of that week (Monday + 4 days) is a holiday
    friday_of_week = monday_before + timedelta(days=4)
    
    if friday_of_week in FRIDAY_HOLIDAYS_2018:
        tuesday_entries_filtered.append(row)

if tuesday_entries_filtered:
    tuesday_entries_holiday = pd.DataFrame(tuesday_entries_filtered)
    print(f"   ✅ Found {len(tuesday_entries_holiday):,} Tuesday entries for holiday weeks")
    # Drop the temporary column
    tuesday_entries_holiday = tuesday_entries_holiday.drop(columns=['expiration_day_of_week', 'days_to_expiry_calc'], errors='ignore')
else:
    tuesday_entries_holiday = pd.DataFrame()
    print(f"   ⚠️  No Tuesday entries found for holiday weeks")

# ------------------------------------------------------------
# 9. Combine both sets
# ------------------------------------------------------------

print(f"\n📊 Step 3: Combining entries...")

# Combine Monday and Tuesday entries
all_entries = pd.concat([monday_entries, tuesday_entries_holiday], ignore_index=True)

# Drop temporary columns
all_entries = all_entries.drop(columns=['date_only_dt', 'day_of_week', 'expiration_day_of_week'], errors='ignore')

print(f"   ✅ Total entries: {len(all_entries):,}")
print(f"      - Monday entries: {len(monday_entries):,}")
print(f"      - Tuesday entries (holiday weeks): {len(tuesday_entries_holiday):,}")

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
# 10. Save data
# ------------------------------------------------------------

print(f"\n💾 Step 4: Saving data...")

# Prepare output directory
base_path = Path(__file__).parent.parent.parent.parent
output_dir = base_path / "data" / TICKER / "weekly"
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"{TEST_YEAR}_options_pessimistic_with_holidays.csv"

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
print(f"  - Monday entries: {len(monday_entries):,}")
print(f"  - Tuesday entries (holiday weeks): {len(tuesday_entries_holiday):,}")
print(f"Unique trading dates: {len(unique_trading_dates)}")
print(f"Unique expiration dates: {len(unique_expirations)}")

