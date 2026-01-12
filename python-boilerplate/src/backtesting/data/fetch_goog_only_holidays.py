#!/usr/bin/env python3
"""
Fetch GOOG (not GOOGL) weekly options with holiday adjustments for holidays folder:
- Filter to only GOOG (exclude GOOGL)
- Normal weeks: Monday entries (days_to_expiry=4, Monday to Friday)
- Friday holiday weeks: Monday entries (days_to_expiry=3, Monday to Thursday)
- Monday holiday weeks: Tuesday entries (days_to_expiry=3, Tuesday to Friday)
- Replace existing holidays data
"""

import os
import sys
import duckdb
import time
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv
import importlib.util
import re

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

TICKER = 'GOOG'
YEARS = list(range(2016, 2026))  # 2016-2025
CUTOFF_DATE = date(2020, 8, 31)  # Strike price division changes on this date

# Market holidays by year
MARKET_HOLIDAYS = {
    2016: [
        date(2016, 1, 1), date(2016, 1, 18), date(2016, 2, 15), date(2016, 3, 25),
        date(2016, 5, 30), date(2016, 7, 4), date(2016, 9, 5), date(2016, 11, 24),
        date(2016, 12, 26),
    ],
    2017: [
        date(2017, 1, 2), date(2017, 1, 16), date(2017, 2, 20), date(2017, 4, 14),
        date(2017, 5, 29), date(2017, 7, 4), date(2017, 9, 4), date(2017, 11, 23),
        date(2017, 12, 25),
    ],
    2018: [
        date(2018, 1, 1), date(2018, 1, 15), date(2018, 2, 19), date(2018, 3, 30),
        date(2018, 5, 28), date(2018, 7, 4), date(2018, 9, 3), date(2018, 11, 22),
        date(2018, 12, 25),
    ],
    2019: [
        date(2019, 1, 1), date(2019, 1, 21), date(2019, 2, 18), date(2019, 4, 19),
        date(2019, 5, 27), date(2019, 7, 4), date(2019, 9, 2), date(2019, 11, 28),
        date(2019, 12, 25),
    ],
    2020: [
        date(2020, 1, 1), date(2020, 1, 20), date(2020, 2, 17), date(2020, 4, 10),
        date(2020, 5, 25), date(2020, 7, 3), date(2020, 9, 7), date(2020, 11, 26),
        date(2020, 12, 25),
    ],
    2021: [
        date(2021, 1, 1), date(2021, 1, 18), date(2021, 2, 15), date(2021, 4, 2),
        date(2021, 5, 31), date(2021, 7, 5), date(2021, 9, 6), date(2021, 11, 25),
        date(2021, 12, 24),
    ],
    2022: [
        date(2022, 1, 17), date(2022, 2, 21), date(2022, 4, 15), date(2022, 5, 30),
        date(2022, 6, 20), date(2022, 7, 4), date(2022, 9, 5), date(2022, 11, 24),
        date(2022, 12, 26),
    ],
    2023: [
        date(2023, 1, 2), date(2023, 1, 16), date(2023, 2, 20), date(2023, 4, 7),
        date(2023, 5, 29), date(2023, 6, 19), date(2023, 7, 4), date(2023, 9, 4),
        date(2023, 11, 23), date(2023, 12, 25),
    ],
    2024: [
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 12, 25),
    ],
    2025: [
        date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 12, 25),
    ],
}

def get_holidays_by_day(year):
    """Get holidays categorized by day of week for a given year."""
    all_holidays = MARKET_HOLIDAYS.get(year, [])
    monday_holidays = [d for d in all_holidays if d.weekday() == 0]
    thursday_holidays = [d for d in all_holidays if d.weekday() == 3]
    friday_holidays = [d for d in all_holidays if d.weekday() == 4]
    return monday_holidays, thursday_holidays, friday_holidays

def parse_option_ticker(ticker: str, entry_date: date = None):
    """
    Parse option ticker format: O:GOOG180126C00197500
    Returns: symbol, expiration_date, option_type, strike
    """
    try:
        # Remove 'O:' prefix if present
        if ticker.startswith('O:'):
            ticker = ticker[2:]
        
        # Extract symbol (first part, variable length)
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        symbol = match.group(1)
        date_str = match.group(2)
        option_type = match.group(3)
        strike_str = match.group(4)
        
        # Parse date: YYMMDD -> YYYY-MM-DD
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        expiration_date = date(year, month, day)
        
        # Parse strike: check if entry_date is on/after Aug 31, 2020
        strike_raw = float(strike_str) / 1000.0
        if entry_date and entry_date >= CUTOFF_DATE:
            # On/After Aug 31, 2020: just divide by 1000
            strike = strike_raw
        else:
            # Before Aug 31, 2020: divide by 1000, then by 4
            strike = strike_raw / 4.0
        
        return {
            'symbol': symbol,
            'expiration_date': expiration_date,
            'option_type': option_type,
            'strike': strike
        }
    except (ValueError, IndexError):
        return None

print("=" * 80)
print("FETCHING GOOG (NOT GOOGL) FOR HOLIDAYS FOLDER")
print("REPLACING EXISTING DATA")
print("=" * 80)

# Load environment variables
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

# Connect to DuckDB
print("\n🔌 Connecting to DuckDB...", flush=True)

try:
    DB_PATH = Path(__file__).parent / "options_temp_goog_only.duckdb"
    con = duckdb.connect(str(DB_PATH))
    print(f"✅ DuckDB connected", flush=True)
except Exception as e:
    print(f"❌ Error connecting to DuckDB: {e}", flush=True)
    raise

# Enable S3 support
print("\n🌐 Enabling S3 support...", flush=True)
try:
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
    print("✅ S3 support enabled", flush=True)
except Exception as e:
    print(f"❌ Error enabling S3: {e}", flush=True)
    raise

# Import helper functions
print("\n📦 Loading helper functions...", flush=True)
base_path = Path(__file__).parent.parent.parent.parent

monthly_path = Path(__file__).parent.parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

premium_path = Path(__file__).parent.parent / "add_premium_columns.py"
spec_premium = importlib.util.spec_from_file_location("add_premium_columns", premium_path)
premium_module = importlib.util.module_from_spec(spec_premium)
spec_premium.loader.exec_module(premium_module)

greeks2_path = Path(__file__).parent.parent / "greeks2.py"
spec_greeks = importlib.util.spec_from_file_location("greeks2", greeks2_path)
greeks2 = importlib.util.module_from_spec(spec_greeks)
spec_greeks.loader.exec_module(greeks2)

add_underlying_prices_from_csv = monthly.add_underlying_prices_from_csv
add_premium_columns = premium_module.add_premium_columns
implied_volatility_call = greeks2.implied_volatility_call
probability_itm = greeks2.probability_itm

print("✅ Helper functions loaded", flush=True)

# Test S3 connection
print("\n🔍 Testing S3 connection...", flush=True)
try:
    test_result = con.execute(f"""
    SELECT COUNT(*) as file_count
    FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/2023/01/*.csv.gz')
    LIMIT 1;
    """).fetchone()
    print(f"✅ S3 connection test successful", flush=True)
except Exception as e:
    print(f"❌ S3 connection test FAILED: {e}", flush=True)
    sys.exit(1)

# Process each year
for TEST_YEAR in YEARS:
    print("\n" + "=" * 80)
    print(f"PROCESSING {TICKER} {TEST_YEAR}")
    print("=" * 80)
    
    # Get holidays for this year
    MONDAY_HOLIDAYS, THURSDAY_HOLIDAYS, FRIDAY_HOLIDAYS = get_holidays_by_day(TEST_YEAR)
    ALL_HOLIDAYS = MARKET_HOLIDAYS.get(TEST_YEAR, [])
    
    print(f"\n📅 Market holidays for {TEST_YEAR}:")
    print(f"   All holidays: {len(ALL_HOLIDAYS)}")
    print(f"   Monday holidays: {len(MONDAY_HOLIDAYS)}")
    print(f"   Thursday holidays: {len(THURSDAY_HOLIDAYS)}")
    print(f"   Friday holidays: {len(FRIDAY_HOLIDAYS)}")
    
    # Load data from S3 - filter to GOOG or GOOGL initially, then filter to GOOG only after parsing
    table_name = f"options_{TEST_YEAR}_{TICKER}_goog_only"
    print(f"\n📥 Loading data from S3 for {TEST_YEAR}...")
    
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
        
        max_retries = 5
        retry_delay = 10
        all_files = None
        
        for attempt in range(max_retries):
            try:
                all_files = con.execute(f"""
                SELECT file 
                FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/*/*.csv.gz')
                ORDER BY file;
                """).fetchall()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"   ⚠️  S3 connection error (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay}s...", flush=True)
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"   ❌ Could not connect to S3 after {max_retries} attempts: {e}", flush=True)
                    raise RuntimeError(f"S3 connection failed for {TICKER} {TEST_YEAR}")
        
        if not all_files:
            print(f"   ❌ No files found for {TEST_YEAR}", flush=True)
            raise RuntimeError(f"No files found for {TICKER} {TEST_YEAR}")
        
        print(f"   ✅ Found {len(all_files)} files")
        
        # Filter to GOOG or GOOGL initially (we'll filter to GOOG only after parsing)
        ticker_condition = f"(ticker LIKE 'O:GOOG%' OR ticker LIKE 'O:GOOGL%')"
        where_clause = f"WHERE ({ticker_condition})"
        
        print(f"   ⏳ Reading and aggregating files (filtering for GOOG/GOOGL)...")
        start_time = time.time()
        
        try:
            file_paths = [f"'{row[0]}'" for row in all_files]
            file_list = ',\n            '.join(file_paths)
            
            con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *
            FROM read_csv_auto([
                {file_list}
            ], compression='gzip')
            {where_clause};
            """)
            
            elapsed = time.time() - start_time
            result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            print(f"   ✅ Finished reading files (took {elapsed:.1f} seconds)")
            print(f"   ✅ Loaded {result[0]:,} rows")
        except Exception as e:
            print(f"   ❌ Could not load data for {TEST_YEAR}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Load data
    try:
        df = con.execute(f"SELECT * FROM {table_name}").df()
    except:
        print(f"   ⚠️  Could not load table {table_name}, skipping...")
        continue
    
    if len(df) == 0:
        print(f"   ⚠️  No data for {TEST_YEAR}, skipping...")
        continue
    
    print(f"\n🔧 Processing data for {TEST_YEAR}...")
    print(f"   Loaded {len(df):,} rows")
    
    # Parse tickers
    print(f"   Parsing option tickers...")
    if 'date_only' in df.columns:
        df['date_only_parsed'] = pd.to_datetime(df['date_only']).dt.date
    else:
        df['date_only_parsed'] = pd.to_datetime(df['window_start'] / 1_000_000_000, unit='s').dt.date
    
    def parse_with_date(row):
        entry_date = row['date_only_parsed'] if 'date_only_parsed' in row else None
        return parse_option_ticker(row['ticker'], entry_date=entry_date)
    
    parsed = df.apply(parse_with_date, axis=1)
    df['underlying_symbol'] = parsed.apply(lambda x: x['symbol'] if x else None)
    df['expiration_date'] = parsed.apply(lambda x: x['expiration_date'] if x else None)
    df['option_type'] = parsed.apply(lambda x: x['option_type'] if x else None)
    df['strike'] = parsed.apply(lambda x: x['strike'] if x else None)
    df = df.drop(columns=['date_only_parsed'], errors='ignore')
    
    # CRITICAL: Filter to GOOG only (exclude GOOGL)
    print(f"   Filtering to GOOG only (excluding GOOGL)...")
    before_filter = len(df)
    df = df[df['underlying_symbol'] == 'GOOG'].copy()
    after_filter = len(df)
    print(f"   ✅ Filtered from {before_filter:,} to {after_filter:,} rows (GOOG only)")
    
    if len(df) == 0:
        print(f"   ⚠️  No GOOG data for {TEST_YEAR}, skipping...")
        continue
    
    # Filter to calls only
    df = df[df['option_type'] == 'C'].copy()
    print(f"   Filtered to calls: {len(df):,} rows")
    
    # Convert dates
    if 'date_only' not in df.columns:
        df['date_only'] = pd.to_datetime(df['window_start'] / 1_000_000_000, unit='s').dt.date
    else:
        df['date_only'] = pd.to_datetime(df['date_only']).dt.date
    
    if not isinstance(df['expiration_date'].iloc[0], date):
        df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
    
    df['date_only_dt'] = pd.to_datetime(df['date_only'])
    df['expiration_date_dt'] = pd.to_datetime(df['expiration_date'])
    
    # Calculate days_to_expiry
    df['days_to_expiry'] = (df['expiration_date_dt'] - df['date_only_dt']).dt.days
    df['day_of_week'] = df['date_only_dt'].dt.dayofweek
    
    # Filter to year
    df = df[df['date_only_dt'].dt.year == TEST_YEAR].copy()
    print(f"   Filtered to {TEST_YEAR}: {len(df):,} rows")
    
    # Include ALL weekly options
    df_weekly = df[df['expiration_date_dt'].dt.dayofweek.isin([3, 4])].copy()
    print(f"   Filtered to weekly options (Thursday and Friday expirations): {len(df_weekly):,} rows")
    
    # Apply holiday filtering
    print(f"\n📊 Applying holiday filtering...")
    
    # Rule 1: Normal Monday-Friday entries
    print(f"   Rule 1: Normal Monday-Friday entries...")
    monday_friday = df_weekly[
        (df_weekly['days_to_expiry'] == 4) &
        (df_weekly['day_of_week'] == 0) &
        (df_weekly['expiration_date_dt'].dt.dayofweek == 4)
    ].copy()
    
    monday_friday_normal = []
    for _, row in monday_friday.iterrows():
        expiration_date = row['expiration_date']
        if expiration_date not in FRIDAY_HOLIDAYS:
            monday_friday_normal.append(row)
    
    monday_friday_normal_df = pd.DataFrame(monday_friday_normal)
    print(f"   ✅ Normal Monday-Friday: {len(monday_friday_normal_df):,} rows")
    
    # Rule 2: Monday-Thursday entries (Friday holiday)
    print(f"   Rule 2: Monday-Thursday entries (Friday holiday)...")
    monday_thursday = df_weekly[
        (df_weekly['days_to_expiry'] == 3) &
        (df_weekly['day_of_week'] == 0) &
        (df_weekly['expiration_date_dt'].dt.dayofweek == 3)
    ].copy()
    
    monday_thursday_holiday = []
    for _, row in monday_thursday.iterrows():
        entry_date = row['date_only']
        friday_of_week = entry_date + timedelta(days=4)
        if friday_of_week in FRIDAY_HOLIDAYS:
            monday_thursday_holiday.append(row)
    
    monday_thursday_holiday_df = pd.DataFrame(monday_thursday_holiday)
    print(f"   ✅ Monday-Thursday (Friday holiday): {len(monday_thursday_holiday_df):,} rows")
    
    # Rule 3: Tuesday-Friday entries (Monday holiday)
    print(f"   Rule 3: Tuesday-Friday entries (Monday holiday)...")
    tuesday_friday = df_weekly[
        (df_weekly['days_to_expiry'] == 3) &
        (df_weekly['day_of_week'] == 1) &
        (df_weekly['expiration_date_dt'].dt.dayofweek == 4)
    ].copy()
    
    tuesday_friday_holiday = []
    for _, row in tuesday_friday.iterrows():
        entry_date = row['date_only']
        expiration_date = row['expiration_date']
        monday_before = entry_date - timedelta(days=1)
        if monday_before in MONDAY_HOLIDAYS and expiration_date not in FRIDAY_HOLIDAYS:
            tuesday_friday_holiday.append(row)
    
    tuesday_friday_holiday_df = pd.DataFrame(tuesday_friday_holiday)
    print(f"   ✅ Tuesday-Friday (Monday holiday): {len(tuesday_friday_holiday_df):,} rows")
    
    # Combine all entries
    print(f"\n📊 Combining entries...")
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
        print(f"   ⚠️  No entries found for {TEST_YEAR}, skipping...")
        continue
    
    # Drop temporary columns
    all_entries = all_entries.drop(columns=['date_only_dt', 'expiration_date_dt', 'day_of_week'], errors='ignore')
    
    print(f"   ✅ Total entries: {len(all_entries):,}")
    
    # Rename columns
    if 'open' in all_entries.columns:
        all_entries = all_entries.rename(columns={
            'open': 'open_price',
            'close': 'close_price',
            'high': 'high_price',
            'low': 'low_price'
        })
    
    # Divide prices by 4
    print(f"\n🔧 Dividing prices by 4...")
    all_entries['open_price'] = all_entries['open_price'] / 4.0
    all_entries['close_price'] = all_entries['close_price'] / 4.0
    all_entries['high_price'] = all_entries['high_price'] / 4.0
    all_entries['low_price'] = all_entries['low_price'] / 4.0
    
    # Select essential columns
    essential_columns = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
        'strike', 'volume', 'open_price', 'close_price', 'high_price', 'low_price', 
        'transactions', 'window_start', 'days_to_expiry'
    ]
    
    ticker_data_clean = all_entries[[c for c in essential_columns if c in all_entries.columns]].copy()
    
    # Match with historical stock prices
    stock_file = base_path / "data" / TICKER / f"HistoricalData_{TICKER}.csv"
    if not stock_file.exists():
        stock_files = list((base_path / "data" / TICKER).glob("HistoricalData*.csv"))
        if stock_files:
            stock_file = stock_files[0]
    
    if not stock_file.exists():
        print(f"   ⚠️  Historical stock data file not found: {stock_file}, skipping stock price merge...")
        ticker_data_final = ticker_data_clean
    else:
        print(f"\n📈 Matching with historical stock prices...")
        ticker_data_with_prices = add_underlying_prices_from_csv(
            ticker_data_clean,
            str(stock_file),
            symbol=TICKER,
            use_pessimistic=True
        )
        
        # Remove duplicate columns
        ticker_data_with_prices = ticker_data_with_prices.loc[:, ~ticker_data_with_prices.columns.duplicated()]
        
        ticker_data_final = add_premium_columns(ticker_data_with_prices)
        
        # Ensure premium = mid_price
        ticker_data_final['mid_price'] = (ticker_data_final['high_price'] + ticker_data_final['low_price']) / 2.0
        ticker_data_final['premium'] = ticker_data_final['mid_price']
        ticker_data_final['premium_yield_pct'] = (ticker_data_final['mid_price'] / ticker_data_final['underlying_spot'] * 100).round(2)
        
        # Add fedfunds_rate
        if 'fedfunds_rate' not in ticker_data_final.columns:
            ticker_data_final['fedfunds_rate'] = 0.02
        
        # Calculate implied volatility and probability ITM
        print(f"   📊 Calculating implied volatility and probability ITM...")
        ticker_data_final['T'] = ticker_data_final['days_to_expiry'] / 365.0
        
        def calc_iv_prob(row):
            try:
                C = row['mid_price'] if 'mid_price' in row and pd.notna(row['mid_price']) else row['close_price']
                S = row['underlying_spot']
                K = row['strike']
                T = row['T']
                r = row['fedfunds_rate'] if pd.notna(row['fedfunds_rate']) else 0.02
                
                if pd.isna(C) or pd.isna(S) or pd.isna(K) or pd.isna(T) or C <= 0 or S <= 0 or K <= 0 or T <= 0:
                    return np.nan, np.nan
                
                iv = implied_volatility_call(C, S, K, T, r)
                if pd.isna(iv) or iv <= 0:
                    return np.nan, np.nan
                
                prob = probability_itm(S, K, T, r, iv)
                return iv, prob
            except:
                return np.nan, np.nan
        
        iv_prob = ticker_data_final.apply(calc_iv_prob, axis=1)
        ticker_data_final['implied_volatility'] = [x[0] for x in iv_prob]
        ticker_data_final['probability_itm'] = [x[1] for x in iv_prob]
        ticker_data_final = ticker_data_final.drop(columns=['T'])
        
        # Add missing columns to match reference
        reference_file = base_path / "data" / TICKER / "weekly" / "2022_options_pessimistic.csv"
        if reference_file.exists():
            reference_df = pd.read_csv(reference_file, nrows=1)
            reference_columns = list(reference_df.columns)
            
            for col in reference_columns:
                if col not in ticker_data_final.columns:
                    if col == 'time_remaining_category':
                        ticker_data_final['time_remaining_category'] = 'Weekly'
                    elif col == 'mid_price':
                        ticker_data_final['mid_price'] = (ticker_data_final['high_price'] + ticker_data_final['low_price']) / 2.0
                    elif col == 'high_yield_pct':
                        ticker_data_final['high_yield_pct'] = (ticker_data_final['high_price'] / ticker_data_final['underlying_spot'] * 100).round(2)
                    else:
                        ticker_data_final[col] = np.nan
            
            # Reorder columns
            existing_cols = [c for c in reference_columns if c in ticker_data_final.columns]
            ticker_data_final = ticker_data_final[existing_cols]
        
        # Round numeric columns
        print(f"   🔧 Rounding numeric columns...")
        exclude_cols = ['ticker', 'window_start', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 'ITM', 'time_remaining_category']
        four_decimal_cols = ['implied_volatility', 'probability_itm']
        
        for col in ticker_data_final.columns:
            if col not in exclude_cols:
                try:
                    col_data = ticker_data_final[col]
                    if hasattr(col_data, 'dtype'):
                        if col_data.dtype in ['float64', 'float32']:
                            if col in four_decimal_cols:
                                ticker_data_final[col] = ticker_data_final[col].round(4)
                            else:
                                ticker_data_final[col] = ticker_data_final[col].round(2)
                except:
                    pass
        
        # Sort and save
        ticker_data_final['date_only'] = pd.to_datetime(ticker_data_final['date_only'])
        ticker_data_final = ticker_data_final.sort_values(['date_only', 'strike']).reset_index(drop=True)
        ticker_data_final['date_only'] = ticker_data_final['date_only'].dt.date
        ticker_data_final['expiration_date'] = pd.to_datetime(ticker_data_final['expiration_date']).dt.date
    
    # Save to holidays folder (REPLACE existing files)
    output_base_dir = base_path / "data" / TICKER / "holidays"
    output_base_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_base_dir / f"{TEST_YEAR}_options_pessimistic.csv"
    
    print(f"\n💾 Saving to {output_file} (REPLACING existing file)...")
    ticker_data_final.to_csv(output_file, index=False)
    
    # Verify
    final_dates = sorted(ticker_data_final['date_only'].unique())
    print(f"✅ Saved {output_file}")
    print(f"   Total rows: {len(ticker_data_final):,}")
    print(f"   Unique entry dates: {len(final_dates)}")
    if len(final_dates) > 0:
        print(f"   First date: {final_dates[0]}")
        print(f"   Last date: {final_dates[-1]}")
    print(f"   All rows are GOOG (not GOOGL): {all(ticker_data_final['underlying_symbol'] == 'GOOG')}")

print("\n" + "=" * 80)
print("✅ ALL GOOG DATA FETCHED AND REPLACED")
print("=" * 80)


