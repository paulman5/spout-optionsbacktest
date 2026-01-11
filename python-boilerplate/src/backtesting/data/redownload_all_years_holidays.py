#!/usr/bin/env python3
"""
Redownload AAPL weekly options data for all years (2016-2025) with holiday adjustments:
- Normal weeks: Monday entries (days_to_expiry=4, Monday to Friday)
- Friday holiday weeks: Monday entries (days_to_expiry=3, Monday to Thursday)
- Monday holiday weeks: Tuesday entries (days_to_expiry=3, Tuesday to Friday)
- Save to holidays folder with all columns matching reference file
- Divide prices by 4 to match correct format
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

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

TICKER = 'AAPL'
YEARS = list(range(2016, 2026))  # 2016-2025

# Market holidays by year (Monday holidays and Friday holidays)
def get_monday_holidays(year):
    """Get Monday holidays for a given year."""
    holidays = []
    # New Year's Day (if Monday)
    ny = date(year, 1, 1)
    if ny.weekday() == 0:
        holidays.append(ny)
    elif ny.weekday() == 6:  # If Sunday, Monday is observed
        holidays.append(ny + timedelta(days=1))
    
    # Martin Luther King Jr. Day (3rd Monday of January)
    mlk = date(year, 1, 15)
    while mlk.weekday() != 0:
        mlk += timedelta(days=1)
    holidays.append(mlk)
    
    # Presidents' Day (3rd Monday of February)
    pres = date(year, 2, 15)
    while pres.weekday() != 0:
        pres += timedelta(days=1)
    holidays.append(pres)
    
    # Memorial Day (last Monday of May)
    mem = date(year, 5, 31)
    while mem.weekday() != 0:
        mem -= timedelta(days=1)
    holidays.append(mem)
    
    # Labor Day (1st Monday of September)
    labor = date(year, 9, 1)
    while labor.weekday() != 0:
        labor += timedelta(days=1)
    holidays.append(labor)
    
    # Columbus Day (2nd Monday of October) - not always a market holiday, but included
    # columbus = date(year, 10, 8)
    # while columbus.weekday() != 0:
    #     columbus += timedelta(days=1)
    # holidays.append(columbus)
    
    # Veterans Day (if Monday)
    vet = date(year, 11, 11)
    if vet.weekday() == 0:
        holidays.append(vet)
    elif vet.weekday() == 6:
        holidays.append(vet + timedelta(days=1))
    
    # Thanksgiving (4th Thursday) - not a Monday, but check if Friday is observed
    # Thanksgiving is Thursday, so Friday after is often a half-day or holiday
    
    # Christmas (if Monday)
    xmas = date(year, 12, 25)
    if xmas.weekday() == 0:
        holidays.append(xmas)
    elif xmas.weekday() == 6:
        holidays.append(xmas + timedelta(days=1))
    
    return sorted(holidays)

def get_friday_holidays(year):
    """Get Friday holidays for a given year."""
    holidays = []
    
    # Good Friday (varies by year - approximate calculation)
    # Good Friday is the Friday before Easter
    # Using a simple approximation: Good Friday is typically in March or April
    # For 2016-2025, Good Friday dates:
    good_friday_dates = {
        2016: date(2016, 3, 25),
        2017: date(2017, 4, 14),
        2018: date(2018, 3, 30),
        2019: date(2019, 4, 19),
        2020: date(2020, 4, 10),
        2021: date(2021, 4, 2),
        2022: date(2022, 4, 15),
        2023: date(2023, 4, 7),
        2024: date(2024, 3, 29),
        2025: date(2025, 4, 18),
    }
    if year in good_friday_dates:
        holidays.append(good_friday_dates[year])
    
    # Day after Thanksgiving (Black Friday) - always a Friday
    # Thanksgiving is 4th Thursday of November
    thanksgiving = date(year, 11, 22)
    while thanksgiving.weekday() != 3:  # Thursday
        thanksgiving += timedelta(days=1)
    black_friday = thanksgiving + timedelta(days=1)
    holidays.append(black_friday)
    
    return sorted(holidays)

print("=" * 80)
print(f"REDOWNLOADING {TICKER} WEEKLY OPTIONS FOR ALL YEARS WITH HOLIDAY ADJUSTMENTS")
print("=" * 80)
print(f"Years: {YEARS[0]}-{YEARS[-1]}")
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
DB_PATH = Path(__file__).parent / "options_temp_all_years.duckdb"

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

print("✅ S3 support enabled", flush=True)

# ------------------------------------------------------------
# 4. Import helper functions
# ------------------------------------------------------------

print("\n📦 Loading helper functions...", flush=True)

monthly_path = Path(__file__).parent.parent / "weekly-monthly" / "monthly.py"
spec = importlib.util.spec_from_file_location("monthly", monthly_path)
monthly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monthly)

premium_path = Path(__file__).parent.parent / "add_premium_columns.py"
spec_premium = importlib.util.spec_from_file_location("add_premium_columns", premium_path)
premium_module = importlib.util.module_from_spec(spec_premium)
spec_premium.loader.exec_module(premium_module)

# Import greeks2 for IV and probability ITM
greeks2_path = Path(__file__).parent.parent / "greeks2.py"
spec_greeks = importlib.util.spec_from_file_location("greeks2", greeks2_path)
greeks2 = importlib.util.module_from_spec(spec_greeks)
spec_greeks.loader.exec_module(greeks2)

load_historical_stock_prices = monthly.load_historical_stock_prices
add_underlying_prices_from_csv = monthly.add_underlying_prices_from_csv
add_premium_columns = premium_module.add_premium_columns
implied_volatility_call = greeks2.implied_volatility_call
probability_itm = greeks2.probability_itm

print("✅ Helper functions loaded", flush=True)

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
        strike = float(strike_str) / 1000.0  # Divide by 1000
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

base_path = Path(__file__).parent.parent.parent.parent
output_base_dir = base_path / "data" / TICKER / "holidays"
output_base_dir.mkdir(parents=True, exist_ok=True)

# Reference file to get column order
reference_file = base_path / "data" / TICKER / "weekly" / "2018_options_pessimistic.csv"
if reference_file.exists():
    reference_df = pd.read_csv(reference_file, nrows=1)
    reference_columns = list(reference_df.columns)
    print(f"\n📋 Reference columns ({len(reference_columns)}): {', '.join(reference_columns)}")
else:
    reference_columns = None
    print("\n⚠️  Reference file not found, will use default column order")

for TEST_YEAR in YEARS:
    print("\n" + "=" * 80)
    print(f"PROCESSING YEAR {TEST_YEAR}")
    print("=" * 80)
    
    # Get holidays for this year
    monday_holidays = get_monday_holidays(TEST_YEAR)
    friday_holidays = get_friday_holidays(TEST_YEAR)
    
    print(f"\n📅 Holidays for {TEST_YEAR}:")
    print(f"   Monday holidays: {len(monday_holidays)} - {[str(d) for d in monday_holidays]}")
    print(f"   Friday holidays: {len(friday_holidays)} - {[str(d) for d in friday_holidays]}")
    
    # Load data from S3
    table_name = f"options_{TEST_YEAR}"
    print(f"\n📥 Loading data from S3 for {TEST_YEAR}...")
    
    # Check if table exists (try multiple possible table names)
    table_exists = False
    possible_table_names = [
        f"options_{TEST_YEAR}",
        f"options_day_aggs_{TEST_YEAR}",
        f"options_day_aggs_{TEST_YEAR}_monday",
    ]
    
    for possible_name in possible_table_names:
        try:
            result = con.execute(f"SELECT COUNT(*) FROM {possible_name}").fetchone()
            if result and result[0] > 0:
                table_exists = True
                table_name = possible_name
                print(f"   ✅ Table {table_name} already exists with {result[0]:,} rows")
                break
        except:
            pass
    
    if not table_exists:
        print(f"   🔍 Finding files for {TEST_YEAR}...")
        
        # Get all files for the year (same approach as aggregate.py)
        try:
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
            ticker_condition = f"ticker LIKE 'O:{TICKER}%'"
            where_clause = f"WHERE ({ticker_condition})"
            
            print(f"   ⏳ Reading and aggregating files (filtering for {TICKER})...")
            start_time = time.time()
            
            # Build file list (same approach as aggregate.py)
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
            result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            print(f"   ✅ Finished reading files (took {elapsed:.1f} seconds / {elapsed/60:.1f} minutes)")
            print(f"   ✅ Loaded {result[0]:,} rows")
        except Exception as e:
            print(f"   ❌ Could not load data for {TEST_YEAR}: {e}")
            print(f"   ⚠️  Skipping {TEST_YEAR} - data may not be available in S3")
            continue
    
    # Load data
    print(f"\n🔧 Processing data for {TEST_YEAR}...")
    try:
        df = con.execute(f"SELECT * FROM {table_name}").df()
    except Exception as e:
        print(f"   ❌ Error loading table {table_name}: {e}")
        print(f"   ⚠️  Skipping {TEST_YEAR}")
        continue
    
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
    
    # Filter to calls only
    df = df[df['option_type'] == 'C'].copy()
    print(f"   Filtered to calls: {len(df):,} rows")
    
    # Filter to weekly options (exclude 3rd Friday monthly)
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    df['exp_day_of_week'] = df['expiration_date'].dt.dayofweek
    
    # Weekly options are Fridays that are NOT the 3rd Friday
    def is_third_friday(exp_date):
        first_day = exp_date.replace(day=1)
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_until_friday)
        third_friday = first_friday + timedelta(days=14)
        return exp_date.date() == third_friday.date()
    
    df['is_third_friday'] = df['expiration_date'].apply(is_third_friday)
    df_weekly = df[(df['exp_day_of_week'] == 4) & (~df['is_third_friday'])].copy()
    print(f"   Filtered to weekly options: {len(df_weekly):,} rows")
    
    # Convert date_only
    df_weekly['date_only'] = pd.to_datetime(df_weekly['window_start'] / 1_000_000_000, unit='s').dt.date
    df_weekly['date_only_dt'] = pd.to_datetime(df_weekly['date_only'])
    
    # Calculate days_to_expiry
    df_weekly['days_to_expiry'] = (df_weekly['expiration_date'] - df_weekly['date_only_dt']).dt.days
    df_weekly['day_of_week'] = df_weekly['date_only_dt'].dt.dayofweek
    
    # Filter to 2018
    df_weekly = df_weekly[df_weekly['date_only_dt'].dt.year == TEST_YEAR].copy()
    print(f"   Filtered to {TEST_YEAR}: {len(df_weekly):,} rows")
    
    # Get normal Monday-Friday entries
    print(f"   Filtering to normal Monday-Friday entries (days_to_expiry=4)...")
    monday_friday = df_weekly[
        (df_weekly['days_to_expiry'] == 4) &
        (df_weekly['day_of_week'] == 0)
    ].copy()
    
    monday_friday_normal = []
    for _, row in monday_friday.iterrows():
        expiration_date = row['expiration_date'].date()
        if expiration_date not in friday_holidays:
            monday_friday_normal.append(row)
    
    monday_friday_normal_df = pd.DataFrame(monday_friday_normal)
    print(f"   ✅ Found {len(monday_friday_normal_df):,} normal Monday-Friday entries")
    
    # Get Monday-Thursday entries (Friday holiday weeks)
    print(f"   Filtering to Monday-Thursday entries (days_to_expiry=3) for Friday holiday weeks...")
    monday_thursday = df_weekly[
        (df_weekly['days_to_expiry'] == 3) &
        (df_weekly['day_of_week'] == 0)
    ].copy()
    
    monday_thursday_holiday = []
    for _, row in monday_thursday.iterrows():
        entry_date = row['date_only']
        friday_of_week = entry_date + timedelta(days=4)
        if friday_of_week in friday_holidays:
            monday_thursday_holiday.append(row)
    
    monday_thursday_holiday_df = pd.DataFrame(monday_thursday_holiday)
    print(f"   ✅ Found {len(monday_thursday_holiday_df):,} Monday-Thursday entries for Friday holiday weeks")
    
    # Get Tuesday-Friday entries (Monday holiday weeks)
    print(f"   Filtering to Tuesday-Friday entries (days_to_expiry=3) for Monday holiday weeks...")
    tuesday_friday = df_weekly[
        (df_weekly['days_to_expiry'] == 3) &
        (df_weekly['day_of_week'] == 1)
    ].copy()
    
    tuesday_friday_holiday = []
    for _, row in tuesday_friday.iterrows():
        entry_date = row['date_only']
        monday_before = entry_date - timedelta(days=1)
        if monday_before in monday_holidays:
            tuesday_friday_holiday.append(row)
    
    tuesday_friday_holiday_df = pd.DataFrame(tuesday_friday_holiday)
    print(f"   ✅ Found {len(tuesday_friday_holiday_df):,} Tuesday-Friday entries for Monday holiday weeks")
    
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
        all_entries = pd.DataFrame()
    
    if len(all_entries) == 0:
        print(f"   ⚠️  No entries found for {TEST_YEAR}, skipping...")
        continue
    
    # Drop temporary columns
    all_entries = all_entries.drop(columns=['date_only_dt', 'day_of_week', 'is_third_friday', 'exp_day_of_week'], errors='ignore')
    
    print(f"   ✅ Total entries: {len(all_entries):,}")
    
    # Rename columns
    all_entries = all_entries.rename(columns={
        'open': 'open_price',
        'close': 'close_price',
        'high': 'high_price',
        'low': 'low_price'
    })
    
    # Divide prices by 4 to match correct format
    print(f"   🔧 Dividing prices by 4 to match correct format...")
    all_entries['open_price'] = all_entries['open_price'] / 4.0
    all_entries['close_price'] = all_entries['close_price'] / 4.0
    all_entries['high_price'] = all_entries['high_price'] / 4.0
    all_entries['low_price'] = all_entries['low_price'] / 4.0
    
    # Divide strike by 4 (already divided by 1000, now divide by 4)
    all_entries['strike'] = all_entries['strike'] / 4.0
    
    # Select essential columns first
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
            
            # Add fedfunds_rate from existing weekly file if available
            if 'fedfunds_rate' not in ticker_data_final.columns:
                weekly_file = base_path / "data" / TICKER / "weekly" / f"{TEST_YEAR}_options_pessimistic.csv"
                if weekly_file.exists():
                    print(f"   📊 Merging fedfunds_rate from existing weekly file...")
                    try:
                        weekly_df = pd.read_csv(weekly_file)
                        if 'fedfunds_rate' in weekly_df.columns:
                            weekly_df['date_only'] = pd.to_datetime(weekly_df['date_only'])
                            fedfunds_map = weekly_df.groupby('date_only')['fedfunds_rate'].first().to_dict()
                            ticker_data_final['date_only_dt'] = pd.to_datetime(ticker_data_final['date_only'])
                            ticker_data_final['fedfunds_rate'] = ticker_data_final['date_only_dt'].map(fedfunds_map)
                            # Forward fill missing values
                            ticker_data_final = ticker_data_final.sort_values('date_only_dt')
                            ticker_data_final['fedfunds_rate'] = ticker_data_final['fedfunds_rate'].ffill().bfill()
                            ticker_data_final = ticker_data_final.drop(columns=['date_only_dt'], errors='ignore')
                            print(f"   ✅ Merged fedfunds_rate")
                        else:
                            ticker_data_final['fedfunds_rate'] = np.nan
                    except Exception as e:
                        print(f"   ⚠️  Error merging fedfunds_rate: {e}")
                        ticker_data_final['fedfunds_rate'] = np.nan
                else:
                    ticker_data_final['fedfunds_rate'] = np.nan
            
            # Calculate implied volatility and probability ITM
            print(f"   📊 Calculating implied volatility and probability ITM...")
            ticker_data_final['T'] = ticker_data_final['days_to_expiry'] / 365.0
            
            def calc_iv_prob(row):
                try:
                    C = row['close_price']
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
            if reference_columns:
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
            
            # Reorder columns to match reference
            if reference_columns:
                # Add any missing columns from reference
                for col in reference_columns:
                    if col not in ticker_data_final.columns:
                        ticker_data_final[col] = np.nan
                # Reorder
                existing_cols = [c for c in reference_columns if c in ticker_data_final.columns]
                ticker_data_final = ticker_data_final[existing_cols]
            
            # Sort by date and strike
            ticker_data_final['date_only'] = pd.to_datetime(ticker_data_final['date_only'])
            ticker_data_final = ticker_data_final.sort_values(['date_only', 'strike']).reset_index(drop=True)
            # Convert date_only back to date string for CSV
            ticker_data_final['date_only'] = ticker_data_final['date_only'].dt.date
            
        except Exception as e:
            print(f"   ⚠️  Error processing: {e}")
            import traceback
            traceback.print_exc()
            ticker_data_final = ticker_data_clean
    else:
        ticker_data_final = ticker_data_clean
        print(f"   ⚠️  Stock price file not found, saving without stock prices")
    
    # Save to holidays folder
    output_dir = output_base_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{TEST_YEAR}_options_pessimistic.csv"
    
    # Ensure date_only is formatted as date string
    if 'date_only' in ticker_data_final.columns:
        if hasattr(ticker_data_final['date_only'].iloc[0], 'date'):
            ticker_data_final['date_only'] = ticker_data_final['date_only'].dt.date
        elif isinstance(ticker_data_final['date_only'].iloc[0], pd.Timestamp):
            ticker_data_final['date_only'] = ticker_data_final['date_only'].dt.date
    
    # Ensure expiration_date is formatted properly
    if 'expiration_date' in ticker_data_final.columns:
        if isinstance(ticker_data_final['expiration_date'].iloc[0], pd.Timestamp):
            ticker_data_final['expiration_date'] = ticker_data_final['expiration_date'].dt.date
    
    ticker_data_final.to_csv(output_file, index=False)
    print(f"\n✅ Saved {output_file}")
    print(f"   Total rows: {len(ticker_data_final):,}")
    print(f"   Columns: {len(ticker_data_final.columns)}")
    if reference_columns:
        matches = list(ticker_data_final.columns) == reference_columns
        print(f"   Matches reference columns: {matches}")
        if not matches:
            print(f"   Reference columns: {reference_columns}")
            print(f"   Actual columns: {list(ticker_data_final.columns)}")

print("\n" + "=" * 80)
print("✅ ALL YEARS PROCESSED!")
print("=" * 80)
print(f"Output directory: {output_base_dir}")

