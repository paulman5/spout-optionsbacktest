#!/usr/bin/env python3
"""
Fetch missing holiday-adjusted weeks from S3:
- Tuesday-Friday entries when Monday is a holiday
- Monday-Thursday entries when Friday is a holiday
- Merge with existing weekly files
"""

import os
import sys
import duckdb
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

# Market holidays by year
MARKET_HOLIDAYS = {
    2016: [
        date(2016, 1, 1), date(2016, 1, 18), date(2016, 2, 15), date(2016, 3, 25),
        date(2016, 5, 30), date(2016, 7, 4), date(2016, 9, 5), date(2016, 11, 24),
        date(2016, 11, 25), date(2016, 12, 26),
    ],
    2017: [
        date(2017, 1, 2), date(2017, 1, 16), date(2017, 2, 20), date(2017, 4, 14),
        date(2017, 5, 29), date(2017, 7, 4), date(2017, 9, 4), date(2017, 11, 23),
        date(2017, 11, 24), date(2017, 12, 25),
    ],
    2018: [
        date(2018, 1, 1), date(2018, 1, 15), date(2018, 2, 19), date(2018, 3, 30),
        date(2018, 5, 28), date(2018, 7, 4), date(2018, 9, 3), date(2018, 11, 22),
        date(2018, 11, 23), date(2018, 12, 25),
    ],
    2019: [
        date(2019, 1, 1), date(2019, 1, 21), date(2019, 2, 18), date(2019, 4, 19),
        date(2019, 5, 27), date(2019, 7, 4), date(2019, 9, 2), date(2019, 11, 28),
        date(2019, 11, 29), date(2019, 12, 25),
    ],
    2020: [
        date(2020, 1, 1), date(2020, 1, 20), date(2020, 2, 17), date(2020, 4, 10),
        date(2020, 5, 25), date(2020, 7, 3), date(2020, 9, 7), date(2020, 11, 26),
        date(2020, 11, 27), date(2020, 12, 25),
    ],
    2021: [
        date(2021, 1, 1), date(2021, 1, 18), date(2021, 2, 15), date(2021, 4, 2),
        date(2021, 5, 31), date(2021, 7, 5), date(2021, 9, 6), date(2021, 11, 25),
        date(2021, 11, 26), date(2021, 12, 24),
    ],
    2022: [
        date(2022, 1, 17), date(2022, 2, 21), date(2022, 4, 15), date(2022, 5, 30),
        date(2022, 6, 20), date(2022, 7, 4), date(2022, 9, 5), date(2022, 11, 24),
        date(2022, 11, 25), date(2022, 12, 26),
    ],
    2023: [
        date(2023, 1, 2), date(2023, 1, 16), date(2023, 2, 20), date(2023, 4, 7),
        date(2023, 5, 29), date(2023, 6, 19), date(2023, 7, 4), date(2023, 9, 4),
        date(2023, 11, 23), date(2023, 11, 24), date(2023, 12, 25),
    ],
    2024: [
        date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
        date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
        date(2024, 11, 28), date(2024, 11, 29), date(2024, 12, 25),
    ],
    2025: [
        date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
        date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
        date(2025, 11, 27), date(2025, 11, 28), date(2025, 12, 25),
    ],
}

def get_monday_holidays(year):
    """Get Monday holidays for a given year."""
    holidays = MARKET_HOLIDAYS.get(year, [])
    return [d for d in holidays if d.weekday() == 0]

def get_friday_holidays(year):
    """Get Friday holidays for a given year."""
    holidays = MARKET_HOLIDAYS.get(year, [])
    return [d for d in holidays if d.weekday() == 4]

def is_third_friday(exp_date):
    """Check if expiration date is the 3rd Friday of the month."""
    if isinstance(exp_date, date):
        exp_date_dt = pd.Timestamp(exp_date)
    else:
        exp_date_dt = exp_date
    first_day = exp_date_dt.replace(day=1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day + timedelta(days=days_until_friday)
    third_friday = first_friday + timedelta(days=14)
    return exp_date_dt.date() == third_friday.date()

print("=" * 80)
print(f"FETCHING MISSING HOLIDAY WEEKS FOR {TICKER}")
print("=" * 80)
print(f"Years: {YEARS[0]}-{YEARS[-1]}")
print()

# Load environment variables
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

# Connect to DuckDB
print("\n🔌 Connecting to DuckDB...", flush=True)
DB_PATH = Path(__file__).parent / "options_temp_missing_weeks.duckdb"

try:
    con = duckdb.connect(str(DB_PATH))
    print(f"✅ DuckDB connected", flush=True)
except Exception as e:
    print(f"❌ Error connecting to DuckDB: {e}", flush=True)
    raise

# Enable S3 support
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

# Helper function to parse option ticker
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

# Output directory
output_base_dir = base_path / "data" / TICKER / "holidays"
output_base_dir.mkdir(parents=True, exist_ok=True)

# Process each year
for TEST_YEAR in YEARS:
    print("\n" + "=" * 80)
    print(f"PROCESSING YEAR {TEST_YEAR}")
    print("=" * 80)
    
    # Get holidays
    monday_holidays = get_monday_holidays(TEST_YEAR)
    friday_holidays = get_friday_holidays(TEST_YEAR)
    
    print(f"\n📅 Market holidays for {TEST_YEAR}:")
    print(f"   Monday holidays: {len(monday_holidays)} - {[str(d) for d in monday_holidays]}")
    print(f"   Friday holidays: {len(friday_holidays)} - {[str(d) for d in friday_holidays]}")
    
    # Load existing weekly file
    weekly_file = base_path / "data" / TICKER / "weekly" / f"{TEST_YEAR}_options_pessimistic.csv"
    holidays_file = output_base_dir / f"{TEST_YEAR}_options_pessimistic.csv"
    
    if not weekly_file.exists():
        print(f"   ⚠️  Weekly file not found: {weekly_file}, skipping...")
        continue
    
    print(f"\n📥 Loading existing weekly file...")
    df_existing = pd.read_csv(weekly_file)
    df_existing['date_only'] = pd.to_datetime(df_existing['date_only']).dt.date
    df_existing['expiration_date'] = pd.to_datetime(df_existing['expiration_date']).dt.date
    print(f"   Loaded {len(df_existing):,} rows")
    
    # Calculate missing weeks
    print(f"\n📅 Calculating missing weeks...")
    
    # Expected weeks
    expected_entries = []
    current_date = date(TEST_YEAR, 1, 1)
    end_date = date(TEST_YEAR, 12, 31)
    
    while current_date <= end_date:
        if current_date.weekday() == 0:  # Monday
            if current_date not in monday_holidays:
                # Normal Monday - check Friday expiration
                friday_exp = current_date + timedelta(days=4)
                if not is_third_friday(friday_exp):
                    if friday_exp in friday_holidays:
                        # Friday is holiday - Monday-Thursday entry
                        thursday_exp = current_date + timedelta(days=3)
                        expected_entries.append({
                            'entry_date': current_date,
                            'entry_day': 'Monday',
                            'expiration_date': thursday_exp,
                            'days_to_expiry': 3,
                            'type': 'Monday-Thursday'
                        })
                    else:
                        # Normal Monday-Friday entry
                        expected_entries.append({
                            'entry_date': current_date,
                            'entry_day': 'Monday',
                            'expiration_date': friday_exp,
                            'days_to_expiry': 4,
                            'type': 'Monday-Friday'
                        })
            else:
                # Monday is holiday - Tuesday entry
                tuesday_entry = current_date + timedelta(days=1)
                friday_exp = tuesday_entry + timedelta(days=3)
                if not is_third_friday(friday_exp) and friday_exp not in friday_holidays:
                    expected_entries.append({
                        'entry_date': tuesday_entry,
                        'entry_day': 'Tuesday',
                        'expiration_date': friday_exp,
                        'days_to_expiry': 3,
                        'type': 'Tuesday-Friday'
                    })
        elif current_date.weekday() == 1:  # Tuesday
            monday_before = current_date - timedelta(days=1)
            if monday_before in monday_holidays:
                friday_exp = current_date + timedelta(days=3)
                if not is_third_friday(friday_exp) and friday_exp not in friday_holidays:
                    expected_entries.append({
                        'entry_date': current_date,
                        'entry_day': 'Tuesday',
                        'expiration_date': friday_exp,
                        'days_to_expiry': 3,
                        'type': 'Tuesday-Friday'
                    })
        current_date += timedelta(days=1)
    
    # Find missing entries
    existing_entry_dates = set(df_existing['date_only'].unique())
    expected_entry_dates = {e['entry_date'] for e in expected_entries}
    missing_entry_dates = expected_entry_dates - existing_entry_dates
    
    print(f"   Expected entries: {len(expected_entries)}")
    print(f"   Existing entry dates: {len(existing_entry_dates)}")
    print(f"   Missing entry dates: {len(missing_entry_dates)}")
    
    if not missing_entry_dates:
        print(f"   ✅ No missing weeks, using existing file...")
        # Just process existing file with holiday adjustments
        df_final = df_existing.copy()
    else:
        print(f"   ⚠️  Missing {len(missing_entry_dates)} weeks:")
        for missing_date in sorted(missing_entry_dates)[:10]:
            print(f"      {missing_date}")
        if len(missing_entry_dates) > 10:
            print(f"      ... and {len(missing_entry_dates) - 10} more")
        
        # Fetch missing entries from S3
        print(f"\n📥 Fetching missing entries from S3...")
        
        missing_entries_list = []
        for expected in expected_entries:
            if expected['entry_date'] in missing_entry_dates:
                entry_date = expected['entry_date']
                exp_date = expected['expiration_date']
                days_to_exp = expected['days_to_expiry']
                entry_day = expected['entry_day']
                
                print(f"   Fetching: {expected['type']} entry on {entry_date} (exp: {exp_date})...")
                
                # Build date range for S3 query
                start_date = entry_date - timedelta(days=1)
                end_date = entry_date + timedelta(days=1)
                
                try:
                    # Query S3 for this specific date range
                    query = f"""
                    SELECT *
                    FROM read_csv_auto([
                        's3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/{entry_date.month:02d}/{entry_date.day:02d}/*.csv.gz'
                    ], compression='gzip')
                    WHERE ticker LIKE 'O:{TICKER}%'
                    LIMIT 100000;
                    """
                    
                    df_missing = con.execute(query).df()
                    
                    if len(df_missing) > 0:
                        # Parse tickers
                        parsed = df_missing['ticker'].apply(parse_option_ticker)
                        df_missing['underlying_symbol'] = parsed.apply(lambda x: x['symbol'] if x else None)
                        df_missing['expiration_date'] = parsed.apply(lambda x: x['expiration_date'] if x else None)
                        df_missing['option_type'] = parsed.apply(lambda x: x['option_type'] if x else None)
                        df_missing['strike'] = parsed.apply(lambda x: x['strike'] if x else None)
                        
                        # Filter to calls
                        df_missing = df_missing[df_missing['option_type'] == 'C'].copy()
                        
                        # Convert dates
                        df_missing['date_only'] = pd.to_datetime(df_missing['window_start'] / 1_000_000_000, unit='s').dt.date
                        df_missing['expiration_date'] = pd.to_datetime(df_missing['expiration_date']).dt.date
                        
                        # Calculate days_to_expiry
                        df_missing['date_only_dt'] = pd.to_datetime(df_missing['date_only'])
                        df_missing['expiration_date_dt'] = pd.to_datetime(df_missing['expiration_date'])
                        df_missing['days_to_expiry'] = (df_missing['expiration_date_dt'] - df_missing['date_only_dt']).dt.days
                        df_missing['day_of_week'] = df_missing['date_only_dt'].dt.dayofweek
                        
                        # Filter to matching entry
                        matching = df_missing[
                            (df_missing['date_only'] == entry_date) &
                            (df_missing['expiration_date'] == exp_date) &
                            (df_missing['days_to_expiry'] == days_to_exp) &
                            (df_missing['day_of_week'] == (0 if entry_day == 'Monday' else 1))
                        ]
                        
                        if len(matching) > 0:
                            missing_entries_list.append(matching)
                            print(f"      ✅ Found {len(matching):,} rows")
                        else:
                            print(f"      ⚠️  No matching rows found")
                    else:
                        print(f"      ⚠️  No data found in S3")
                        
                except Exception as e:
                    print(f"      ❌ Error fetching: {e}")
                    continue
        
        # Combine existing and missing entries
        if missing_entries_list:
            df_missing_combined = pd.concat(missing_entries_list, ignore_index=True)
            
            # Process missing entries to match existing format
            # Rename columns
            if 'open' in df_missing_combined.columns:
                df_missing_combined = df_missing_combined.rename(columns={
                    'open': 'open_price',
                    'close': 'close_price',
                    'high': 'high_price',
                    'low': 'low_price'
                })
            
            # Divide prices by 4
            df_missing_combined['open_price'] = df_missing_combined['open_price'] / 4.0
            df_missing_combined['close_price'] = df_missing_combined['close_price'] / 4.0
            df_missing_combined['high_price'] = df_missing_combined['high_price'] / 4.0
            df_missing_combined['low_price'] = df_missing_combined['low_price'] / 4.0
            df_missing_combined['strike'] = df_missing_combined['strike'] / 4.0
            
            # Drop temporary columns
            df_missing_combined = df_missing_combined.drop(columns=['date_only_dt', 'expiration_date_dt', 'day_of_week'], errors='ignore')
            
            # Combine with existing
            df_final = pd.concat([df_existing, df_missing_combined], ignore_index=True)
            print(f"\n   ✅ Combined: {len(df_existing):,} existing + {len(df_missing_combined):,} missing = {len(df_final):,} total")
        else:
            df_final = df_existing.copy()
            print(f"\n   ⚠️  No missing entries fetched, using existing data only")
    
    # Now apply holiday filtering to get final dataset
    print(f"\n🔧 Applying holiday adjustments to final dataset...")
    
    df_final['date_only_dt'] = pd.to_datetime(df_final['date_only'])
    df_final['expiration_date_dt'] = pd.to_datetime(df_final['expiration_date'])
    
    # Rule 1: Normal Monday-Friday
    monday_friday_normal = df_final[
        (df_final['days_to_expiry'] == 4) &
        (df_final['date_only_dt'].dt.dayofweek == 0) &
        (~df_final['expiration_date'].isin(friday_holidays))
    ].copy()
    
    # Rule 2: Monday-Thursday (Friday holiday)
    monday_thursday_holiday = df_final[
        (df_final['days_to_expiry'] == 3) &
        (df_final['date_only_dt'].dt.dayofweek == 0)
    ].copy()
    monday_thursday_holiday = monday_thursday_holiday[
        monday_thursday_holiday['expiration_date'].isin(friday_holidays) |
        (monday_thursday_holiday['date_only'] + timedelta(days=4)).isin(friday_holidays)
    ].copy()
    
    # Rule 3: Tuesday-Friday (Monday holiday)
    tuesday_friday_holiday = df_final[
        (df_final['days_to_expiry'] == 3) &
        (df_final['date_only_dt'].dt.dayofweek == 1)
    ].copy()
    tuesday_friday_holiday = tuesday_friday_holiday[
        ((tuesday_friday_holiday['date_only'] - timedelta(days=1)).isin(monday_holidays)) &
        (~tuesday_friday_holiday['expiration_date'].isin(friday_holidays))
    ].copy()
    
    # Combine
    all_entries_list = []
    if len(monday_friday_normal) > 0:
        all_entries_list.append(monday_friday_normal)
    if len(monday_thursday_holiday) > 0:
        all_entries_list.append(monday_thursday_holiday)
    if len(tuesday_friday_holiday) > 0:
        all_entries_list.append(tuesday_friday_holiday)
    
    if all_entries_list:
        df_final_filtered = pd.concat(all_entries_list, ignore_index=True)
    else:
        df_final_filtered = df_final.copy()
    
    df_final_filtered = df_final_filtered.drop(columns=['date_only_dt', 'expiration_date_dt'], errors='ignore')
    
    print(f"   ✅ Final entries: {len(df_final_filtered):,}")
    print(f"      - Normal Monday-Friday: {len(monday_friday_normal):,}")
    print(f"      - Monday-Thursday (Friday holiday): {len(monday_thursday_holiday):,}")
    print(f"      - Tuesday-Friday (Monday holiday): {len(tuesday_friday_holiday):,}")
    
    # Match with historical stock prices and add premium columns
    stock_file = base_path / "data" / TICKER / f"HistoricalData_{TICKER}.csv"
    if not stock_file.exists():
        stock_files = list((base_path / "data" / TICKER).glob("HistoricalData*.csv"))
        if stock_files:
            stock_file = stock_files[0]
    
    if stock_file.exists():
        print(f"\n📈 Matching with historical stock prices...")
        try:
            df_final_filtered = add_underlying_prices_from_csv(
                df_final_filtered,
                str(stock_file),
                symbol=TICKER,
                use_pessimistic=True
            )
            
            df_final_filtered = add_premium_columns(df_final_filtered)
            
            # Add fedfunds_rate, IV, prob_itm from existing file if available
            if 'fedfunds_rate' not in df_final_filtered.columns:
                if weekly_file.exists():
                    weekly_df = pd.read_csv(weekly_file)
                    if 'fedfunds_rate' in weekly_df.columns:
                        weekly_df['date_only'] = pd.to_datetime(weekly_df['date_only'])
                        fedfunds_map = weekly_df.groupby('date_only')['fedfunds_rate'].first().to_dict()
                        df_final_filtered['date_only_dt'] = pd.to_datetime(df_final_filtered['date_only'])
                        df_final_filtered['fedfunds_rate'] = df_final_filtered['date_only_dt'].map(fedfunds_map)
                        df_final_filtered['fedfunds_rate'] = df_final_filtered['fedfunds_rate'].ffill().bfill()
                        df_final_filtered = df_final_filtered.drop(columns=['date_only_dt'], errors='ignore')
            
            # Calculate IV and prob_itm if missing
            if 'implied_volatility' not in df_final_filtered.columns or df_final_filtered['implied_volatility'].isna().all():
                print(f"   📊 Calculating implied volatility and probability ITM...")
                df_final_filtered['T'] = df_final_filtered['days_to_expiry'] / 365.0
                
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
                
                iv_prob = df_final_filtered.apply(calc_iv_prob, axis=1)
                df_final_filtered['implied_volatility'] = [x[0] for x in iv_prob]
                df_final_filtered['probability_itm'] = [x[1] for x in iv_prob]
                df_final_filtered = df_final_filtered.drop(columns=['T'])
            
        except Exception as e:
            print(f"   ⚠️  Error processing: {e}")
            import traceback
            traceback.print_exc()
    
    # Ensure all columns match reference
    reference_file = base_path / "data" / TICKER / "weekly" / "2018_options_pessimistic.csv"
    if reference_file.exists():
        reference_df = pd.read_csv(reference_file, nrows=1)
        reference_columns = list(reference_df.columns)
        
        for col in reference_columns:
            if col not in df_final_filtered.columns:
                if col == 'time_remaining_category':
                    df_final_filtered['time_remaining_category'] = 'Weekly'
                elif col == 'mid_price':
                    df_final_filtered['mid_price'] = (df_final_filtered['high_price'] + df_final_filtered['low_price']) / 2.0
                elif col == 'high_yield_pct':
                    df_final_filtered['high_yield_pct'] = (df_final_filtered['high_price'] / df_final_filtered['underlying_spot'] * 100).round(2)
                else:
                    df_final_filtered[col] = np.nan
        
        # Reorder columns
        existing_cols = [c for c in reference_columns if c in df_final_filtered.columns]
        df_final_filtered = df_final_filtered[existing_cols]
    
    # Round numeric columns
    print(f"   🔧 Rounding numeric columns...")
    exclude_cols = ['ticker', 'window_start', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 'ITM', 'time_remaining_category']
    four_decimal_cols = ['implied_volatility', 'probability_itm']
    
    for col in df_final_filtered.columns:
        if col not in exclude_cols:
            if df_final_filtered[col].dtype in ['float64', 'float32']:
                if col in four_decimal_cols:
                    df_final_filtered[col] = df_final_filtered[col].round(4)
                else:
                    df_final_filtered[col] = df_final_filtered[col].round(2)
    
    # Sort and save
    df_final_filtered['date_only'] = pd.to_datetime(df_final_filtered['date_only'])
    df_final_filtered = df_final_filtered.sort_values(['date_only', 'strike']).reset_index(drop=True)
    df_final_filtered['date_only'] = df_final_filtered['date_only'].dt.date
    df_final_filtered['expiration_date'] = pd.to_datetime(df_final_filtered['expiration_date']).dt.date
    
    output_file = output_base_dir / f"{TEST_YEAR}_options_pessimistic.csv"
    df_final_filtered.to_csv(output_file, index=False)
    
    # Verify week coverage
    final_dates = sorted(df_final_filtered['date_only'].unique())
    expected_dates = {e['entry_date'] for e in expected_entries}
    missing = expected_dates - set(final_dates)
    
    print(f"\n✅ Saved {output_file}")
    print(f"   Total rows: {len(df_final_filtered):,}")
    print(f"   Unique entry dates: {len(final_dates)}")
    print(f"   Expected entry dates: {len(expected_dates)}")
    if missing:
        print(f"   ⚠️  Still missing {len(missing)} expected weeks:")
        for d in sorted(missing)[:5]:
            print(f"      {d}")
    else:
        print(f"   ✅ All expected weeks are present!")

print("\n" + "=" * 80)
print("✅ ALL YEARS PROCESSED!")
print("=" * 80)


