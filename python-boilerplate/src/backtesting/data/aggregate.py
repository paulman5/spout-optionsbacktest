import os
import sys
import duckdb
import time
import pandas as pd
from datetime import datetime, date
from dotenv import load_dotenv

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("🚀 Starting aggregation script...", flush=True)

# ------------------------------------------------------------
# 1. Load environment variables
# ------------------------------------------------------------

print("   Loading environment variables...", flush=True)
load_dotenv()

S3_ACCESS_KEY = os.getenv("MASSIVE_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("MASSIVE_API_KEY")
S3_ENDPOINT   = os.getenv("MASSIVE_S3_ENDPOINT")
S3_BUCKET     = os.getenv("MASSIVE_S3_BUCKET")
S3_REGION     = os.getenv("MASSIVE_S3_REGION", "us-east-1")

# Hard fail if anything is missing
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

print("   Connecting to DuckDB...", flush=True)
DB_PATH = "options.duckdb"

try:
    con = duckdb.connect(DB_PATH)
    print(f"✅ DuckDB connected ({DB_PATH})", flush=True)
except Exception as e:
    print(f"❌ Error connecting to DuckDB: {e}", flush=True)
    raise

# ------------------------------------------------------------
# 3. Enable S3 support
# ------------------------------------------------------------

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Remove protocol from endpoint - DuckDB adds it automatically
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
# 4. Test S3 access (read ONE row)
# ------------------------------------------------------------

TEST_YEAR = 2022

# ------------------------------------------------------------
# Configure which tickers to filter (empty list = all tickers)
# ------------------------------------------------------------
# Add ticker symbols you want to filter (e.g., ['SPY', 'TSLA', 'AAPL'])
# Leave empty [] to process all tickers (slower but complete)
TICKERS_TO_FILTER = ['TSLA']  # Add your desired tickers here

# ------------------------------------------------------------
# Configure entry days for backtesting (days_to_expiry at entry)
# ------------------------------------------------------------
# For backtesting, we want to simulate entering positions at specific days before expiration
# Use a range (min, max) to allow flexibility - exact days may not always be available
# Set to None to include all days (not recommended for backtesting)
# Weekly options: enter on Monday with exactly 5 days to expiry (one row per contract)
# Monthly options: typically enter 20-30 days before expiration
WEEKLY_ENTRY_DAYS_RANGE = 5  # Weekly options: enter Monday with exactly 5 days to expiry
MONTHLY_ENTRY_DAYS_RANGE = (28, 32)  # Enter monthly options 28-32 days before expiration

if TICKERS_TO_FILTER:
    print(f"🎯 Filtering for tickers: {', '.join(TICKERS_TO_FILTER)}", flush=True)
else:
    print("⚠️  No ticker filter - processing ALL tickers (this will be slow)", flush=True)

if WEEKLY_ENTRY_DAYS_RANGE is not None or MONTHLY_ENTRY_DAYS_RANGE is not None:
    print(f"📅 Entry day filtering enabled:", flush=True)
    if WEEKLY_ENTRY_DAYS_RANGE is not None:
        if isinstance(WEEKLY_ENTRY_DAYS_RANGE, tuple):
            min_days, max_days = WEEKLY_ENTRY_DAYS_RANGE
            print(f"   Weekly options: entry at {min_days}-{max_days} days to expiry", flush=True)
        else:
            print(f"   Weekly options: entry at max days_to_expiry per contract (one row per contract)", flush=True)
    if MONTHLY_ENTRY_DAYS_RANGE is not None:
        min_days, max_days = MONTHLY_ENTRY_DAYS_RANGE
        print(f"   Monthly options: entry at {min_days}-{max_days} days to expiry", flush=True)
else:
    print("⚠️  No entry day filtering - will include all days_to_expiry (not recommended for backtesting)", flush=True)

print(f"🔍 Testing S3 access for {TEST_YEAR}...")

# First, find files using glob
files = con.execute(f"""
SELECT file 
FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/*/*.csv.gz')
LIMIT 1;
""").fetchall()

if not files:
    raise RuntimeError(f"❌ No files found for {TEST_YEAR}")

test_file = files[0][0]
print(f"   Found test file: {test_file}")

test_df = con.execute(f"""
SELECT *
FROM read_csv_auto('{test_file}', compression='gzip')
LIMIT 1;
""").df()

if test_df.empty:
    raise RuntimeError("❌ S3 access succeeded but returned no rows")

print("✅ S3 access confirmed")
print(test_df.head(1))

# ------------------------------------------------------------
# 5. Aggregate ONE full year into DuckDB
# ------------------------------------------------------------

print(f"📦 Aggregating options day aggregates for {TEST_YEAR}...")
script_start_time = time.time()

# Check if table already exists (skip S3 download if it does)
table_name = f"options_day_aggs_{TEST_YEAR}"
table_exists = False
try:
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    if result and result[0] > 0:
        table_exists = True
        print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Table {table_name} already exists with {result[0]:,} rows - skipping S3 download", flush=True)
except:
    pass

if not table_exists:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] Starting file discovery...")
    
    # Get all files for the year
    all_files = con.execute(f"""
    SELECT file 
    FROM glob('s3://{S3_BUCKET}/us_options_opra/day_aggs_v1/{TEST_YEAR}/*/*.csv.gz')
    ORDER BY file;
    """).fetchall()
    
    if not all_files:
        raise RuntimeError(f"❌ No files found for {TEST_YEAR}")
    
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Found {len(all_files)} files for {TEST_YEAR}")
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] Building file list...")
    
    # Build UNION ALL query to read all files
    file_paths = [f"'{row[0]}'" for row in all_files]
    file_list = ',\n    '.join(file_paths)
    
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⏳ Reading and aggregating all {len(all_files)} files...", flush=True)
    if TICKERS_TO_FILTER:
        print(f"   [{datetime.now().strftime('%H:%M:%S')}] Filtering for ticker(s): {', '.join(TICKERS_TO_FILTER)} - this will save significant time!", flush=True)
        print(f"   [{datetime.now().strftime('%H:%M:%S')}] Note: DuckDB still reads all files but filters in memory (still faster than processing all data)", flush=True)
    else:
        print(f"   [{datetime.now().strftime('%H:%M:%S')}] This may take a while (processing from S3)...", flush=True)
    start_time = time.time()
    
    # Build WHERE clause for ticker filtering
    where_clause = ""
    if TICKERS_TO_FILTER:
        # Options tickers in the data are in format like "O:SPY190118C00250000"
        # Format: O:SYMBOL + expiration + C/P + strike
        # We filter by checking if ticker starts with "O:" followed by our symbol
        ticker_conditions = " OR ".join([f"ticker LIKE 'O:{ticker}%'" for ticker in TICKERS_TO_FILTER])
        where_clause = f"WHERE ({ticker_conditions})"
        print(f"   [{datetime.now().strftime('%H:%M:%S')}] SQL filter: {where_clause}", flush=True)
    
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⏳ Starting data read from S3 (this may take several minutes)...", flush=True)
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] Processing {len(all_files)} files...", flush=True)
    
    con.execute(f"""
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT *
    FROM read_csv_auto([
        {file_list}
    ], compression='gzip')
    {where_clause};
    """)
    
    elapsed = time.time() - start_time
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Finished reading files (took {elapsed:.1f} seconds / {elapsed/60:.1f} minutes)", flush=True)

print(f"   [{datetime.now().strftime('%H:%M:%S')}] Counting rows...", flush=True)

row_count = con.execute(
    f"SELECT COUNT(*) FROM {table_name}"
).fetchone()[0]

print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Aggregated {row_count:,} rows into {table_name}")

# ------------------------------------------------------------
# 6. Export ONE Parquet file (recommended)
# ------------------------------------------------------------

OUTPUT_FILE = f"options_day_aggs_{TEST_YEAR}.parquet"

print(f"💾 Writing {OUTPUT_FILE}...")
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⏳ Exporting {row_count:,} rows to Parquet format...")
export_start = time.time()

con.execute(f"""
COPY options_day_aggs_{TEST_YEAR}
TO '{OUTPUT_FILE}'
(FORMAT PARQUET);
""")

export_elapsed = time.time() - export_start
file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)  # Size in MB
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {OUTPUT_FILE} ({file_size:.1f} MB, took {export_elapsed:.1f} seconds)")

# ------------------------------------------------------------
# 7. Clean and categorize the data
# ------------------------------------------------------------

print(f"🧹 Cleaning and categorizing data...", flush=True)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Parsing option tickers and categorizing...", flush=True)

# Load data into pandas for processing
df = con.execute(f"SELECT * FROM options_day_aggs_{TEST_YEAR}").df()

# Parse tickers and add expiration dates, strikes, etc.
import pandas as pd
from datetime import date

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

# Parse tickers
parsed = df['ticker'].apply(parse_option_ticker)
df['underlying_symbol'] = parsed.apply(lambda x: x['symbol'] if x else None)
df['expiration_date'] = parsed.apply(lambda x: x['expiration_date'] if x else None)
df['option_type'] = parsed.apply(lambda x: x['option_type'] if x else None)
df['strike'] = parsed.apply(lambda x: x['strike'] if x else None)

# Convert window_start to date_only
df['date_only'] = pd.to_datetime(df['window_start'] / 1_000_000_000, unit='s').dt.date
df['expiration_date'] = pd.to_datetime(df['expiration_date'])

# Calculate days_to_expiry
df['days_to_expiry'] = (df['expiration_date'] - pd.to_datetime(df['date_only'])).dt.days

# Categorize expiration type
# First pass: identify Monthly (3rd Friday) and potential Weekly (other Fridays)
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
        return 'Weekly_Candidate'  # Could be weekly or long-term
    return 'Other'

df['expiration_category'] = df.apply(categorize_expiration_preliminary, axis=1)

# Second pass: Distinguish Weekly from Long-term
# Weekly options are short-term - they typically have max days_to_expiry <= 60 days
# Long-term options (LEAPS) will have much longer max days_to_expiry (90+ days)
# We use a higher threshold because weekly options can be listed/tracked several weeks before expiration
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Distinguishing weekly vs long-term options...", flush=True)
weekly_candidates = df[df['expiration_category'] == 'Weekly_Candidate'].copy()
if len(weekly_candidates) > 0:
    # For each expiration_date, find the maximum days_to_expiry
    max_dte_by_exp = weekly_candidates.groupby('expiration_date')['days_to_expiry'].max().reset_index()
    max_dte_by_exp.columns = ['expiration_date', 'max_days_to_expiry']
    
    # Weekly options: max days_to_expiry < 120 days (about 4 months)
    # Long-term options (LEAPS): max days_to_expiry >= 120 days (6+ months)
    # This is more lenient to catch all weekly options throughout the year
    WEEKLY_MAX_DTE_THRESHOLD = 120  # If max days_to_expiry < 120, it's a weekly option
    
    weekly_exp_dates = set(max_dte_by_exp[max_dte_by_exp['max_days_to_expiry'] < WEEKLY_MAX_DTE_THRESHOLD]['expiration_date'])
    longterm_exp_dates = set(max_dte_by_exp[max_dte_by_exp['max_days_to_expiry'] >= WEEKLY_MAX_DTE_THRESHOLD]['expiration_date'])
    
    # Update categorization
    df.loc[df['expiration_date'].isin(weekly_exp_dates), 'expiration_category'] = 'Weekly'
    df.loc[df['expiration_date'].isin(longterm_exp_dates), 'expiration_category'] = 'Long-term'
    
    print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly options: {len(weekly_exp_dates)} expiration dates", flush=True)
    print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Long-term options: {len(longterm_exp_dates)} expiration dates", flush=True)
    if len(weekly_exp_dates) > 0:
        sample_weekly = sorted([str(d) for d in list(weekly_exp_dates)])[:10]
        print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Sample weekly expirations: {sample_weekly}", flush=True)

# Calculate friday_number (which Friday of the month)
def calculate_friday_number(exp_date):
    """Calculate which Friday of the month (1st, 2nd, 3rd, 4th, or 5th)."""
    if exp_date.dayofweek != 4:  # Not a Friday
        # For Thursday before 3rd Friday, return 3
        if exp_date.dayofweek == 3:
            first_day = exp_date.replace(day=1)
            days_until_friday = (4 - first_day.weekday()) % 7
            first_friday = first_day.replace(day=1 + days_until_friday)
            third_friday = first_friday.replace(day=first_friday.day + 14)
            if exp_date.date() == (third_friday - pd.Timedelta(days=1)).date():
                return 3
        return None
    
    first_day = exp_date.replace(day=1)
    days_until_friday = (4 - first_day.weekday()) % 7
    first_friday = first_day.replace(day=1 + days_until_friday)
    
    # Calculate which Friday
    friday_number = ((exp_date.day - first_friday.day) // 7) + 1
    return friday_number

df['friday_number'] = df['expiration_date'].apply(calculate_friday_number)

# Calculate exp_day_of_week (0=Monday, 4=Friday, etc.)
df['exp_day_of_week'] = df['expiration_date'].dt.dayofweek

# option_type_actual is the same as expiration_category
df['option_type_actual'] = df['expiration_category']

# Calculate time_remaining_category based on days_to_expiry
# IMPORTANT: Weekly options should never be classified as "Long-term"
def categorize_time_remaining(days, expiration_category):
    """Categorize time remaining based on days to expiry.
    Weekly options are capped at 'Monthly' to prevent misclassification as long-term."""
    if days == 0:
        return '0-DTE'
    elif 1 <= days <= 7:
        return 'Weekly'
    elif 8 <= days <= 14:
        return 'Bi-weekly'
    elif 15 <= days <= 35:
        return 'Monthly'
    else:
        # Weekly options should never be "Long-term" - cap them at "Monthly"
        if expiration_category == 'Weekly':
            return 'Monthly'
        return 'Long-term'

df['time_remaining_category'] = df.apply(
    lambda row: categorize_time_remaining(row['days_to_expiry'], row['expiration_category']), 
    axis=1
)

# Filter to only Weekly and Monthly (exclude Long-term and Other)
df = df[df['expiration_category'].isin(['Weekly', 'Monthly'])].copy()
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Filtered to Weekly and Monthly options only", flush=True)

# Filter to symbol if specified
if TICKERS_TO_FILTER:
    df = df[df['underlying_symbol'].isin(TICKERS_TO_FILTER)].copy()

# Filter to specific entry days for backtesting
# For weekly options: keep only the entry row (max days_to_expiry per contract) with exactly 5 days
# For monthly options: filter to entry day range
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Filtering to entry days for backtesting...", flush=True)
rows_before = len(df)

# For weekly options: filter to exactly 5 trading days (Monday-Friday before expiration)
# days_to_expiry should be 1-5 AND date_only must be a weekday (Monday=0, Friday=4)
if WEEKLY_ENTRY_DAYS_RANGE is not None:
    weekly_df = df[df['expiration_category'] == 'Weekly'].copy()
    if len(weekly_df) > 0:
        # Convert date_only to datetime to check day of week
        weekly_df['date_only_dt'] = pd.to_datetime(weekly_df['date_only'])
        weekly_df['day_of_week'] = weekly_df['date_only_dt'].dt.dayofweek  # Monday=0, Friday=4
        
        # Filter to ONLY Monday entries (days_to_expiry=4) for each expiration
        # Monday entry: days_to_expiry=4, and date_only must be a Monday (day_of_week=0)
        weekly_entry = weekly_df[
            (weekly_df['days_to_expiry'] == 4) &   # Only Monday entry (4 days to expiry)
            (weekly_df['day_of_week'] == 0)        # Must be a Monday (strictly Monday)
        ].copy()
        
        # Drop the temporary columns
        weekly_entry = weekly_entry.drop(columns=['date_only_dt', 'day_of_week'])
        
        print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly: {len(weekly_entry):,} rows (Monday entries only, days_to_expiry=4)", flush=True)
        if len(weekly_entry) > 0:
            dte_stats = weekly_entry['days_to_expiry'].describe()
            dte_dist = weekly_entry['days_to_expiry'].value_counts().sort_index()
            print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly days_to_expiry distribution: {dict(dte_dist)}", flush=True)
            # Verify all dates are weekdays
            date_dt = pd.to_datetime(weekly_entry['date_only'])
            weekend_count = len(weekly_entry[(date_dt.dt.dayofweek >= 5)])
            if weekend_count > 0:
                print(f"   [{datetime.now().strftime('%H:%M:%S')}]    ⚠️  WARNING: Found {weekend_count} rows with weekend dates!", flush=True)
            else:
                print(f"   [{datetime.now().strftime('%H:%M:%S')}]    ✅ All dates are weekdays (Monday-Friday)", flush=True)
    else:
        weekly_entry = pd.DataFrame(columns=df.columns)

# For monthly options: filter to entry day range
if MONTHLY_ENTRY_DAYS_RANGE is not None:
    min_days, max_days = MONTHLY_ENTRY_DAYS_RANGE
    monthly_df = df[df['expiration_category'] == 'Monthly'].copy()
    if len(monthly_df) > 0:
        monthly_entry = monthly_df[
            (monthly_df['days_to_expiry'] >= min_days) & 
            (monthly_df['days_to_expiry'] <= max_days)
        ].copy()
        print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Monthly: {len(monthly_entry):,} rows ({min_days}-{max_days} days to expiry)", flush=True)
    else:
        monthly_entry = pd.DataFrame(columns=df.columns)

# Combine weekly and monthly
if WEEKLY_ENTRY_DAYS_RANGE is not None and MONTHLY_ENTRY_DAYS_RANGE is not None:
    df = pd.concat([weekly_entry, monthly_entry], ignore_index=True)
elif WEEKLY_ENTRY_DAYS_RANGE is not None:
    df = weekly_entry
elif MONTHLY_ENTRY_DAYS_RANGE is not None:
    df = monthly_entry

rows_after = len(df)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Filtered from {rows_before:,} to {rows_after:,} rows (entry day filtering)", flush=True)
if rows_after == 0:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  WARNING: No rows match entry day criteria!", flush=True)
    if WEEKLY_ENTRY_DAYS_RANGE:
        if isinstance(WEEKLY_ENTRY_DAYS_RANGE, tuple):
            print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly range: {WEEKLY_ENTRY_DAYS_RANGE}", flush=True)
        else:
            print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly: entry at exactly {WEEKLY_ENTRY_DAYS_RANGE} days", flush=True)
    if MONTHLY_ENTRY_DAYS_RANGE:
        print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Monthly range: {MONTHLY_ENTRY_DAYS_RANGE}", flush=True)

# Rename columns to match expected names
df = df.rename(columns={
    'open': 'open_price',
    'close': 'close_price',
    'high': 'high_price',
    'low': 'low_price'
})

# Create expiration metadata lookup table (to avoid redundancy)
# expiration_category and friday_number are the same for all rows with the same expiration_date
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Creating expiration metadata lookup...", flush=True)
expiration_metadata = df[['expiration_date', 'expiration_category', 'friday_number']].drop_duplicates(
    subset=['expiration_date']
).copy()
expiration_metadata = expiration_metadata.sort_values('expiration_date').reset_index(drop=True)

# Select columns for main data table (removed redundant expiration_category and friday_number)
# These can be joined from expiration_metadata if needed
essential_columns = [
    'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type',
    'strike', 'volume', 'open_price', 'close_price', 'high_price', 'low_price', 
    'transactions', 'window_start', 'days_to_expiry', 'time_remaining_category'
]

df_clean = df[essential_columns].copy()

# Split into weekly and monthly for separate backtesting
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Splitting into weekly and monthly datasets...", flush=True)
df_with_metadata = df_clean.merge(expiration_metadata, on='expiration_date', how='left')
df_weekly = df_with_metadata[df_with_metadata['expiration_category'] == 'Weekly'][essential_columns].copy()
df_monthly = df_with_metadata[df_with_metadata['expiration_category'] == 'Monthly'][essential_columns].copy()

print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly: {len(df_weekly):,} rows", flush=True)
print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Monthly: {len(df_monthly):,} rows", flush=True)

# Save cleaned data back to DuckDB
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Saving cleaned data...", flush=True)
con.execute("CREATE OR REPLACE TABLE options_day_aggs_cleaned AS SELECT * FROM df_clean")
con.register('df_weekly', df_weekly)
con.execute("CREATE OR REPLACE TABLE options_day_aggs_weekly AS SELECT * FROM df_weekly")
con.register('df_monthly', df_monthly)
con.execute("CREATE OR REPLACE TABLE options_day_aggs_monthly AS SELECT * FROM df_monthly")

# Save expiration metadata to DuckDB (register the DataFrame first)
con.register('expiration_metadata_df', expiration_metadata)
con.execute("CREATE OR REPLACE TABLE expiration_metadata AS SELECT * FROM expiration_metadata_df")

# Export weekly options - CSV
WEEKLY_CSV = f"options_day_aggs_{TEST_YEAR}_weekly.csv"
df_weekly.to_csv(WEEKLY_CSV, index=False)
weekly_csv_size = os.path.getsize(WEEKLY_CSV) / (1024 * 1024)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {WEEKLY_CSV} ({weekly_csv_size:.1f} MB, {len(df_weekly):,} rows)", flush=True)

# Export weekly options - Parquet
WEEKLY_PARQUET = f"options_day_aggs_{TEST_YEAR}_weekly.parquet"
con.execute(f"COPY options_day_aggs_weekly TO '{WEEKLY_PARQUET}' (FORMAT PARQUET)")
weekly_parquet_size = os.path.getsize(WEEKLY_PARQUET) / (1024 * 1024)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {WEEKLY_PARQUET} ({weekly_parquet_size:.1f} MB)", flush=True)

# Export weekly options - Excel
try:
    WEEKLY_EXCEL = f"options_day_aggs_{TEST_YEAR}_weekly.xlsx"
    df_weekly.to_excel(WEEKLY_EXCEL, index=False, engine='openpyxl')
    weekly_excel_size = os.path.getsize(WEEKLY_EXCEL) / (1024 * 1024)
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {WEEKLY_EXCEL} ({weekly_excel_size:.1f} MB)", flush=True)
except ImportError:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Skipping Excel export (openpyxl not installed)", flush=True)
except Exception as e:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Error exporting Excel: {e}", flush=True)

# Export monthly options - CSV
MONTHLY_CSV = f"options_day_aggs_{TEST_YEAR}_monthly.csv"
df_monthly.to_csv(MONTHLY_CSV, index=False)
monthly_csv_size = os.path.getsize(MONTHLY_CSV) / (1024 * 1024)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {MONTHLY_CSV} ({monthly_csv_size:.1f} MB, {len(df_monthly):,} rows)", flush=True)

# Export monthly options - Parquet
MONTHLY_PARQUET = f"options_day_aggs_{TEST_YEAR}_monthly.parquet"
con.execute(f"COPY options_day_aggs_monthly TO '{MONTHLY_PARQUET}' (FORMAT PARQUET)")
monthly_parquet_size = os.path.getsize(MONTHLY_PARQUET) / (1024 * 1024)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {MONTHLY_PARQUET} ({monthly_parquet_size:.1f} MB)", flush=True)

# Export monthly options - Excel
try:
    MONTHLY_EXCEL = f"options_day_aggs_{TEST_YEAR}_monthly.xlsx"
    df_monthly.to_excel(MONTHLY_EXCEL, index=False, engine='openpyxl')
    monthly_excel_size = os.path.getsize(MONTHLY_EXCEL) / (1024 * 1024)
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {MONTHLY_EXCEL} ({monthly_excel_size:.1f} MB)", flush=True)
except ImportError:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Skipping Excel export (openpyxl not installed)", flush=True)
except Exception as e:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Error exporting Excel: {e}", flush=True)

# Export expiration metadata
METADATA_CSV = f"expiration_metadata_{TEST_YEAR}.csv"
expiration_metadata.to_csv(METADATA_CSV, index=False)
metadata_size = os.path.getsize(METADATA_CSV) / (1024 * 1024)
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {METADATA_CSV} ({metadata_size:.2f} MB, {len(expiration_metadata)} unique expirations)", flush=True)

METADATA_PARQUET = f"expiration_metadata_{TEST_YEAR}.parquet"
con.execute(f"COPY expiration_metadata TO '{METADATA_PARQUET}' (FORMAT PARQUET)")
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {METADATA_PARQUET}", flush=True)

# Export expiration metadata Excel
try:
    METADATA_EXCEL = f"expiration_metadata_{TEST_YEAR}.xlsx"
    expiration_metadata.to_excel(METADATA_EXCEL, index=False, engine='openpyxl')
    metadata_excel_size = os.path.getsize(METADATA_EXCEL) / (1024 * 1024)
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Wrote {METADATA_EXCEL} ({metadata_excel_size:.2f} MB)", flush=True)
except ImportError:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Skipping Excel export (openpyxl not installed)", flush=True)
except Exception as e:
    print(f"   [{datetime.now().strftime('%H:%M:%S')}] ⚠️  Error exporting Excel: {e}", flush=True)

# Final summary
print(f"   [{datetime.now().strftime('%H:%M:%S')}] ✅ Export complete!", flush=True)
print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Weekly options: {len(df_weekly):,} rows", flush=True)
print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Monthly options: {len(df_monthly):,} rows", flush=True)
print(f"   [{datetime.now().strftime('%H:%M:%S')}]    Unique expiration dates: {len(expiration_metadata):,}", flush=True)

# ------------------------------------------------------------
# 8. Done
# ------------------------------------------------------------

total_elapsed = time.time() - script_start_time
print(f"🎉 DuckDB aggregation and cleaning completed successfully")
print(f"   [{datetime.now().strftime('%H:%M:%S')}] Total time: {total_elapsed:.1f} seconds ({total_elapsed/60:.1f} minutes)")
