#!/usr/bin/env python3
"""
Process existing weekly CSV files with holiday adjustments:
- Read existing weekly files
- Apply holiday adjustments (Monday-Thursday for Friday holidays, Tuesday-Friday for Monday holidays)
- Ensure all columns match reference file
- Save to holidays folder
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path
import sys
import importlib.util

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

TICKER = 'AAPL'
YEARS = list(range(2016, 2026))  # 2016-2025

# Market holidays by year
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
    
    # Veterans Day (if Monday)
    vet = date(year, 11, 11)
    if vet.weekday() == 0:
        holidays.append(vet)
    elif vet.weekday() == 6:
        holidays.append(vet + timedelta(days=1))
    
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
    
    # Good Friday dates
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
    
    # Day after Thanksgiving (Black Friday)
    thanksgiving = date(year, 11, 22)
    while thanksgiving.weekday() != 3:  # Thursday
        thanksgiving += timedelta(days=1)
    black_friday = thanksgiving + timedelta(days=1)
    holidays.append(black_friday)
    
    return sorted(holidays)

print("=" * 80)
print(f"PROCESSING EXISTING WEEKLY FILES WITH HOLIDAY ADJUSTMENTS")
print("=" * 80)
print(f"Ticker: {TICKER}")
print(f"Years: {YEARS[0]}-{YEARS[-1]}")
print()

# Load helper functions
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

# Reference file for column order
reference_file = base_path / "data" / TICKER / "weekly" / "2018_options_pessimistic.csv"
if reference_file.exists():
    reference_df = pd.read_csv(reference_file, nrows=1)
    reference_columns = list(reference_df.columns)
    print(f"📋 Reference columns ({len(reference_columns)}): {', '.join(reference_columns[:5])}...")
else:
    reference_columns = None
    print("⚠️  Reference file not found, will use default column order")

# Output directory
output_base_dir = base_path / "data" / TICKER / "holidays"
output_base_dir.mkdir(parents=True, exist_ok=True)

for TEST_YEAR in YEARS:
    print("\n" + "=" * 80)
    print(f"PROCESSING YEAR {TEST_YEAR}")
    print("=" * 80)
    
    # Get holidays
    monday_holidays = get_monday_holidays(TEST_YEAR)
    friday_holidays = get_friday_holidays(TEST_YEAR)
    
    print(f"\n📅 Holidays for {TEST_YEAR}:")
    print(f"   Monday holidays: {len(monday_holidays)}")
    print(f"   Friday holidays: {len(friday_holidays)}")
    
    # Load existing weekly file
    weekly_file = base_path / "data" / TICKER / "weekly" / f"{TEST_YEAR}_options_pessimistic.csv"
    
    if not weekly_file.exists():
        print(f"   ⚠️  File not found: {weekly_file}, skipping...")
        continue
    
    print(f"\n📥 Loading {weekly_file.name}...")
    df = pd.read_csv(weekly_file)
    print(f"   Loaded {len(df):,} rows")
    
    # Convert dates
    df['date_only'] = pd.to_datetime(df['date_only']).dt.date
    df['expiration_date'] = pd.to_datetime(df['expiration_date']).dt.date
    
    # Filter for holiday-adjusted entries
    print(f"\n🔧 Applying holiday adjustments...")
    
    # Convert to datetime for easier manipulation
    df['date_only_dt'] = pd.to_datetime(df['date_only'])
    df['expiration_date_dt'] = pd.to_datetime(df['expiration_date'])
    
    # Rule 1: Normal Monday-Friday entries (days_to_expiry=4, Monday entry, Friday NOT a holiday)
    print(f"   Rule 1: Normal Monday-Friday entries (days_to_expiry=4, Monday entry, Friday NOT holiday)...")
    monday_friday = df[
        (df['days_to_expiry'] == 4) &
        (df['date_only_dt'].dt.dayofweek == 0)  # Monday
    ].copy()
    
    monday_friday_normal = []
    for _, row in monday_friday.iterrows():
        entry_date = row['date_only']
        expiration_date = row['expiration_date']
        # Only include if Friday expiration is NOT a holiday
        if expiration_date not in friday_holidays:
            monday_friday_normal.append(row)
    
    monday_friday_normal_df = pd.DataFrame(monday_friday_normal)
    print(f"   ✅ Normal Monday-Friday: {len(monday_friday_normal_df):,} rows")
    
    # Rule 2: Monday-Thursday entries (days_to_expiry=3, Monday entry, Friday IS a holiday)
    print(f"   Rule 2: Monday-Thursday entries (days_to_expiry=3, Monday entry, Friday IS holiday)...")
    monday_thursday = df[
        (df['days_to_expiry'] == 3) &
        (df['date_only_dt'].dt.dayofweek == 0)  # Monday
    ].copy()
    
    monday_thursday_holiday = []
    for _, row in monday_thursday.iterrows():
        entry_date = row['date_only']
        expiration_date = row['expiration_date']
        # Check if the expiration Friday is a holiday
        friday_exp = entry_date + timedelta(days=4)
        if friday_exp in friday_holidays or expiration_date in friday_holidays:
            monday_thursday_holiday.append(row)
    
    monday_thursday_holiday_df = pd.DataFrame(monday_thursday_holiday)
    print(f"   ✅ Monday-Thursday (Friday holiday): {len(monday_thursday_holiday_df):,} rows")
    
    # Rule 3: Tuesday-Friday entries (days_to_expiry=3, Tuesday entry, Monday WAS a holiday)
    print(f"   Rule 3: Tuesday-Friday entries (days_to_expiry=3, Tuesday entry, Monday WAS holiday)...")
    tuesday_friday = df[
        (df['days_to_expiry'] == 3) &
        (df['date_only_dt'].dt.dayofweek == 1)  # Tuesday
    ].copy()
    
    tuesday_friday_holiday = []
    for _, row in tuesday_friday.iterrows():
        entry_date = row['date_only']
        expiration_date = row['expiration_date']
        monday_before = entry_date - timedelta(days=1)
        # Check if Monday was a holiday AND Friday expiration is NOT a holiday
        if monday_before in monday_holidays and expiration_date not in friday_holidays:
            tuesday_friday_holiday.append(row)
    
    tuesday_friday_holiday_df = pd.DataFrame(tuesday_friday_holiday)
    print(f"   ✅ Tuesday-Friday (Monday holiday): {len(tuesday_friday_holiday_df):,} rows")
    
    # Combine all entries
    all_entries_list = []
    if len(monday_friday_normal_df) > 0:
        all_entries_list.append(monday_friday_normal_df)
    if len(monday_thursday_holiday_df) > 0:
        all_entries_list.append(monday_thursday_holiday_df)
    if len(tuesday_friday_holiday_df) > 0:
        all_entries_list.append(tuesday_friday_holiday_df)
    
    if all_entries_list:
        df_final = pd.concat(all_entries_list, ignore_index=True)
    else:
        print(f"   ⚠️  No entries found for {TEST_YEAR}, skipping...")
        continue
    
    # Drop temporary columns
    df_final = df_final.drop(columns=['date_only_dt'], errors='ignore')
    
    print(f"   ✅ Total entries: {len(df_final):,}")
    
    # Check for missing weeks
    print(f"\n   📅 Checking week coverage...")
    entry_dates = set(df_final['date_only'])
    
    # Calculate expected weeks
    expected_weeks = []
    current_date = date(TEST_YEAR, 1, 1)
    end_date = date(TEST_YEAR, 12, 31)
    
    def is_third_friday(exp_date):
        first_day = exp_date.replace(day=1)
        days_until_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_until_friday)
        third_friday = first_friday + timedelta(days=14)
        return exp_date == third_friday
    
    while current_date <= end_date:
        if current_date.weekday() == 0:  # Monday
            if current_date not in monday_holidays:
                # Normal Monday - check Friday expiration
                friday_exp = current_date + timedelta(days=4)
                if not is_third_friday(friday_exp):
                    if friday_exp in friday_holidays:
                        # Friday is holiday - Monday-Thursday
                        expected_weeks.append(('Monday-Thursday', current_date))
                    else:
                        # Normal Monday-Friday
                        expected_weeks.append(('Monday-Friday', current_date))
            else:
                # Monday is holiday - Tuesday-Friday
                tuesday_entry = current_date + timedelta(days=1)
                friday_exp = tuesday_entry + timedelta(days=3)
                if not is_third_friday(friday_exp) and friday_exp not in friday_holidays:
                    expected_weeks.append(('Tuesday-Friday', tuesday_entry))
        elif current_date.weekday() == 1:  # Tuesday
            monday_before = current_date - timedelta(days=1)
            if monday_before in monday_holidays:
                friday_exp = current_date + timedelta(days=3)
                if not is_third_friday(friday_exp) and friday_exp not in friday_holidays:
                    expected_weeks.append(('Tuesday-Friday', current_date))
        current_date += timedelta(days=1)
    
    expected_entry_dates = {ed for _, ed in expected_weeks}
    missing_weeks = expected_entry_dates - entry_dates
    
    if missing_weeks:
        print(f"   ⚠️  Missing {len(missing_weeks)} expected weeks:")
        for missing_date in sorted(missing_weeks)[:10]:
            print(f"      {missing_date} ({missing_date.strftime('%A')})")
        if len(missing_weeks) > 10:
            print(f"      ... and {len(missing_weeks) - 10} more")
    else:
        print(f"   ✅ All expected weeks are present ({len(expected_entry_dates)} weeks)")
    
    # Ensure all columns from reference are present
    if reference_columns:
        for col in reference_columns:
            if col not in df_final.columns:
                if col == 'time_remaining_category':
                    df_final['time_remaining_category'] = 'Weekly'
                elif col == 'mid_price':
                    df_final['mid_price'] = (df_final['high_price'] + df_final['low_price']) / 2.0
                elif col == 'high_yield_pct':
                    if 'underlying_spot' in df_final.columns:
                        df_final['high_yield_pct'] = (df_final['high_price'] / df_final['underlying_spot'] * 100).round(2)
                    else:
                        df_final['high_yield_pct'] = np.nan
                else:
                    df_final[col] = np.nan
        
        # Reorder columns
        existing_cols = [c for c in reference_columns if c in df_final.columns]
        df_final = df_final[existing_cols]
    
    # Sort by date and strike
    df_final['date_only'] = pd.to_datetime(df_final['date_only'])
    df_final = df_final.sort_values(['date_only', 'strike']).reset_index(drop=True)
    df_final['date_only'] = df_final['date_only'].dt.date
    df_final['expiration_date'] = pd.to_datetime(df_final['expiration_date']).dt.date
    
    # Round numeric columns: 4 decimals for IV & prob_itm, 2 for others (except window_start and ticker)
    print(f"   🔧 Rounding numeric columns...")
    exclude_cols = ['ticker', 'window_start', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 'ITM', 'time_remaining_category']
    four_decimal_cols = ['implied_volatility', 'probability_itm']
    
    for col in df_final.columns:
        if col not in exclude_cols:
            if df_final[col].dtype in ['float64', 'float32']:
                if col in four_decimal_cols:
                    df_final[col] = df_final[col].round(4)
                else:
                    df_final[col] = df_final[col].round(2)
            elif df_final[col].dtype == 'object':
                # Check if it's a numeric string
                try:
                    numeric_vals = pd.to_numeric(df_final[col], errors='coerce')
                    if numeric_vals.notna().any():
                        if col in four_decimal_cols:
                            df_final[col] = numeric_vals.round(4)
                        else:
                            df_final[col] = numeric_vals.round(2)
                except:
                    pass
    
    # Save to holidays folder
    output_file = output_base_dir / f"{TEST_YEAR}_options_pessimistic.csv"
    df_final.to_csv(output_file, index=False)
    
    print(f"\n✅ Saved {output_file}")
    print(f"   Total rows: {len(df_final):,}")
    print(f"   Columns: {len(df_final.columns)}")
    if reference_columns:
        matches = list(df_final.columns) == reference_columns
        print(f"   Matches reference: {matches}")

print("\n" + "=" * 80)
print("✅ ALL YEARS PROCESSED!")
print("=" * 80)
print(f"Output directory: {output_base_dir}")

