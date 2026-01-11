#!/usr/bin/env python3
"""
Check if all tickers with holiday options data have all 52 weeks covered.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict

data_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data")

# Find all tickers with holidays directory
tickers_with_holidays = []
for ticker_dir in data_dir.iterdir():
    if ticker_dir.is_dir():
        holidays_dir = ticker_dir / "holidays"
        if holidays_dir.exists() and holidays_dir.is_dir():
            tickers_with_holidays.append(ticker_dir.name)

print(f"Found {len(tickers_with_holidays)} tickers with holidays directory")
print(f"Tickers: {sorted(tickers_with_holidays)}\n")

results = defaultdict(dict)

for ticker in sorted(tickers_with_holidays):
    holidays_dir = data_dir / ticker / "holidays"
    
    # Get all year files
    year_files = sorted([f for f in holidays_dir.glob("*_options_pessimistic.csv")])
    
    print(f"\n{'='*80}")
    print(f"Checking {ticker}")
    print(f"{'='*80}")
    
    for year_file in year_files:
        year = year_file.stem.split("_")[0]
        
        try:
            # Read the file
            df = pd.read_csv(year_file)
            
            # Convert date_only to datetime
            df['date_only'] = pd.to_datetime(df['date_only'])
            
            # Get unique entry dates
            unique_dates = df['date_only'].dt.date.unique()
            
            # Calculate week numbers (ISO week)
            weeks = set()
            for date in unique_dates:
                iso_year, iso_week, _ = date.isocalendar()
                # Count weeks that belong to the file's year OR weeks that start in the file's year
                # (week 1 of a year might start in late December of previous year)
                if iso_year == int(year):
                    weeks.add(iso_week)
                # Also check if this date is in the file's year but week belongs to next year
                # (this handles week 1 of next year that starts in current year)
                elif date.year == int(year) and iso_year == int(year) + 1 and iso_week == 1:
                    # Week 1 of next year that starts in current year - count it for current year
                    pass  # Don't count this as it belongs to next year
            
            # Also check if we need to look at previous year's week 1
            # Week 1 of current year might start in previous year's December
            if int(year) > 2015:  # Don't check for years before 2016
                prev_year_file = holidays_dir / f"{int(year)-1}_options_pessimistic.csv"
                if prev_year_file.exists():
                    try:
                        prev_df = pd.read_csv(prev_year_file)
                        prev_df['date_only'] = pd.to_datetime(prev_df['date_only'])
                        prev_unique_dates = prev_df['date_only'].dt.date.unique()
                        for date in prev_unique_dates:
                            iso_year, iso_week, _ = date.isocalendar()
                            # If date is in previous year but week belongs to current year
                            if date.year == int(year) - 1 and iso_year == int(year) and iso_week == 1:
                                weeks.add(1)
                                break
                    except:
                        pass
            
            num_weeks = len(weeks)
            missing_weeks = set(range(1, 53)) - weeks
            
            results[ticker][year] = {
                'num_weeks': num_weeks,
                'missing_weeks': missing_weeks,
                'total_entries': len(df),
                'unique_dates': len(unique_dates)
            }
            
            status = "✅" if num_weeks >= 52 else "❌"
            print(f"{status} {year}: {num_weeks} weeks covered", end="")
            
            if num_weeks < 52:
                print(f" (missing weeks: {sorted(missing_weeks)})")
            else:
                print()
                
        except Exception as e:
            print(f"❌ {year}: Error - {e}")
            results[ticker][year] = {'error': str(e)}

# Summary
print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}\n")

tickers_with_issues = []
for ticker in sorted(results.keys()):
    for year, data in sorted(results[ticker].items()):
        if 'error' in data:
            print(f"❌ {ticker} {year}: ERROR - {data['error']}")
            tickers_with_issues.append((ticker, year))
        elif data['num_weeks'] < 52:
            print(f"❌ {ticker} {year}: Only {data['num_weeks']} weeks (missing: {sorted(data['missing_weeks'])})")
            tickers_with_issues.append((ticker, year))

if not tickers_with_issues:
    print("✅ All tickers have all 52 weeks covered!")
else:
    print(f"\n⚠️  Found {len(tickers_with_issues)} ticker-year combinations with missing weeks")

