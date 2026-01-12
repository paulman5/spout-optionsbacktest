#!/usr/bin/env python3
"""
Fix OTM percentage for CRWD monthly 2022 file.
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

file_path = Path("python-boilerplate/data/CRWD/monthly/2022_options_pessimistic.csv")

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR CRWD MONTHLY 2022")
print("=" * 80)
print(f"File: {file_path}")
print()

# Load the file
df = pd.read_csv(file_path)
print(f"Loaded {len(df):,} rows")

# Check current state
print(f"\nCurrent OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")

# Calculate correct OTM percentage
# Formula: ((strike - underlying_spot) / underlying_spot) * 100
df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100

# Round to 2 decimal places
df['otm_pct'] = df['otm_pct'].round(2)

print(f"New OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")

# Show some statistics
rows_with_spot = df['underlying_spot'].notna().sum()
if rows_with_spot > 0:
    itm_count = (df['otm_pct'] < 0).sum()
    otm_count = (df['otm_pct'] >= 0).sum()
    print(f"ITM (OTM% < 0): {itm_count:,}")
    print(f"OTM (OTM% >= 0): {otm_count:,}")

# Show sample of corrected values
print(f"\nSample of corrected values:")
sample = df[['strike', 'underlying_spot', 'otm_pct']].head(10)
print(sample.to_string(index=False))

# Save the file
print(f"\nSaving updated file...")
df.to_csv(file_path, index=False)
print(f"✅ Fixed and saved {file_path}")

print(f"\n{'='*80}")

#!/usr/bin/env python3
"""
Fix OTM percentage for CRWD monthly 2022 file.
Formula: ((strike - underlying_spot) / underlying_spot) * 100
"""

import pandas as pd
from pathlib import Path

file_path = Path("python-boilerplate/data/CRWD/monthly/2022_options_pessimistic.csv")

print("=" * 80)
print("FIXING OTM PERCENTAGE FOR CRWD MONTHLY 2022")
print("=" * 80)
print(f"File: {file_path}")
print()

# Load the file
df = pd.read_csv(file_path)
print(f"Loaded {len(df):,} rows")

# Check current state
print(f"\nCurrent OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")

# Calculate correct OTM percentage
# Formula: ((strike - underlying_spot) / underlying_spot) * 100
df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100

# Round to 2 decimal places
df['otm_pct'] = df['otm_pct'].round(2)

print(f"New OTM range: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")

# Show some statistics
rows_with_spot = df['underlying_spot'].notna().sum()
if rows_with_spot > 0:
    itm_count = (df['otm_pct'] < 0).sum()
    otm_count = (df['otm_pct'] >= 0).sum()
    print(f"ITM (OTM% < 0): {itm_count:,}")
    print(f"OTM (OTM% >= 0): {otm_count:,}")

# Show sample of corrected values
print(f"\nSample of corrected values:")
sample = df[['strike', 'underlying_spot', 'otm_pct']].head(10)
print(sample.to_string(index=False))

# Save the file
print(f"\nSaving updated file...")
df.to_csv(file_path, index=False)
print(f"✅ Fixed and saved {file_path}")

print(f"\n{'='*80}")


