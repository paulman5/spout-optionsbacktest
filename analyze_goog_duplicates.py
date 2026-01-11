#!/usr/bin/env python3
import pandas as pd
import numpy as np

file_path = 'python-boilerplate/data/GOOG/holidays/2021_options_pessimistic.csv'
df = pd.read_csv(file_path)

# Create key for matching
df['key'] = df['date_only'].astype(str) + '_' + df['expiration_date'].astype(str) + '_' + df['strike'].astype(str)

goog_rows = df[df['underlying_symbol'] == 'GOOG'].copy()
googl_rows = df[df['underlying_symbol'] == 'GOOGL'].copy()

goog_keys = set(goog_rows['key'])
googl_keys = set(googl_rows['key'])
matching_keys = goog_keys & googl_keys

print('Analysis of GOOG vs GOOGL duplicates:')
print('='*80)
print(f'Total matching pairs: {len(matching_keys):,}')

# Compare premium differences
premium_diffs = []
volume_diffs = []

for key in list(matching_keys)[:1000]:  # Sample first 1000
    goog_match = goog_rows[goog_rows['key'] == key]
    googl_match = googl_rows[googl_rows['key'] == key]
    
    if len(goog_match) > 0 and len(googl_match) > 0:
        goog_row = goog_match.iloc[0]
        googl_row = googl_match.iloc[0]
        
        # Compare mid prices
        goog_mid = (goog_row['high_price'] + goog_row['low_price']) / 2
        googl_mid = (googl_row['high_price'] + googl_row['low_price']) / 2
        
        premium_diffs.append(abs(goog_mid - googl_mid))
        
        # Compare volumes
        volume_diffs.append(goog_row['volume'] - googl_row['volume'])

print(f'\nPremium differences (mid price):')
print(f'  Mean absolute difference: ${np.mean(premium_diffs):.2f}')
print(f'  Median absolute difference: ${np.median(premium_diffs):.2f}')
print(f'  Max difference: ${np.max(premium_diffs):.2f}')

print(f'\nVolume differences (GOOG - GOOGL):')
print(f'  Mean: {np.mean(volume_diffs):.1f}')
print(f'  Median: {np.median(volume_diffs):.1f}')
goog_more = sum(1 for v in volume_diffs if v > 0)
googl_more = sum(1 for v in volume_diffs if v < 0)
print(f'  GOOG has more volume: {goog_more} / {len(volume_diffs)}')
print(f'  GOOGL has more volume: {googl_more} / {len(volume_diffs)}')

# Check which has more total volume across all rows
total_goog_volume = df[df['underlying_symbol'] == 'GOOG']['volume'].sum()
total_googl_volume = df[df['underlying_symbol'] == 'GOOGL']['volume'].sum()

print(f'\nTotal volume across all rows:')
print(f'  GOOG: {total_goog_volume:,}')
print(f'  GOOGL: {total_googl_volume:,}')
if total_goog_volume > total_googl_volume:
    print(f'  More liquid: GOOG')
else:
    print(f'  More liquid: GOOGL')

