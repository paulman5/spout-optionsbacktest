#!/usr/bin/env python3
"""
Verify that all put options have been removed from monthly and holidays files.
"""

import pandas as pd
from pathlib import Path

base_dir = Path("python-boilerplate/data")
directories = ['monthly', 'holidays']

print("=" * 80)
print("VERIFYING NO PUT OPTIONS IN MONTHLY AND HOLIDAYS FILES")
print("=" * 80)
print()

total_files = 0
files_with_puts = []
total_puts = 0

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    
    for subdir in directories:
        data_dir = ticker_dir / subdir
        if not data_dir.exists():
            continue
        
        for file in sorted(data_dir.glob('*_options_pessimistic.csv')):
            try:
                df = pd.read_csv(file)
                total_files += 1
                
                if 'option_type' in df.columns:
                    puts_count = (df['option_type'] == 'P').sum()
                    if puts_count > 0:
                        files_with_puts.append((ticker, subdir, file.name, puts_count))
                        total_puts += puts_count
            except Exception as e:
                print(f"Error reading {ticker}/{subdir}/{file.name}: {e}")

print(f"Total files checked: {total_files}")
print()

if files_with_puts:
    print(f"⚠️  Found {len(files_with_puts)} files with put options:")
    print()
    for ticker, subdir, filename, puts_count in files_with_puts:
        print(f"  {ticker}/{subdir}/{filename}: {puts_count} puts")
    print()
    print(f"Total puts found: {total_puts}")
else:
    print("✅ All files verified - no put options found!")
    print(f"All {total_files} files contain only call options.")

print("=" * 80)

