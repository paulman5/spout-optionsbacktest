#!/usr/bin/env python3
"""
Calculate TSLA monthly options with 4-7% probability ITM for 2016-2025:
- Count unique months where ITM == 'YES' (1 per month)
- Calculate average APY per year
- ITM Rate: (ITM months / 12) * 100%
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")
ticker = "TSLA"
monthly_dir = base_dir / ticker / 'monthly'

print("=" * 120)
print(f"CALCULATING TSLA MONTHLY OPTIONS (4-7% PROBABILITY ITM) FOR 2016-2025")
print("=" * 120)
print()

if not monthly_dir.exists():
    print(f"❌ Monthly directory not found: {monthly_dir}")
    exit(1)

# Store per-year data
yearly_data = defaultdict(lambda: {
    'total_options': 0,
    'unique_expirations': set(),
    'itm_expirations': set(),
    'apy_values': []
})

# Process all files for 2016-2025
print("Processing files with 4-7% probability ITM range...")
for year in range(2016, 2026):
    file = monthly_dir / f"{year}_options_pessimistic.csv"
    if not file.exists():
        print(f"  Warning: {file.name} not found")
        continue
    
    try:
        df = pd.read_csv(file)
        
        if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
            print(f"  Warning: Missing columns in {file.name}")
            continue
        
        # Filter by 4-7% probability ITM
        filtered = df[(df['probability_itm'] >= 0.04) & 
                      (df['probability_itm'] <= 0.07) & 
                      (df['probability_itm'].notna())]
        
        if len(filtered) == 0:
            print(f"  {year}: No options in 4-7% range")
            continue
        
        print(f"  {year}: Found {len(filtered)} options in 4-7% range")
        
        # Track unique expirations - for monthly: extract unique months (YYYY-MM)
        for exp_date in filtered['expiration_date'].unique():
            if pd.notna(exp_date):
                exp_dt = pd.to_datetime(exp_date)
                month_key = f"{exp_dt.year}-{exp_dt.month:02d}"  # YYYY-MM format
                yearly_data[year]['unique_expirations'].add(month_key)
        
        # Track ITM expirations - for monthly: extract unique months where ITM == 'YES'
        itm_rows = filtered[filtered['ITM'] == 'YES']
        for exp_date in itm_rows['expiration_date'].unique():
            if pd.notna(exp_date):
                exp_dt = pd.to_datetime(exp_date)
                month_key = f"{exp_dt.year}-{exp_dt.month:02d}"  # YYYY-MM format
                yearly_data[year]['itm_expirations'].add(month_key)
        
        # Collect APY values (premium_yield_pct)
        if 'premium_yield_pct' in filtered.columns:
            apy_vals = filtered['premium_yield_pct'].dropna()
            if len(apy_vals) > 0:
                # Check if values are already in percentage form (multiplied by 100)
                # If median is > 10, assume values are already percentages and divide by 100
                if apy_vals.median() > 10:
                    apy_vals = apy_vals / 100.0
                yearly_data[year]['apy_values'].extend(apy_vals.tolist())
        
        yearly_data[year]['total_options'] = len(filtered)
        
    except Exception as e:
        print(f"  Error processing {file.name}: {e}")
        continue

print()

# Convert sets to counts and calculate averages
processed_yearly = {}
for year in sorted(yearly_data.keys()):
    data = yearly_data[year]
    unique_exp_count = len(data['unique_expirations'])
    itm_exp_count = len(data['itm_expirations'])
    
    # Calculate average APY
    avg_apy = np.mean(data['apy_values']) if data['apy_values'] else 0
    
    # For monthly: ITM rate = (unique months with ITM / 12) * 100%
    itm_rate_per_year = (itm_exp_count / 12 * 100) if unique_exp_count > 0 else 0
    
    processed_yearly[year] = {
        'total_options': data['total_options'],
        'unique_expirations': unique_exp_count,
        'itm_expirations': itm_exp_count,
        'itm_rate_per_year': itm_rate_per_year,
        'avg_apy': avg_apy
    }

# Calculate average IV from monthly files
print("Calculating average IV...")
all_ivs = []
for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
    try:
        df = pd.read_csv(file)
        if 'implied_volatility' in df.columns:
            ivs = df['implied_volatility'].dropna()
            all_ivs.extend(ivs.tolist())
    except:
        continue

avg_iv = np.mean(all_ivs) if all_ivs else 0
print(f"Average IV: {avg_iv*100:.2f}%")
print()

# Create output
output_lines = []
output_lines.append("  TSLA Monthly Options (4-7% Probability ITM):")
output_lines.append(f"  {'Year':<8} {'Total Options':<15} {'Unique Months':<15} {'ITM Months':<12} {'ITM Rate/Year':<15} {'Avg APY':<12}")
output_lines.append(f"  {'-'*8} {'-'*15} {'-'*15} {'-'*12} {'-'*15} {'-'*12}")

total_all_years = 0
itm_all_years = 0
unique_exp_all = 0
apy_values_all = []

for year in sorted(processed_yearly.keys()):
    year_data = processed_yearly[year]
    total = year_data['total_options']
    unique_exp = year_data['unique_expirations']
    itm_exp = year_data['itm_expirations']
    itm_rate = year_data['itm_rate_per_year']
    avg_apy = year_data['avg_apy']
    
    total_all_years += total
    itm_all_years += itm_exp
    unique_exp_all += unique_exp
    if avg_apy > 0:
        apy_values_all.append(avg_apy)
    
    output_lines.append(f"  {year:<8} {total:>14,} {unique_exp:>14,} {itm_exp:>11,} {itm_rate:>14.2f}% {avg_apy:>11.4f}%")

# Calculate overall ITM rate
num_years = len(processed_yearly)
overall_itm_rate = (itm_all_years / (num_years * 12) * 100) if num_years > 0 else 0

# Calculate average APY - simple average of yearly averages (same as ORCL)
overall_apy = np.mean(apy_values_all) if apy_values_all else 0

output_lines.append(f"  {'-'*8} {'-'*15} {'-'*15} {'-'*12} {'-'*15} {'-'*12}")
output_lines.append(f"  {'TOTAL':<8} {total_all_years:>14,} {unique_exp_all:>14,} {itm_all_years:>11,} {overall_itm_rate:>14.2f}% {overall_apy:>11.4f}%")

# Print to console
for line in output_lines:
    print(line)

# Insert or replace in the main output file right after TSLA weekly section
output_file = Path("itm_liquidation_and_apy_per_year.txt")
with open(output_file, 'r') as f:
    content = f.read()

# Find the insertion point (after TSLA weekly TOTAL line)
insertion_marker = "  TOTAL             1,586         517        50           9.67%      0.3041%\n"

# Check if TSLA monthly section already exists and replace it
if "  TSLA Monthly Options (4-7% Probability ITM):" in content:
    # Find and replace the existing section
    import re
    pattern = r"  TSLA Monthly Options \(4-7% Probability ITM\):.*?TOTAL.*?\n"
    replacement = "\n".join(output_lines) + "\n"
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    print("Replaced existing TSLA monthly section")
elif insertion_marker in content:
    # Insert the monthly data right after the weekly section
    insertion_text = "\n" + "\n".join(output_lines) + "\n"
    content = content.replace(insertion_marker, insertion_marker + insertion_text)
    print("Inserted new TSLA monthly section")
else:
    print(f"❌ Could not find insertion point in {output_file}")
    exit(1)

with open(output_file, 'w') as f:
    f.write(content)
print()
print(f"✅ TSLA monthly (4-7% prob ITM) data calculated and updated in: {output_file}")
