#!/usr/bin/env python3
"""
Create a summary table of monthly APYs and average hit rates for all tickers.
"""

import re
from pathlib import Path

input_file = Path("itm_liquidation_and_apy_per_year.txt")

print("=" * 120)
print("CREATING MONTHLY APY SUMMARY TABLE")
print("=" * 120)
print()

# Read the file
with open(input_file, 'r') as f:
    content = f.read()

# Find all monthly sections - look for "Monthly Options" followed by TOTAL line
# Pattern: find ticker, then find monthly section, then extract TOTAL line
ticker_sections = re.split(r'(?=TICKER:)', content)

monthly_data = []

for section in ticker_sections:
    if not section.strip() or 'TICKER:' not in section:
        continue
    
    # Extract ticker name
    ticker_match = re.search(r'TICKER:\s+([A-Z0-9]+(?:\s+\[FLAGGED AS LOW\])?)', section)
    if not ticker_match:
        continue
    
    ticker = ticker_match.group(1).strip()
    ticker_clean = re.sub(r'\s+\[.*?\]', '', ticker)
    
    # Find all monthly sections in this ticker's section
    monthly_matches = re.finditer(
        r'Monthly Options \(([0-9]+-[0-9]+)% Probability ITM\):.*?TOTAL\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9.]+)%\s+([0-9.]+)%',
        section,
        re.DOTALL
    )
    
    for match in monthly_matches:
        prob_range = match.group(1)
        total_options = match.group(2).replace(',', '')
        unique_months = match.group(3).replace(',', '')
        itm_months = match.group(4).replace(',', '')
        itm_rate = float(match.group(5))
        avg_apy = float(match.group(6))
        
        monthly_data.append({
            'ticker': ticker_clean,
            'prob_range': prob_range,
            'itm_rate': itm_rate,
            'avg_apy': avg_apy,
            'total_options': int(total_options),
            'unique_months': int(unique_months),
            'itm_months': int(itm_months)
        })

# Sort by ticker name
monthly_data.sort(key=lambda x: x['ticker'])

# Group by ticker and calculate average hit rate (if multiple ranges per ticker)
ticker_hit_rates = {}
for data in monthly_data:
    ticker = data['ticker']
    if ticker not in ticker_hit_rates:
        ticker_hit_rates[ticker] = []
    ticker_hit_rates[ticker].append(data['itm_rate'])

# Calculate average hit rate per ticker
ticker_avg_hit_rates = []
for ticker, hit_rates in ticker_hit_rates.items():
    avg_hit_rate = sum(hit_rates) / len(hit_rates)
    ticker_avg_hit_rates.append({
        'ticker': ticker,
        'avg_hit_rate': avg_hit_rate
    })

# Sort by ticker name
ticker_avg_hit_rates.sort(key=lambda x: x['ticker'])

# Create summary table
output_lines = []
output_lines.append("=" * 120)
output_lines.append("MONTHLY OPTIONS SUMMARY: AVERAGE ITM HIT RATE BY TICKER")
output_lines.append("=" * 120)
output_lines.append("")
output_lines.append(f"{'Ticker':<15} {'Avg Hit Rate':<15}")
output_lines.append(f"{'-'*15} {'-'*15}")

for data in ticker_avg_hit_rates:
    output_lines.append(
        f"{data['ticker']:<15} "
        f"{data['avg_hit_rate']:>14.2f}%"
    )

# Calculate overall average hit rate
if ticker_avg_hit_rates:
    overall_avg_hit_rate = sum(d['avg_hit_rate'] for d in ticker_avg_hit_rates) / len(ticker_avg_hit_rates)
    
    output_lines.append(f"{'-'*15} {'-'*15}")
    output_lines.append(
        f"{'AVERAGE':<15} "
        f"{overall_avg_hit_rate:>14.2f}%"
    )

output_lines.append("")
output_lines.append("=" * 120)

# Print to console
for line in output_lines:
    print(line)

# Remove old summary table if it exists and replace with new one
with open(input_file, 'r') as f:
    content = f.read()

# Remove old summary table
old_pattern = r"========================================================================================================================\nMONTHLY OPTIONS SUMMARY:.*?========================================================================================================================\n"
content = re.sub(old_pattern, "", content, flags=re.DOTALL)

# Append new summary table
with open(input_file, 'w') as f:
    f.write(content)
    f.write("\n\n")
    f.write("\n".join(output_lines))
    f.write("\n")

print()
print(f"✅ Monthly APY summary table appended to: {input_file}")
