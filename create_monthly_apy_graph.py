#!/usr/bin/env python3
"""
Create a graph showing average monthly APY by year (2016-2025).
Y-axis: Average APY (0-1% and 1-2% ranges)
X-axis: Years (2016-2025)
"""

import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path
from collections import defaultdict

input_file = Path("itm_liquidation_and_apy_per_year.txt")

print("=" * 120)
print("CREATING MONTHLY APY GRAPH BY YEAR (2016-2025)")
print("=" * 120)
print()

# Read the file
with open(input_file, 'r') as f:
    content = f.read()

# Extract yearly APY and hit rate data from monthly sections
# Pattern: Monthly Options section with yearly breakdown
yearly_apy_data = defaultdict(list)  # year -> list of APY values
yearly_hit_rate_data = defaultdict(list)  # year -> list of hit rate values

# Find all monthly sections and extract yearly APY and hit rate values
monthly_sections = re.finditer(
    r'Monthly Options \([0-9]+-[0-9]+% Probability ITM\):.*?(?=TOTAL|Monthly Options|TICKER:|$)',
    content,
    re.DOTALL
)

for section_match in monthly_sections:
    section = section_match.group(0)
    
    # Extract yearly data lines (format: "  2016                 38             12           0           0.00%      0.1624%")
    # Format: Year, Total Options, Unique Months, ITM Months, ITM Rate/Year, Avg APY
    year_lines = re.finditer(
        r'^\s+(\d{4})\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+([\d.]+)%',
        section,
        re.MULTILINE
    )
    
    for line_match in year_lines:
        year = int(line_match.group(1))
        hit_rate = float(line_match.group(5))  # ITM Rate/Year
        apy = float(line_match.group(6))  # Avg APY
        
        # Only include years 2016-2025
        if 2016 <= year <= 2025:
            yearly_apy_data[year].append(apy)
            yearly_hit_rate_data[year].append(hit_rate)

# Calculate average APY, hit rate, and leverage per year
years = sorted(yearly_apy_data.keys())
avg_apy_by_year = []
avg_hit_rate_by_year = []
leverage_by_year = []

for year in years:
    apy_values = yearly_apy_data[year]
    hit_rate_values = yearly_hit_rate_data[year]
    avg_apy_monthly = np.mean(apy_values) if apy_values else 0
    avg_apy_yearly = avg_apy_monthly * 12  # Convert monthly to yearly
    avg_hit_rate = np.mean(hit_rate_values) if hit_rate_values else 0
    leverage = (avg_apy_yearly / 70.0) * 100  # Leverage = (Yearly APY / 70) * 100
    avg_apy_by_year.append(avg_apy_yearly)
    avg_hit_rate_by_year.append(avg_hit_rate)
    leverage_by_year.append(leverage)
    print(f"{year}: {len(apy_values)} data points, Avg Yearly APY = {avg_apy_yearly:.4f}%, Avg Hit Rate = {avg_hit_rate:.2f}%, Leverage = {leverage:.4f}%")

print()

# Create the graph
fig, ax = plt.subplots(figsize=(12, 8))

# Plot the data - APY line
ax.plot(years, avg_apy_by_year, marker='o', linewidth=2, markersize=8, color='#2E86AB', label='Average Yearly APY')

# Plot the leverage line (APY / 70)
ax.plot(years, leverage_by_year, marker='s', linewidth=2, markersize=6, color='#A23B72', linestyle='--', label='Leverage (APY / 70)')

# Set Y-axis range and add grid lines
ax.set_ylim(0, 20)  # 0% to 20%
ax.set_yticks([0, 4, 8, 12, 16, 20])  # 0%, 4%, 8%, 12%, 16%, 20%
ax.set_yticklabels(['0%', '4%', '8%', '12%', '16%', '20%'])
ax.grid(True, alpha=0.3, linestyle='--')

# Set X-axis
ax.set_xlim(2015.5, 2025.5)
ax.set_xticks(years)
ax.set_xticklabels(years, rotation=45, ha='right')

# Labels and title
ax.set_xlabel('Year', fontsize=12, fontweight='bold')
ax.set_ylabel('Average Yearly APY (%)', fontsize=12, fontweight='bold')
ax.set_title('Average Yearly APY by Year (2016-2025)', fontsize=14, fontweight='bold', pad=20)

# Add legend with custom ITM rate entry
itm_patch = mpatches.Patch(facecolor='#90EE90', edgecolor='#2d5016', linewidth=2.5, alpha=0.85, label='ITM rate')
ax.legend(handles=[ax.get_lines()[0], ax.get_lines()[1], itm_patch], loc='upper left', fontsize=10)

# Add value labels on points (APY) and hit rate just above X-axis
for year, apy, hit_rate, leverage in zip(years, avg_apy_by_year, avg_hit_rate_by_year, leverage_by_year):
    # Hit rate label positioned horizontally just above each year on the X-axis
    ax.annotate(f'{hit_rate:.1f}%', (year, 0), textcoords="offset points",
                xytext=(0, 25), ha='center', fontsize=11, color='#1a5f1a', fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.8', facecolor='#90EE90', alpha=0.85, 
                         edgecolor='#2d5016', linewidth=2.5, linestyle='-'))
    
    # APY label on the point
    # Move 2019 (5.620%) a bit more up
    # Move 2021 (6.701%) a bit more to the right
    if year == 2019:
        apy_x_offset = -10
        apy_y_offset = 15  # Move up
    elif year == 2021:
        apy_x_offset = 15  # Move more to the right
        apy_y_offset = 10
    else:
        apy_x_offset = 0
        apy_y_offset = 10
    ax.annotate(f'{apy:.3f}%', (year, apy), textcoords="offset points", 
                xytext=(apy_x_offset, apy_y_offset), ha='center', fontsize=9, color='#2E86AB', fontweight='bold')
    
    # Leverage label on the leverage point, positioned slightly above the point
    # Move 2021 (9.5731%) a little more to the right
    # Move 2019 (8.0292%) more up and left
    # Move 2016 (6.3838%) a bit up
    if year == 2021:
        leverage_x_offset = 18  # Move more to the right
        leverage_y_offset = 15
    elif year == 2019:
        leverage_x_offset = -15  # Move more to the left
        leverage_y_offset = 30  # Move more up
    elif year == 2016:
        leverage_x_offset = 0
        leverage_y_offset = 20  # Move up
    else:
        leverage_x_offset = 0
        leverage_y_offset = 15
    ax.annotate(f'{leverage:.4f}%', (year, leverage), textcoords="offset points", 
                xytext=(leverage_x_offset, leverage_y_offset), ha='center', fontsize=9, color='#A23B72', fontweight='bold')

plt.tight_layout()

# Save the graph
output_file = Path("monthly_apy_by_year_2016_2025.png")
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"✅ Graph saved to: {output_file}")

# Also save as PDF
output_file_pdf = Path("monthly_apy_by_year_2016_2025.pdf")
plt.savefig(output_file_pdf, bbox_inches='tight')
print(f"✅ Graph saved to: {output_file_pdf}")

# Show summary statistics
print()
print("Summary Statistics:")
print(f"  Overall Average APY: {np.mean(avg_apy_by_year):.4f}%")
print(f"  Min APY: {min(avg_apy_by_year):.4f}% ({years[avg_apy_by_year.index(min(avg_apy_by_year))]})")
print(f"  Max APY: {max(avg_apy_by_year):.4f}% ({years[avg_apy_by_year.index(max(avg_apy_by_year))]})")
print(f"  Years in 0-1% range: {sum(1 for apy in avg_apy_by_year if apy < 1.0)}")
print(f"  Years in 1-2% range: {sum(1 for apy in avg_apy_by_year if 1.0 <= apy < 2.0)}")

# plt.show()  # Commented out to avoid blocking
print("Graph generation complete. Check the saved files.")
