#!/usr/bin/env python3
"""
Debug the strike calculation issue.
"""

import pandas as pd

def parse_strike_from_ticker(ticker):
    """Extract strike from ticker and divide by 1000."""
    if not ticker.startswith('O:'):
        return None
    
    ticker = ticker[2:]  # Remove 'O:'
    
    # Find first digit to separate symbol from numbers
    symbol_end = 0
    for i, char in enumerate(ticker):
        if char.isdigit():
            symbol_end = i
            break
    
    if symbol_end == 0:
        return None
    
    remaining = ticker[symbol_end:]
    if len(remaining) < 15:  # Need at least 6 (date) + 1 (type) + 8 (strike)
        return None
    
    try:
        # Skip 6-digit date and 1-char type, get strike
        strike_str = remaining[7:]
        return float(strike_str) / 1000.0
    except (ValueError, IndexError):
        return None

# Check the current CSV
csv_path = "python-boilerplate/data/TSLA/monthly/2018_options_pessimistic.csv"
df = pd.read_csv(csv_path)

# Find the problematic ticker
target_ticker = "O:TSLA180216C00250000"
row = df[df['ticker'] == target_ticker].iloc[0]

print("Current CSV data:")
print(f"Ticker: {row['ticker']}")
print(f"Current strike: {row['strike']}")
print(f"Date: {row['date_only']}")

# Calculate what it should be
correct_strike = parse_strike_from_ticker(target_ticker)
print(f"\nCorrect calculation:")
print(f"Raw strike from ticker: {correct_strike}")

# Apply split adjustment
from datetime import datetime
date_obj = datetime.strptime(row['date_only'], '%Y-%m-%d')
if date_obj < datetime(2020, 8, 31):
    split_multiplier = 15.0
elif date_obj < datetime(2022, 8, 25):
    split_multiplier = 3.0
else:
    split_multiplier = 1.0

adjusted_strike = round(correct_strike / split_multiplier, 2)
print(f"Split multiplier: {split_multiplier}")
print(f"Adjusted strike: {adjusted_strike}")

print(f"\nDiscrepancy:")
print(f"Expected: {adjusted_strike}")
print(f"Actual: {row['strike']}")
print(f"Difference: {abs(adjusted_strike - row['strike'])}")
