#!/usr/bin/env python3
"""
Calculate ITM liquidation rate for options that match our APY calculation filters:
- High monthly IV → Weekly with 4-7% probability ITM
- Low monthly IV → Monthly with 10-15% probability ITM
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

print("=" * 100)
print("CALCULATING ITM LIQUIDATION RATE FOR FILTERED OPTIONS")
print("=" * 100)
print("Options matching APY calculation filters:")
print("  - High monthly IV → Weekly with 4-7% probability ITM")
print("  - Low monthly IV → Monthly with 10-15% probability ITM")
print()

# Calculate IV threshold
all_ivs = []
for ticker_dir in base_dir.iterdir():
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        for file in monthly_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    all_ivs.extend(ivs.tolist())
            except:
                continue

iv_threshold = np.median(all_ivs) if all_ivs else 0.30
print(f"IV Threshold: {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
print()

# Determine monthly IV level for each ticker
ticker_monthly_iv = {}
for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_ivs = []
    monthly_dir = ticker_dir / 'monthly'
    if monthly_dir.exists():
        for file in monthly_dir.glob('*_options_pessimistic.csv'):
            try:
                df = pd.read_csv(file)
                if 'implied_volatility' in df.columns:
                    ivs = df['implied_volatility'].dropna()
                    monthly_ivs.extend(ivs.tolist())
            except:
                continue
    
    if monthly_ivs:
        avg_monthly_iv = np.mean(monthly_ivs)
        ticker_monthly_iv[ticker] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
    else:
        ticker_monthly_iv[ticker] = 'low'

# Calculate ITM rates for filtered options
ticker_stats = defaultdict(lambda: {
    'weekly_total': 0,
    'weekly_itm': 0,
    'monthly_total': 0,
    'monthly_itm': 0
})

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    
    # Process weekly if high monthly IV
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Filter by 4-7% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.04) & 
                                  (df['probability_itm'] <= 0.07) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    total = len(filtered)
                    itm_count = (filtered['ITM'] == 'YES').sum()
                    
                    ticker_stats[ticker]['weekly_total'] += total
                    ticker_stats[ticker]['weekly_itm'] += itm_count
                except:
                    continue
    
    # Process monthly if low monthly IV
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Filter by 10-15% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.10) & 
                                  (df['probability_itm'] <= 0.15) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    total = len(filtered)
                    itm_count = (filtered['ITM'] == 'YES').sum()
                    
                    ticker_stats[ticker]['monthly_total'] += total
                    ticker_stats[ticker]['monthly_itm'] += itm_count
                except:
                    continue

# Calculate percentages
print("=" * 100)
print("ITM LIQUIDATION RATE FOR FILTERED OPTIONS BY TICKER")
print("=" * 100)
print(f"{'Ticker':<8} {'Type':<8} {'Total':<12} {'ITM':<12} {'ITM Rate':<12}")
print("-" * 100)

ticker_weekly_rates = {}
ticker_monthly_rates = {}

for ticker in sorted(ticker_stats.keys()):
    stats = ticker_stats[ticker]
    
    # Weekly stats
    if stats['weekly_total'] > 0:
        weekly_rate = (stats['weekly_itm'] / stats['weekly_total']) * 100
        ticker_weekly_rates[ticker] = weekly_rate
        print(f"{ticker:<8} {'Weekly':<8} {stats['weekly_total']:>11,} {stats['weekly_itm']:>11,} {weekly_rate:>11.2f}%")
    
    # Monthly stats
    if stats['monthly_total'] > 0:
        monthly_rate = (stats['monthly_itm'] / stats['monthly_total']) * 100
        ticker_monthly_rates[ticker] = monthly_rate
        print(f"{ticker:<8} {'Monthly':<8} {stats['monthly_total']:>11,} {stats['monthly_itm']:>11,} {monthly_rate:>11.2f}%")

print("-" * 100)

# Overall statistics
total_weekly = sum(s['weekly_total'] for s in ticker_stats.values())
itm_weekly = sum(s['weekly_itm'] for s in ticker_stats.values())
total_monthly = sum(s['monthly_total'] for s in ticker_stats.values())
itm_monthly = sum(s['monthly_itm'] for s in ticker_stats.values())

if total_weekly > 0:
    overall_weekly_rate = (itm_weekly / total_weekly) * 100
    print(f"{'TOTAL':<8} {'Weekly':<8} {total_weekly:>11,} {itm_weekly:>11,} {overall_weekly_rate:>11.2f}%")

if total_monthly > 0:
    overall_monthly_rate = (itm_monthly / total_monthly) * 100
    print(f"{'TOTAL':<8} {'Monthly':<8} {total_monthly:>11,} {itm_monthly:>11,} {overall_monthly_rate:>11.2f}%")

# Print as lists
print("\n" + "=" * 100)
print("ITM LIQUIDATION RATES AS LISTS")
print("=" * 100)

print("\nWeekly ITM Rate per Ticker (%) - High Monthly IV tickers with 4-7% prob ITM:")
weekly_tickers = sorted(ticker_weekly_rates.keys())
print([round(ticker_weekly_rates[t], 2) for t in weekly_tickers])

print("\nMonthly ITM Rate per Ticker (%) - Low Monthly IV tickers with 10-15% prob ITM:")
monthly_tickers = sorted(ticker_monthly_rates.keys())
print([round(ticker_monthly_rates[t], 2) for t in monthly_tickers])

print("\nWeekly Ticker Names (alphabetical):")
print(weekly_tickers)

print("\nMonthly Ticker Names (alphabetical):")
print(monthly_tickers)

print("\n" + "=" * 100)


