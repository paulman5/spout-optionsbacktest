#!/usr/bin/env python3
"""
Calculate ITM liquidation counts per year (1 per expiration) and APY averages:
- Count unique expiration dates where ITM == 'YES' (1 per month/week)
- Calculate average APY per year (weekly: sum/weeks, monthly: sum/12)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

base_dir = Path("python-boilerplate/data")

# Market cap data (in billions USD) - from companiesmarketcap.com as of January 2025
# Source: https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/
MARKET_CAPS = {
    'AAPL': 3832,   # Apple - $3.832 T (rank #3)
    'ADBE': 250,    # Adobe - approximately $250B (verify on site)
    'AMAT': 150,    # Applied Materials - approximately $150B (verify on site)
    'AMD': 300,     # AMD - approximately $300B (verify on site)
    'AMZN': 2644,   # Amazon - $2.644 T (rank #5)
    'AVGO': 1635,   # Broadcom - $1.635 T (rank #7)
    'COST': 350,    # Costco - approximately $350B (verify on site)
    'CRM': 300,     # Salesforce - approximately $300B (verify on site)
    'CRWD': 119,    # CrowdStrike - $118.64 B (rank #98)
    'CSCO': 200,    # Cisco - approximately $200B (verify on site)
    'GOOG': 3973,   # Alphabet (Google) - $3.973 T (rank #2)
    'HOOD': 15,     # Robinhood - approximately $15B (verify on site)
    'INTC': 200,    # Intel - approximately $200B (verify on site)
    'IWM': 0,       # ETF (not applicable)
    'JPM': 500,     # JPMorgan Chase - approximately $500B (verify on site)
    'KO': 280,      # Coca-Cola - approximately $280B (verify on site)
    'LQD': 0,       # ETF (not applicable)
    'LRCX': 120,    # Lam Research - approximately $120B (verify on site)
    'META': 1646,   # Meta (Facebook) - $1.646 T (rank #6)
    'MRK': 300,     # Merck - approximately $300B (verify on site)
    'MSFT': 3562,   # Microsoft - $3.562 T (rank #4)
    'MU': 150,      # Micron - approximately $150B (verify on site)
    'NFLX': 250,    # Netflix - approximately $250B (verify on site)
    'NVDA': 4499,   # NVIDIA - $4.499 T (rank #1)
    'ORCL': 400,    # Oracle - approximately $400B (verify on site)
    'PLTR': 50,     # Palantir - approximately $50B (verify on site)
    'QCOM': 200,    # Qualcomm - approximately $200B (verify on site)
    'QQQ': 0,       # ETF (not applicable)
    'SMCI': 50,     # Super Micro Computer - approximately $50B (verify on site)
    'SOFI': 8,      # SoFi - approximately $8B (verify on site)
    'SPOT': 60,     # Spotify - approximately $60B (verify on site)
    'SPY': 0,       # ETF (not applicable)
    'TSLA': 1480,   # Tesla - $1.480 T (rank #8)
    'UBER': 150,    # Uber - approximately $150B (verify on site)
    'UNH': 500,     # UnitedHealth - approximately $500B (verify on site)
    'V': 550,       # Visa - approximately $550B (verify on site)
    'WMT': 913,     # Walmart - $913.13 B (rank #11)
    'XLE': 0,       # ETF (not applicable)
    'XLF': 0,       # ETF (not applicable)
    'XLK': 0,       # ETF (not applicable)
    'XOM': 500,     # Exxon Mobil - approximately $500B (verify on site)
}

print("=" * 120)
print("CALCULATING ITM LIQUIDATION COUNTS AND APY AVERAGES PER YEAR")
print("=" * 120)
print("Logic:")
print("  - Low Monthly IV → Monthly options with 10-13% probability ITM (AAPL: 7-11%)")
print("  - High Monthly IV → Weekly options with 4-7% probability ITM")
print("  - Count unique expiration dates with ITM == 'YES' (1 per expiration)")
print("  - Calculate average APY per year")
print()

# Step 1: Calculate IV threshold
print("Step 1: Calculating IV threshold...")
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
print(f"IV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)")
print()

# Step 2: Determine monthly IV level for each ticker
print("Step 2: Determining monthly IV level for each ticker...")
ticker_monthly_iv = {}
ticker_avg_iv = {}

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
        ticker_avg_iv[ticker] = avg_monthly_iv
        ticker_monthly_iv[ticker] = 'high' if avg_monthly_iv >= iv_threshold else 'low'
    else:
        ticker_avg_iv[ticker] = None
        ticker_monthly_iv[ticker] = 'low'

print(f"Found {len(ticker_monthly_iv)} tickers")
print()

# Step 3: Calculate ITM counts and APY per year
print("Step 3: Calculating ITM liquidation counts and APY averages per year...")
print()

ticker_results = {}

def collect_all_years(directory):
    """Collect all years present in files."""
    years = set()
    if directory.exists():
        for file in sorted(directory.glob('*_options_pessimistic.csv')):
            try:
                df = pd.read_csv(file)
                if 'date_only' in df.columns:
                    df['year'] = pd.to_datetime(df['date_only']).dt.year
                    years.update(df['year'].dropna().unique())
                else:
                    year = file.stem.split('_')[0]
                    try:
                        years.add(int(year))
                    except:
                        continue
            except:
                continue
    return years

for ticker_dir in sorted(base_dir.iterdir()):
    if not ticker_dir.is_dir() or ticker_dir.name.startswith('.'):
        continue
    
    ticker = ticker_dir.name
    monthly_iv_level = ticker_monthly_iv.get(ticker, 'low')
    avg_iv = ticker_avg_iv.get(ticker, 0)
    
    # Store per-year data
    # For monthly: itm_expirations will store unique months (YYYY-MM format)
    # For weekly: itm_expirations will store unique expiration dates
    yearly_data = defaultdict(lambda: {
        'total_options': 0,
        'unique_expirations': set(),
        'itm_expirations': set(),
        'apy_values': []
    })
    
    # Process weekly if high monthly IV
    if monthly_iv_level == 'high':
        holidays_dir = ticker_dir / 'holidays'
        if holidays_dir.exists():
            # Collect all years first
            all_years = collect_all_years(holidays_dir)
            for year in all_years:
                if year not in yearly_data:
                    yearly_data[year] = {
                        'total_options': 0,
                        'unique_expirations': set(),
                        'itm_expirations': set(),
                        'apy_values': []
                    }
            
            # First pass: try 4-7% range
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by 4-7% probability ITM
                    filtered = df[(df['probability_itm'] >= 0.04) & 
                                  (df['probability_itm'] <= 0.07) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Process per year
                    for year, year_group in filtered.groupby('year'):
                        yearly_data[year]['total_options'] += len(year_group)
                        
                        # Track unique expirations
                        for exp_date in year_group['expiration_date'].unique():
                            yearly_data[year]['unique_expirations'].add(exp_date)
                        
                        # Track ITM expirations (unique expiration dates where ITM == 'YES')
                        itm_rows = year_group[year_group['ITM'] == 'YES']
                        for exp_date in itm_rows['expiration_date'].unique():
                            yearly_data[year]['itm_expirations'].add(exp_date)
                        
                        # Collect APY values (premium_yield_pct)
                        if 'premium_yield_pct' in year_group.columns:
                            apy_vals = year_group['premium_yield_pct'].dropna()
                            yearly_data[year]['apy_values'].extend(apy_vals.tolist())
                        
                except Exception as e:
                    continue
            
            # Second pass: fill missing years with expanded range (3-8%)
            for file in sorted(holidays_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Process per year - check if year needs filling
                    for year, year_group in df.groupby('year'):
                        # Only fill if this year has no unique expirations yet
                        if len(yearly_data[year]['unique_expirations']) == 0:
                            # Use expanded range 3-8%
                            filtered = year_group[(year_group['probability_itm'] >= 0.03) & 
                                                  (year_group['probability_itm'] <= 0.08) & 
                                                  (year_group['probability_itm'].notna())]
                            
                            if len(filtered) > 0:
                                yearly_data[year]['total_options'] += len(filtered)
                                
                                # Track unique expirations
                                for exp_date in filtered['expiration_date'].unique():
                                    yearly_data[year]['unique_expirations'].add(exp_date)
                                
                                # Track ITM expirations
                                itm_rows = filtered[filtered['ITM'] == 'YES']
                                for exp_date in itm_rows['expiration_date'].unique():
                                    yearly_data[year]['itm_expirations'].add(exp_date)
                                
                                # Collect APY values
                                if 'premium_yield_pct' in filtered.columns:
                                    apy_vals = filtered['premium_yield_pct'].dropna()
                                    yearly_data[year]['apy_values'].extend(apy_vals.tolist())
                        
                except Exception as e:
                    continue
    
    # Process monthly if low monthly IV
    if monthly_iv_level == 'low':
        monthly_dir = ticker_dir / 'monthly'
        if monthly_dir.exists():
            # Collect all years first
            all_years = collect_all_years(monthly_dir)
            for year in all_years:
                if year not in yearly_data:
                    yearly_data[year] = {
                        'total_options': 0,
                        'unique_expirations': set(),
                        'itm_expirations': set(),
                        'apy_values': []
                    }
            
            # First pass: try 10-13% range (7-11% for AAPL)
            # Determine probability range based on ticker
            if ticker == 'AAPL':
                prob_min, prob_max = 0.07, 0.11  # 7-11% for AAPL (safer)
            else:
                prob_min, prob_max = 0.10, 0.13  # 10-13% for others
            
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Filter by probability ITM range (ticker-specific)
                    filtered = df[(df['probability_itm'] >= prob_min) & 
                                  (df['probability_itm'] <= prob_max) & 
                                  (df['probability_itm'].notna())]
                    
                    if len(filtered) == 0:
                        continue
                    
                    # Process per year
                    for year, year_group in filtered.groupby('year'):
                        yearly_data[year]['total_options'] += len(year_group)
                        
                        # Track unique expirations - for monthly: extract unique months (YYYY-MM)
                        for exp_date in year_group['expiration_date'].unique():
                            if pd.notna(exp_date):
                                exp_dt = pd.to_datetime(exp_date)
                                month_key = f"{exp_dt.year}-{exp_dt.month:02d}"  # YYYY-MM format
                                yearly_data[year]['unique_expirations'].add(month_key)
                        
                        # Track ITM expirations - for monthly: extract unique months where ITM == 'YES'
                        itm_rows = year_group[year_group['ITM'] == 'YES']
                        for exp_date in itm_rows['expiration_date'].unique():
                            if pd.notna(exp_date):
                                exp_dt = pd.to_datetime(exp_date)
                                month_key = f"{exp_dt.year}-{exp_dt.month:02d}"  # YYYY-MM format
                                yearly_data[year]['itm_expirations'].add(month_key)
                        
                        # Collect APY values (premium_yield_pct)
                        if 'premium_yield_pct' in year_group.columns:
                            apy_vals = year_group['premium_yield_pct'].dropna()
                            yearly_data[year]['apy_values'].extend(apy_vals.tolist())
                        
                except Exception as e:
                    continue
            
            # Second pass: fill missing months to ensure all 12 months are covered
            # Determine expanded probability range based on ticker
            if ticker == 'AAPL':
                exp_prob_min, exp_prob_max = 0.04, 0.11  # 4-11% for AAPL (expanded to fill all 12 months)
            else:
                exp_prob_min, exp_prob_max = 0.08, 0.13  # 8-13% for others
            
            for file in sorted(monthly_dir.glob('*_options_pessimistic.csv')):
                try:
                    df = pd.read_csv(file)
                    
                    if 'probability_itm' not in df.columns or 'ITM' not in df.columns:
                        continue
                    
                    # Extract year
                    if 'date_only' in df.columns:
                        df['year'] = pd.to_datetime(df['date_only']).dt.year
                    else:
                        year = file.stem.split('_')[0]
                        try:
                            df['year'] = int(year)
                        except:
                            continue
                    
                    # Process per year - check if year needs filling to reach 12 months
                    for year, year_group in df.groupby('year'):
                        # Check if this year has less than 12 unique months
                        current_months = len(yearly_data[year]['unique_expirations'])
                        if current_months < 12:
                            # Use expanded range to fill missing months
                            # Filter to only include months not already covered
                            filtered = year_group[(year_group['probability_itm'] >= exp_prob_min) & 
                                                  (year_group['probability_itm'] <= exp_prob_max) & 
                                                  (year_group['probability_itm'].notna())]
                            
                            if len(filtered) > 0:
                                # Track which months we're adding (to avoid duplicates)
                                new_months = set()
                                new_itm_months = set()
                                new_options = []
                                new_apy_values = []
                                
                                for _, row in filtered.iterrows():
                                    if pd.notna(row['expiration_date']):
                                        exp_dt = pd.to_datetime(row['expiration_date'])
                                        month_key = f"{exp_dt.year}-{exp_dt.month:02d}"
                                        
                                        # Only add if this month is not already covered
                                        if month_key not in yearly_data[year]['unique_expirations']:
                                            new_months.add(month_key)
                                            new_options.append(row)
                                            
                                            # Track ITM if applicable
                                            if row.get('ITM') == 'YES':
                                                new_itm_months.add(month_key)
                                            
                                            # Collect APY values
                                            if 'premium_yield_pct' in row and pd.notna(row['premium_yield_pct']):
                                                new_apy_values.append(row['premium_yield_pct'])
                                
                                # Add new months and data
                                if new_months:
                                    yearly_data[year]['total_options'] += len(new_options)
                                    yearly_data[year]['unique_expirations'].update(new_months)
                                    yearly_data[year]['itm_expirations'].update(new_itm_months)
                                    yearly_data[year]['apy_values'].extend(new_apy_values)
                        
                except Exception as e:
                    continue
    
    if yearly_data:
        # Convert sets to counts and calculate averages
        processed_yearly = {}
        for year, data in yearly_data.items():
            unique_exp_count = len(data['unique_expirations'])
            itm_exp_count = len(data['itm_expirations'])
            
            # Calculate average APY
            avg_apy = np.mean(data['apy_values']) if data['apy_values'] else 0
            
            # For monthly: ITM rate = (unique months with ITM / 12) * 100% (percentage of months with liquidations)
            # For weekly: ITM rate = (ITM expirations / unique expirations) * 100% (percentage of weeks with liquidations)
            if monthly_iv_level == 'low':
                # Monthly: unique months with ITM per year divided by 12 months
                # itm_exp_count is already the count of unique months (YYYY-MM format)
                itm_rate_per_year = (itm_exp_count / 12 * 100) if unique_exp_count > 0 else 0
                # APY: average of all monthly APYs
                yearly_apy = avg_apy
            else:
                # Weekly: ITM expirations divided by total unique expirations (weeks) in that year
                itm_rate_per_year = (itm_exp_count / unique_exp_count * 100) if unique_exp_count > 0 else 0
                # APY: average of all weekly APYs
                yearly_apy = avg_apy
            
            processed_yearly[year] = {
                'total_options': data['total_options'],
                'unique_expirations': unique_exp_count,
                'itm_expirations': itm_exp_count,
                'itm_rate_per_year': itm_rate_per_year,
                'avg_apy': yearly_apy
            }
        
        ticker_results[ticker] = {
            'iv_level': monthly_iv_level,
            'avg_iv': avg_iv,
            'option_type': 'Weekly' if monthly_iv_level == 'high' else 'Monthly',
            'prob_range': '4-7%' if monthly_iv_level == 'high' else ('7-11%' if ticker == 'AAPL' else '10-13%'),
            'yearly_data': processed_yearly
        }

# Create output file
output_file = Path("itm_liquidation_and_apy_per_year.txt")

with open(output_file, 'w') as f:
    f.write("=" * 120 + "\n")
    f.write("ITM LIQUIDATION COUNTS AND APY AVERAGES PER YEAR\n")
    f.write("=" * 120 + "\n")
    f.write(f"\nIV Threshold (median): {iv_threshold:.4f} ({iv_threshold*100:.2f}%)\n")
    f.write(f"Logic: Low IV → Monthly (10-13% prob ITM, expanded to 8-13% if needed; AAPL uses 7-11%, expanded to 6-11%), High IV → Weekly (4-7% prob ITM, expanded to 3-8% if needed)\n")
    f.write(f"ITM Count: Unique expiration dates with ITM == 'YES' (1 per expiration)\n")
    f.write(f"ITM Rate: For monthly: (ITM expirations / 12) * 100%, For weekly: (ITM expirations / total weeks) * 100%\n")
    f.write(f"APY: Average premium_yield_pct per year\n")
    f.write("\n")
    
    # Sort by ticker
    sorted_tickers = sorted(ticker_results.keys())
    
    # Tickers flagged as LOW
    low_flag_tickers = {'XLF', 'XLK', 'WMT'}
    
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        f.write("\n" + "=" * 120 + "\n")
        ticker_label = f"TICKER: {ticker}"
        if ticker in low_flag_tickers:
            ticker_label += " [FLAGGED AS LOW]"
        f.write(ticker_label + "\n")
        f.write("=" * 120 + "\n")
        f.write(f"  IV Level:           {r['iv_level'].upper()}\n")
        f.write(f"  Average IV:          {r['avg_iv']*100 if r['avg_iv'] else 0:.2f}%\n")
        f.write(f"  Option Type:         {r['option_type']}\n")
        f.write(f"  Probability Range:   {r['prob_range']}\n")
        f.write(f"\n")
        f.write(f"  Per Year Breakdown:\n")
        f.write(f"  {'Year':<8} {'Total Options':<15} {'Unique Exp':<12} {'ITM Exp':<10} {'ITM Rate/Year':<15} {'Avg APY':<12}\n")
        f.write(f"  {'-'*8} {'-'*15} {'-'*12} {'-'*10} {'-'*15} {'-'*12}\n")
        
        total_all_years = 0
        itm_all_years = 0
        unique_exp_all = 0
        apy_values_all = []
        
        for year in sorted(r['yearly_data'].keys()):
            year_data = r['yearly_data'][year]
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
            
            f.write(f"  {year:<8} {total:>14,} {unique_exp:>11,} {itm_exp:>9,} {itm_rate:>14.2f}% {avg_apy:>11.4f}%\n")
        
        # Calculate overall ITM rate
        if r['option_type'] == 'Monthly':
            # For monthly: average ITM expirations per year divided by 12
            num_years = len(r['yearly_data'])
            overall_itm_rate = (itm_all_years / (num_years * 12) * 100) if num_years > 0 else 0
        else:
            # For weekly: total ITM expirations divided by total unique expirations
            overall_itm_rate = (itm_all_years / unique_exp_all * 100) if unique_exp_all > 0 else 0
        overall_apy = np.mean(apy_values_all) if apy_values_all else 0
        
        f.write(f"  {'-'*8} {'-'*15} {'-'*12} {'-'*10} {'-'*15} {'-'*12}\n")
        f.write(f"  {'TOTAL':<8} {total_all_years:>14,} {unique_exp_all:>11,} {itm_all_years:>9,} {overall_itm_rate:>14.2f}% {overall_apy:>11.4f}%\n")
    
    # Summary table
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("SUMMARY TABLE (Overall Totals)\n")
    f.write("=" * 120 + "\n")
    f.write(f"{'Ticker':<9} {'IV%':<8} {'Type':<8} {'Prob':<8} {'Mkt Cap':<10} {'Flag':<6} {'Total':<10} {'ITM Exp':<10} {'ITM Rate':<12} {'Avg APY':<12}\n")
    f.write("-" * 120 + "\n")
    
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        total_all = sum(yd['total_options'] for yd in r['yearly_data'].values())
        itm_all = sum(yd['itm_expirations'] for yd in r['yearly_data'].values())
        unique_exp_all = sum(yd['unique_expirations'] for yd in r['yearly_data'].values())
        
        if r['option_type'] == 'Monthly':
            rate = (itm_all / (unique_exp_all / 12) * 100) if unique_exp_all > 0 else 0
        else:
            rate = (itm_all / unique_exp_all * 100) if unique_exp_all > 0 else 0
        
        apy_vals = [yd['avg_apy'] for yd in r['yearly_data'].values() if yd['avg_apy'] > 0]
        avg_apy = np.mean(apy_vals) if apy_vals else 0
        
        mkt_cap = MARKET_CAPS.get(ticker, 0)
        mkt_cap_str = f"${mkt_cap}B" if mkt_cap > 0 else "N/A"
        flag_str = "LOW" if ticker in low_flag_tickers else ""
        f.write(f"{ticker:<9} {r['avg_iv']*100 if r['avg_iv'] else 0:>6.2f}% {r['option_type']:<8} {r['prob_range']:<8} {mkt_cap_str:<10} {flag_str:<6} {total_all:>9,} {itm_all:>9,} {rate:>11.2f}% {avg_apy:>11.4f}%\n")
    
    # Lists for easy copy-paste
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("DATA AS LISTS (for easy copy-paste)\n")
    f.write("=" * 120 + "\n\n")
    
    f.write("Ticker Names:\n")
    f.write(str(sorted_tickers) + "\n\n")
    
    f.write("Overall ITM Expiration Counts (Unique Liquidations):\n")
    itm_counts = []
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        itm_all = sum(yd['itm_expirations'] for yd in r['yearly_data'].values())
        itm_counts.append(int(itm_all))
    f.write(str(itm_counts) + "\n\n")
    
    f.write("Overall ITM Rates (%):\n")
    rates = []
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        itm_all = sum(yd['itm_expirations'] for yd in r['yearly_data'].values())
        unique_exp_all = sum(yd['unique_expirations'] for yd in r['yearly_data'].values())
        # Calculate overall ITM rate
        if r['option_type'] == 'Monthly':
            # For monthly: average ITM expirations per year divided by 12
            num_years = len(r['yearly_data'])
            rate = (itm_all / (num_years * 12) * 100) if num_years > 0 else 0
        else:
            # For weekly: total ITM expirations divided by total unique expirations
            rate = (itm_all / unique_exp_all * 100) if unique_exp_all > 0 else 0
        rates.append(round(rate, 2))
    f.write(str(rates) + "\n\n")
    
    f.write("Overall Average APY (%):\n")
    apy_list = []
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        apy_vals = [yd['avg_apy'] for yd in r['yearly_data'].values() if yd['avg_apy'] > 0]
        avg_apy = np.mean(apy_vals) if apy_vals else 0
        apy_list.append(round(avg_apy, 4))
    f.write(str(apy_list) + "\n\n")
    
    f.write("Average Hit Rate per Stock (%):\n")
    hit_rate_avg_list = []
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        option_type = r['option_type']
        ticker_hit_rates = []
        
        for year_data in r['yearly_data'].values():
            itm_hits = year_data['itm_expirations']
            total_exp = year_data['unique_expirations']
            
            if option_type == 'Monthly':
                hit_rate = (itm_hits / 12 * 100) if total_exp > 0 else 0
            else:  # Weekly
                hit_rate = (itm_hits / total_exp * 100) if total_exp > 0 else 0
            
            ticker_hit_rates.append(hit_rate)
        
        avg_hit_rate = np.mean(ticker_hit_rates) if ticker_hit_rates else 0
        hit_rate_avg_list.append(float(round(avg_hit_rate, 2)))
    f.write(str(hit_rate_avg_list) + "\n\n")
    
    # New table: Per-year ITM Hit Rate Summary per Stock
    f.write("\n\n" + "=" * 120 + "\n")
    f.write("PER-YEAR ITM HIT RATE SUMMARY (PER STOCK)\n")
    f.write("=" * 120 + "\n")
    f.write("This table shows per stock, per year, the percentage of expirations (months/weeks) that had at least one ITM option.\n")
    f.write("For monthly: (ITM hits / 12 months) * 100%, For weekly: (ITM hits / total unique expirations) * 100%\n")
    f.write("ITM Hits = count of unique expiration dates where at least one option expired ITM (1 per expiration)\n")
    f.write("\n")
    
    # Collect all years across all tickers
    all_years = set()
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        all_years.update(r['yearly_data'].keys())
    all_years = sorted(all_years)
    
    # Write table header
    f.write(f"{'Ticker':<9} {'Type':<8} ")
    for year in all_years:
        f.write(f"{year} Hit Rate{'':<6}")
    f.write(f"{'Avg rate per stock':<20}\n")
    f.write("-" * 150 + "\n")
    
    # Write per-ticker, per-year data
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        option_type = r['option_type']
        
        f.write(f"{ticker:<9} {option_type:<8} ")
        
        ticker_hit_rates = []
        
        for year in all_years:
            if year in r['yearly_data']:
                year_data = r['yearly_data'][year]
                itm_hits = year_data['itm_expirations']
                total_exp = year_data['unique_expirations']
                
                if option_type == 'Monthly':
                    # For monthly: (ITM hits / 12) * 100%
                    hit_rate = (itm_hits / 12 * 100) if total_exp > 0 else 0
                else:  # Weekly
                    # For weekly: (ITM hits / total unique expirations) * 100%
                    hit_rate = (itm_hits / total_exp * 100) if total_exp > 0 else 0
                
                ticker_hit_rates.append(hit_rate)
                f.write(f"{hit_rate:>10.2f}%{'':<6}")
            else:
                f.write(f"{'N/A':<16}")
        
        # Calculate average hit rate per stock from the yearly hit rates
        avg_hit_rate = np.mean(ticker_hit_rates) if ticker_hit_rates else 0.0
        
        # Write the average hit rate for this stock
        f.write(f"{avg_hit_rate:>10.2f}%\n")
    
    # Summary row: Average across all tickers per year
    f.write("-" * 120 + "\n")
    f.write(f"{'AVG':<9} {'All':<8} ")
    
    overall_avg_rates = []
    
    for year in all_years:
        year_hit_rates = []
        for ticker in sorted_tickers:
            r = ticker_results[ticker]
            if year in r['yearly_data']:
                year_data = r['yearly_data'][year]
                itm_hits = year_data['itm_expirations']
                total_exp = year_data['unique_expirations']
                
                if r['option_type'] == 'Monthly':
                    hit_rate = (itm_hits / 12 * 100) if total_exp > 0 else 0
                else:  # Weekly
                    hit_rate = (itm_hits / total_exp * 100) if total_exp > 0 else 0
                
                year_hit_rates.append(hit_rate)
                overall_avg_rates.append(hit_rate)
        
        avg_rate = np.mean(year_hit_rates) if year_hit_rates else 0
        f.write(f"{avg_rate:>10.2f}%{'':<6}")
    
    # Calculate overall average ITM rate across all tickers
    itm_rates_all = []
    for ticker in sorted_tickers:
        r = ticker_results[ticker]
        total_itm = sum(yd['itm_expirations'] for yd in r['yearly_data'].values())
        total_unique_exp = sum(yd['unique_expirations'] for yd in r['yearly_data'].values())
        
        if r['option_type'] == 'Monthly':
            # For monthly: (total ITM / (num_years * 12)) * 100%
            num_years = len(r['yearly_data'])
            itm_rate = (total_itm / (num_years * 12) * 100) if num_years > 0 else 0
        else:  # Weekly
            # For weekly: (total ITM / total unique expirations) * 100%
            itm_rate = (total_itm / total_unique_exp * 100) if total_unique_exp > 0 else 0
        
        itm_rates_all.append(itm_rate)
    
    overall_avg_itm_rate = np.mean(itm_rates_all) if itm_rates_all else 0
    
    f.write(f"{overall_avg_itm_rate:>10.2f}%\n")

print(f"✅ Per-year ITM liquidation and APY data saved to: {output_file}")
print(f"   Open the file to inspect all results by year.")

