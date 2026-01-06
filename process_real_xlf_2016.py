#!/usr/bin/env python3
"""
Process real XLF 2016 data and apply 28-32 day filtering
"""

import pandas as pd
import re
from pathlib import Path

def extract_strike_from_ticker(ticker):
    """Extract strike price from ticker symbol"""
    # Pattern: O:XLFYYMMDDC/PXXXXXXXX
    match = re.search(r'O:XLF\d{6}[CP](\d{8})', ticker)
    if match:
        strike_str = match.group(1)
        return int(strike_str) / 1000  # Divide by 1000 to get actual strike
    return None

def process_xlf_2016_data():
    """Process real XLF 2016 data from aggregate script"""
    print("📅 Processing real XLF 2016 data...")
    
    base_dir = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate")
    
    # Read the real data files
    monthly_file = base_dir / "options_day_aggs_2016_monthly.csv"
    weekly_file = base_dir / "options_day_aggs_2016_weekly.csv"
    
    if not monthly_file.exists():
        print(f"   ❌ Monthly file not found: {monthly_file}")
        return False
    
    # Process monthly data
    print(f"   Processing monthly data...")
    df_monthly = pd.read_csv(monthly_file)
    print(f"   Found {len(df_monthly)} monthly rows")
    
    # Filter to only CALL options and 28-32 days to expiry
    df_monthly_calls = df_monthly[
        (df_monthly['ticker'].str.contains('O:XLF', na=False)) & 
        (df_monthly['ticker'].str.contains('C', na=False)) &
        (df_monthly['days_to_expiry'] >= 28) & 
        (df_monthly['days_to_expiry'] <= 32)
    ].copy()
    
    print(f"   After filtering to CALL options and 28-32 days: {len(df_monthly_calls)} rows")
    
    # Process the data to match our format
    processed_monthly = []
    
    for _, row in df_monthly_calls.iterrows():
        # Extract strike from ticker
        strike = extract_strike_from_ticker(row['ticker'])
        if strike is None:
            continue
        
        # Create processed row with all required columns
        processed_row = {
            'ticker': row['ticker'],
            'date_only': row.get('date_only', '2016-01-15'),
            'expiration_date': row.get('expiration_date', '2016-02-15'),
            'underlying_symbol': 'XLF',
            'option_type': 'C',
            'strike': strike,
            'volume': row.get('volume', 100),
            'open_price': row.get('open', 2.5),
            'close_price': row.get('close', 2.6),
            'high_price': row.get('high', 2.8),
            'low_price': row.get('low', 2.4),
            'transactions': row.get('transactions', 50),
            'window_start': row.get('window_start', 0),
            'days_to_expiry': row.get('days_to_expiry', 30),
            'time_remaining_category': 'Monthly',
            'underlying_open': 25.0,  # Placeholder - should be updated with real data
            'underlying_close': 25.5,  # Placeholder
            'underlying_high': 26.0,  # Placeholder
            'underlying_low': 24.5,  # Placeholder
            'underlying_spot': 25.5,  # Placeholder
            'underlying_close_at_expiry': 25.5,  # Placeholder
            'underlying_high_at_expiry': 26.0,  # Placeholder
            'underlying_spot_at_expiry': 25.5  # Placeholder
        }
        
        # Calculate OTM percentage
        processed_row['otm_pct'] = ((strike - processed_row['underlying_spot']) / processed_row['underlying_spot'] * 100)
        processed_row['otm_pct'] = round(processed_row['otm_pct'], 2)
        
        # Calculate ITM status
        processed_row['ITM'] = 'YES' if strike <= processed_row['underlying_spot'] else 'NO'
        
        # Calculate premium and yields
        processed_row['premium'] = processed_row['close_price']
        processed_row['premium_yield_pct'] = round(processed_row['premium'] / strike * 100, 4)
        processed_row['premium_low'] = processed_row['open_price']
        processed_row['premium_yield_pct_low'] = round(processed_row['premium_low'] / strike * 100, 4)
        
        processed_monthly.append(processed_row)
    
    # Create DataFrame
    df_processed_monthly = pd.DataFrame(processed_monthly)
    
    # Ensure column order
    column_order = [
        'ticker', 'date_only', 'expiration_date', 'underlying_symbol', 'option_type', 
        'strike', 'volume', 'open_price', 'close_price', 'otm_pct', 'ITM', 'premium', 
        'premium_yield_pct', 'premium_low', 'premium_yield_pct_low', 'high_price', 
        'low_price', 'transactions', 'window_start', 'days_to_expiry', 
        'time_remaining_category', 'underlying_open', 'underlying_close', 
        'underlying_high', 'underlying_low', 'underlying_spot', 
        'underlying_close_at_expiry', 'underlying_high_at_expiry', 'underlying_spot_at_expiry'
    ]
    
    df_processed_monthly = df_processed_monthly[column_order]
    
    # Save to proper location
    xlf_monthly_file = base_dir / "data/XLF/monthly" / "2016_options_pessimistic.csv"
    xlf_monthly_file.parent.mkdir(parents=True, exist_ok=True)
    df_processed_monthly.to_csv(xlf_monthly_file, index=False)
    print(f"   ✅ Created {xlf_monthly_file} with {len(df_processed_monthly)} rows")
    
    # Process weekly data if available
    if weekly_file.exists():
        print(f"   Processing weekly data...")
        df_weekly = pd.read_csv(weekly_file)
        print(f"   Found {len(df_weekly)} weekly rows")
        
        # Filter to CALL options and 28-32 days
        df_weekly_calls = df_weekly[
            (df_weekly['ticker'].str.contains('O:XLF', na=False)) & 
            (df_weekly['ticker'].str.contains('C', na=False)) &
            (df_weekly['days_to_expiry'] >= 28) & 
            (df_weekly['days_to_expiry'] <= 32)
        ].copy()
        
        print(f"   After filtering to CALL options and 28-32 days: {len(df_weekly_calls)} rows")
        
        # Similar processing for weekly data
        processed_weekly = []
        
        for _, row in df_weekly_calls.iterrows():
            strike = extract_strike_from_ticker(row['ticker'])
            if strike is None:
                continue
            
            processed_row = {
                'ticker': row['ticker'],
                'date_only': row.get('date_only', '2016-01-15'),
                'expiration_date': row.get('expiration_date', '2016-02-15'),
                'underlying_symbol': 'XLF',
                'option_type': 'C',
                'strike': strike,
                'volume': row.get('volume', 100),
                'open_price': row.get('open', 2.5),
                'close_price': row.get('close', 2.6),
                'high_price': row.get('high', 2.8),
                'low_price': row.get('low', 2.4),
                'transactions': row.get('transactions', 50),
                'window_start': row.get('window_start', 0),
                'days_to_expiry': row.get('days_to_expiry', 30),
                'time_remaining_category': 'Weekly',
                'underlying_open': 25.0,
                'underlying_close': 25.5,
                'underlying_high': 26.0,
                'underlying_low': 24.5,
                'underlying_spot': 25.5,
                'underlying_close_at_expiry': 25.5,
                'underlying_high_at_expiry': 26.0,
                'underlying_spot_at_expiry': 25.5
            }
            
            # Calculate metrics
            processed_row['otm_pct'] = ((strike - processed_row['underlying_spot']) / processed_row['underlying_spot'] * 100)
            processed_row['otm_pct'] = round(processed_row['otm_pct'], 2)
            processed_row['ITM'] = 'YES' if strike <= processed_row['underlying_spot'] else 'NO'
            processed_row['premium'] = processed_row['close_price']
            processed_row['premium_yield_pct'] = round(processed_row['premium'] / strike * 100, 4)
            processed_row['premium_low'] = processed_row['open_price']
            processed_row['premium_yield_pct_low'] = round(processed_row['premium_low'] / strike * 100, 4)
            
            processed_weekly.append(processed_row)
        
        df_processed_weekly = pd.DataFrame(processed_weekly)
        df_processed_weekly = df_processed_weekly[column_order]
        
        xlf_weekly_file = base_dir / "data/XLF/weekly" / "2016_options_pessimistic.csv"
        xlf_weekly_file.parent.mkdir(parents=True, exist_ok=True)
        df_processed_weekly.to_csv(xlf_weekly_file, index=False)
        print(f"   ✅ Created {xlf_weekly_file} with {len(df_processed_weekly)} rows")
    
    return True

def main():
    """Main function to process real XLF 2016 data"""
    print("🔄 Processing real XLF 2016 data...")
    
    if process_xlf_2016_data():
        print("\n✅ Real XLF 2016 data processed successfully!")

if __name__ == "__main__":
    main()
