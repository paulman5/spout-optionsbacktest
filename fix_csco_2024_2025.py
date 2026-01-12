#!/usr/bin/env python3
"""
Fix CSCO 2024 and 2025 monthly files:
1. Fix strike prices (divide ticker strike by 1000)
2. Merge historical stock data
3. Recalculate OTM percentage
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

base_dir = Path("python-boilerplate/data/CSCO")
stock_csv = base_dir / "HistoricalData_CSCO.csv"
directories = ['monthly', 'holidays']
years = ['2024', '2025']

print("=" * 80)
print("FIXING CSCO 2024 AND 2025 FILES")
print("=" * 80)
print("1. Fix strike prices (divide ticker strike by 1000)")
print("2. Merge historical stock data")
print("3. Recalculate OTM percentage")
print()

def load_historical_stock_prices(csv_path: Path) -> pd.DataFrame:
    """Load historical stock prices from CSV file."""
    df = pd.read_csv(csv_path)
    
    # Parse date column (assuming MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date
    
    # Extract numeric values from price columns (remove $ and commas)
    def clean_price(value):
        if pd.isna(value):
            return None
        str_val = str(value).replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except ValueError:
            return None
    
    df['close'] = df['Close/Last'].apply(clean_price)
    df['open'] = df['Open'].apply(clean_price)
    df['high'] = df['High'].apply(clean_price)
    df['low'] = df['Low'].apply(clean_price)
    
    result = df[['date', 'open', 'high', 'low', 'close']].copy()
    result = result.sort_values('date').reset_index(drop=True)
    
    return result

def extract_strike_from_ticker(ticker: str) -> float:
    """Extract strike from ticker and divide by 1000."""
    try:
        if ticker.startswith('O:'):
            ticker = ticker[2:]
        
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        strike_str = match.group(4)  # e.g., "00045000"
        strike = float(strike_str) / 1000.0  # e.g., 45.0
        return strike
    except (ValueError, IndexError, AttributeError, TypeError):
        return None

# Load stock prices
if not stock_csv.exists():
    print(f"❌ Stock data file not found: {stock_csv}")
    exit(1)

print(f"Loading stock data from {stock_csv.name}...")
stock_prices = load_historical_stock_prices(stock_csv)
print(f"   Loaded {len(stock_prices):,} stock price records")
print()

# Create lookup dictionary
stock_price_dict_date = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
stock_price_dict_exp = stock_prices.set_index('date')[['close', 'high']].to_dict('index')

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        continue
    
    for year in years:
        file = data_dir / f"{year}_options_pessimistic.csv"
        if not file.exists():
            print(f"⚠️  File not found: {file}")
            continue
        
        print(f"{'='*80}")
        print(f"Processing {subdir}/{year}...")
        print(f"{'='*80}")
        
        try:
            df = pd.read_csv(file)
            original_rows = len(df)
            print(f"   Loaded {original_rows:,} rows")
            
            # Step 1: Fix strike prices
            print(f"\n   1. Fixing strike prices...")
            strikes_before = df['strike'].copy()
            
            # Extract strike from ticker for all rows
            df['strike_new'] = df['ticker'].apply(extract_strike_from_ticker)
            
            # Count how many need fixing
            needs_fix = df['strike_new'].notna() & (df['strike'] != df['strike_new'])
            fix_count = needs_fix.sum()
            
            if fix_count > 0:
                print(f"      Rows to fix: {fix_count:,}")
                print(f"      Strike range before: {df['strike'].min():.2f} to {df['strike'].max():.2f}")
                df.loc[needs_fix, 'strike'] = df.loc[needs_fix, 'strike_new']
                print(f"      Strike range after: {df['strike'].min():.2f} to {df['strike'].max():.2f}")
            else:
                print(f"      ✅ Strikes already correct")
            
            df = df.drop(columns=['strike_new'], errors='ignore')
            
            # Step 2: Merge stock data
            print(f"\n   2. Merging stock data...")
            
            # Convert dates
            df['date_only_date'] = pd.to_datetime(df['date_only']).dt.date
            if 'expiration_date' in df.columns:
                df['expiration_date_date'] = pd.to_datetime(df['expiration_date']).dt.date
            
            # Drop existing underlying columns if they exist
            underlying_cols = ['underlying_open', 'underlying_close', 'underlying_high', 
                             'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
                             'underlying_high_at_expiry', 'underlying_spot_at_expiry']
            df = df.drop(columns=[col for col in underlying_cols if col in df.columns], errors='ignore')
            
            # Add entry date prices
            def get_date_price(row, price_type):
                date_val = row['date_only_date']
                if date_val in stock_price_dict_date:
                    return stock_price_dict_date[date_val][price_type]
                return None
            
            df['underlying_open'] = df.apply(lambda row: get_date_price(row, 'open'), axis=1)
            df['underlying_close'] = df.apply(lambda row: get_date_price(row, 'close'), axis=1)
            df['underlying_high'] = df.apply(lambda row: get_date_price(row, 'high'), axis=1)
            df['underlying_low'] = df.apply(lambda row: get_date_price(row, 'low'), axis=1)
            
            # Set underlying_spot (pessimistic = high)
            df['underlying_spot'] = df['underlying_high']
            
            # Add expiration prices
            if 'expiration_date_date' in df.columns:
                def get_exp_price(row, price_type):
                    exp_date = row['expiration_date_date']
                    if exp_date in stock_price_dict_exp:
                        return stock_price_dict_exp[exp_date][price_type]
                    return None
                
                df['underlying_close_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
                df['underlying_high_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
                df['underlying_spot_at_expiry'] = df['underlying_high_at_expiry']  # pessimistic
            
            # Drop temporary columns
            df = df.drop(columns=['date_only_date', 'expiration_date_date'], errors='ignore')
            
            has_spot = df['underlying_spot'].notna().sum()
            print(f"      Rows with underlying_spot: {has_spot:,} / {original_rows:,} ({has_spot/original_rows*100:.1f}%)")
            
            # Step 3: Recalculate OTM percentage
            print(f"\n   3. Recalculating OTM percentage...")
            if 'otm_pct' in df.columns:
                print(f"      OTM range before: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
            df['otm_pct'] = df['otm_pct'].round(2)
            
            print(f"      OTM range after: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Update ITM column based on OTM percentage
            if 'ITM' in df.columns:
                df['ITM'] = df['otm_pct'].apply(lambda x: 'YES' if x < 0 else 'NO')
            
            # Ensure mid_price exists
            if 'mid_price' not in df.columns:
                if 'high_price' in df.columns and 'low_price' in df.columns:
                    df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
                else:
                    df['mid_price'] = df['close_price']
            
            # Ensure fedfunds_rate exists
            if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
                df['fedfunds_rate'] = 0.02
            
            # Save the file
            print(f"\n   Saving file...")
            df.to_csv(file, index=False)
            print(f"   ✅ Fixed and saved {file.name}")
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()

print(f"\n{'='*80}")
print("✅ COMPLETE!")
print(f"{'='*80}")


Fix CSCO 2024 and 2025 monthly files:
1. Fix strike prices (divide ticker strike by 1000)
2. Merge historical stock data
3. Recalculate OTM percentage
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

base_dir = Path("python-boilerplate/data/CSCO")
stock_csv = base_dir / "HistoricalData_CSCO.csv"
directories = ['monthly', 'holidays']
years = ['2024', '2025']

print("=" * 80)
print("FIXING CSCO 2024 AND 2025 FILES")
print("=" * 80)
print("1. Fix strike prices (divide ticker strike by 1000)")
print("2. Merge historical stock data")
print("3. Recalculate OTM percentage")
print()

def load_historical_stock_prices(csv_path: Path) -> pd.DataFrame:
    """Load historical stock prices from CSV file."""
    df = pd.read_csv(csv_path)
    
    # Parse date column (assuming MM/DD/YYYY format)
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').dt.date
    
    # Extract numeric values from price columns (remove $ and commas)
    def clean_price(value):
        if pd.isna(value):
            return None
        str_val = str(value).replace('$', '').replace(',', '').strip()
        try:
            return float(str_val)
        except ValueError:
            return None
    
    df['close'] = df['Close/Last'].apply(clean_price)
    df['open'] = df['Open'].apply(clean_price)
    df['high'] = df['High'].apply(clean_price)
    df['low'] = df['Low'].apply(clean_price)
    
    result = df[['date', 'open', 'high', 'low', 'close']].copy()
    result = result.sort_values('date').reset_index(drop=True)
    
    return result

def extract_strike_from_ticker(ticker: str) -> float:
    """Extract strike from ticker and divide by 1000."""
    try:
        if ticker.startswith('O:'):
            ticker = ticker[2:]
        
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)', ticker)
        if not match:
            return None
        
        strike_str = match.group(4)  # e.g., "00045000"
        strike = float(strike_str) / 1000.0  # e.g., 45.0
        return strike
    except (ValueError, IndexError, AttributeError, TypeError):
        return None

# Load stock prices
if not stock_csv.exists():
    print(f"❌ Stock data file not found: {stock_csv}")
    exit(1)

print(f"Loading stock data from {stock_csv.name}...")
stock_prices = load_historical_stock_prices(stock_csv)
print(f"   Loaded {len(stock_prices):,} stock price records")
print()

# Create lookup dictionary
stock_price_dict_date = stock_prices.set_index('date')[['open', 'close', 'high', 'low']].to_dict('index')
stock_price_dict_exp = stock_prices.set_index('date')[['close', 'high']].to_dict('index')

for subdir in directories:
    data_dir = base_dir / subdir
    if not data_dir.exists():
        continue
    
    for year in years:
        file = data_dir / f"{year}_options_pessimistic.csv"
        if not file.exists():
            print(f"⚠️  File not found: {file}")
            continue
        
        print(f"{'='*80}")
        print(f"Processing {subdir}/{year}...")
        print(f"{'='*80}")
        
        try:
            df = pd.read_csv(file)
            original_rows = len(df)
            print(f"   Loaded {original_rows:,} rows")
            
            # Step 1: Fix strike prices
            print(f"\n   1. Fixing strike prices...")
            strikes_before = df['strike'].copy()
            
            # Extract strike from ticker for all rows
            df['strike_new'] = df['ticker'].apply(extract_strike_from_ticker)
            
            # Count how many need fixing
            needs_fix = df['strike_new'].notna() & (df['strike'] != df['strike_new'])
            fix_count = needs_fix.sum()
            
            if fix_count > 0:
                print(f"      Rows to fix: {fix_count:,}")
                print(f"      Strike range before: {df['strike'].min():.2f} to {df['strike'].max():.2f}")
                df.loc[needs_fix, 'strike'] = df.loc[needs_fix, 'strike_new']
                print(f"      Strike range after: {df['strike'].min():.2f} to {df['strike'].max():.2f}")
            else:
                print(f"      ✅ Strikes already correct")
            
            df = df.drop(columns=['strike_new'], errors='ignore')
            
            # Step 2: Merge stock data
            print(f"\n   2. Merging stock data...")
            
            # Convert dates
            df['date_only_date'] = pd.to_datetime(df['date_only']).dt.date
            if 'expiration_date' in df.columns:
                df['expiration_date_date'] = pd.to_datetime(df['expiration_date']).dt.date
            
            # Drop existing underlying columns if they exist
            underlying_cols = ['underlying_open', 'underlying_close', 'underlying_high', 
                             'underlying_low', 'underlying_spot', 'underlying_close_at_expiry',
                             'underlying_high_at_expiry', 'underlying_spot_at_expiry']
            df = df.drop(columns=[col for col in underlying_cols if col in df.columns], errors='ignore')
            
            # Add entry date prices
            def get_date_price(row, price_type):
                date_val = row['date_only_date']
                if date_val in stock_price_dict_date:
                    return stock_price_dict_date[date_val][price_type]
                return None
            
            df['underlying_open'] = df.apply(lambda row: get_date_price(row, 'open'), axis=1)
            df['underlying_close'] = df.apply(lambda row: get_date_price(row, 'close'), axis=1)
            df['underlying_high'] = df.apply(lambda row: get_date_price(row, 'high'), axis=1)
            df['underlying_low'] = df.apply(lambda row: get_date_price(row, 'low'), axis=1)
            
            # Set underlying_spot (pessimistic = high)
            df['underlying_spot'] = df['underlying_high']
            
            # Add expiration prices
            if 'expiration_date_date' in df.columns:
                def get_exp_price(row, price_type):
                    exp_date = row['expiration_date_date']
                    if exp_date in stock_price_dict_exp:
                        return stock_price_dict_exp[exp_date][price_type]
                    return None
                
                df['underlying_close_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'close'), axis=1)
                df['underlying_high_at_expiry'] = df.apply(lambda row: get_exp_price(row, 'high'), axis=1)
                df['underlying_spot_at_expiry'] = df['underlying_high_at_expiry']  # pessimistic
            
            # Drop temporary columns
            df = df.drop(columns=['date_only_date', 'expiration_date_date'], errors='ignore')
            
            has_spot = df['underlying_spot'].notna().sum()
            print(f"      Rows with underlying_spot: {has_spot:,} / {original_rows:,} ({has_spot/original_rows*100:.1f}%)")
            
            # Step 3: Recalculate OTM percentage
            print(f"\n   3. Recalculating OTM percentage...")
            if 'otm_pct' in df.columns:
                print(f"      OTM range before: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            df['otm_pct'] = ((df['strike'] - df['underlying_spot']) / df['underlying_spot']) * 100
            df['otm_pct'] = df['otm_pct'].round(2)
            
            print(f"      OTM range after: {df['otm_pct'].min():.2f}% to {df['otm_pct'].max():.2f}%")
            
            # Update ITM column based on OTM percentage
            if 'ITM' in df.columns:
                df['ITM'] = df['otm_pct'].apply(lambda x: 'YES' if x < 0 else 'NO')
            
            # Ensure mid_price exists
            if 'mid_price' not in df.columns:
                if 'high_price' in df.columns and 'low_price' in df.columns:
                    df['mid_price'] = (df['high_price'] + df['low_price']) / 2.0
                else:
                    df['mid_price'] = df['close_price']
            
            # Ensure fedfunds_rate exists
            if 'fedfunds_rate' not in df.columns or df['fedfunds_rate'].isna().all():
                df['fedfunds_rate'] = 0.02
            
            # Save the file
            print(f"\n   Saving file...")
            df.to_csv(file, index=False)
            print(f"   ✅ Fixed and saved {file.name}")
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()

print(f"\n{'='*80}")
print("✅ COMPLETE!")
print(f"{'='*80}")