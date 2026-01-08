#!/usr/bin/env python3
"""
Test script to understand Massive API response structure.
"""

import requests
import json
import pandas as pd
from pathlib import Path

API_KEY = "7asm1ymlJwuul2I4LWFzsvVtuWDUd3Q0"
BASE_URL = "https://api.massive.com/v3/snapshot/options"

def convert_ticker_to_api_format(ticker, exchange="A"):
    """
    Convert our ticker format to API format.
    
    Our format: O:TSLA10219C006667000
    API format: O:A250815C00055000
    
    Steps:
    1. Remove O: prefix
    2. Extract symbol, expiration, type, strike
    3. Convert to API format with exchange code
    """
    if not ticker.startswith("O:"):
        return None
    
    ticker_clean = ticker[2:]  # Remove "O:"
    
    # Find where symbol ends (first digit)
    symbol_end = 0
    for i, char in enumerate(ticker_clean):
        if char.isdigit():
            symbol_end = i
            break
    
    if symbol_end == 0:
        return None
    
    symbol = ticker_clean[:symbol_end]  # TSLA
    remaining = ticker_clean[symbol_end:]  # 10219C006667000
    
    if len(remaining) < 15:
        return None
    
    expiration = remaining[:6]  # 10219 (YYMMDD)
    option_type = remaining[6]  # C or P
    strike_encoded = remaining[7:]  # 006667000
    
    # API format: O:{exchange}{expiration}{type}{strike}
    api_ticker = f"O:{exchange}{expiration}{option_type}{strike_encoded}"
    
    return api_ticker

def test_api_call(api_ticker, exchange="A"):
    """Test a single API call."""
    url = f"{BASE_URL}/{exchange}/{api_ticker}"
    params = {"apiKey": API_KEY}
    
    print(f"\nTesting API call:")
    print(f"  URL: {url}")
    print(f"  Ticker: {api_ticker}")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"  Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  Response: {json.dumps(data, indent=2)}")
            return data
        else:
            print(f"  Error Response: {response.text}")
            return None
    except Exception as e:
        print(f"  Exception: {e}")
        return None

def main():
    """Test API with actual TSLA tickers from 2021 data."""
    
    # Load a sample of TSLA 2021 data
    csv_path = Path("python-boilerplate/data/TSLA/monthly/2021_options_pessimistic.csv")
    
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return
    
    # Read first 10 rows
    df = pd.read_csv(csv_path, nrows=10)
    
    print("=" * 80)
    print("MASSIVE API TESTING")
    print("=" * 80)
    print(f"\nLoaded {len(df)} sample rows from {csv_path}")
    print(f"\nSample tickers:")
    for idx, ticker in enumerate(df['ticker'].head(5), 1):
        print(f"  {idx}. {ticker}")
    
    # Test ticker conversion
    print("\n" + "=" * 80)
    print("TICKER FORMAT CONVERSION")
    print("=" * 80)
    
    test_ticker = df['ticker'].iloc[0]
    print(f"\nOriginal ticker: {test_ticker}")
    
    # Try different exchanges
    exchanges = ["A", "Q", "X"]  # AMEX, NASDAQ, NYSE
    
    for exchange in exchanges:
        api_ticker = convert_ticker_to_api_format(test_ticker, exchange)
        print(f"\nExchange '{exchange}': {api_ticker}")
        
        # Test API call
        result = test_api_call(api_ticker, exchange)
        
        if result and result.get("status") != "ERROR":
            print(f"\n✅ SUCCESS with exchange '{exchange}'!")
            print(f"Found valid response structure")
            break
        else:
            print(f"❌ Failed with exchange '{exchange}'")

if __name__ == "__main__":
    main()

