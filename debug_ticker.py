#!/usr/bin/env python3

def parse_option_ticker(ticker):
    """Parse option ticker to extract symbol, expiration, type, and strike."""
    if not ticker.startswith('O:'):
        return None
    ticker = ticker[2:]
    symbol_end = 0
    for i, char in enumerate(ticker):
        if char.isdigit():
            symbol_end = i
            break
    if symbol_end == 0:
        return None
    symbol = ticker[:symbol_end]
    remaining = ticker[symbol_end:]
    if len(remaining) < 7:
        return None
    expiration_str = remaining[:6]
    option_type = remaining[6]
    strike_str = remaining[7:]
    
    print(f"Ticker: {ticker}")
    print(f"  Symbol: {symbol}")
    print(f"  Remaining: {remaining}")
    print(f"  Expiration: {expiration_str}")
    print(f"  Type: {option_type}")
    print(f"  Strike str: {strike_str}")
    print(f"  Strike int: {int(strike_str)}")
    print(f"  Strike /1000: {int(strike_str) / 1000.0}")
    print()

# Test with problematic tickers
test_tickers = [
    "O:TSLA200221C00010000",
    "O:TSLA200221C01000000", 
    "O:TSLA200221C00200000",
    "O:TSLA190215C00200000",
    "O:TSLA160219C00160000"  # Add the 2016 example
]

for ticker in test_tickers:
    parse_option_ticker(ticker)
