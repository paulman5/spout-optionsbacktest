# Yahoo Finance API Setup Guide

This guide explains how to use the Yahoo Finance API OAuth credentials that have been configured in this project.

## Credentials Status

✅ **Credentials are configured and loaded successfully!**

- **App ID**: `Z8JuxEOd`
- **Client ID**: Configured in `.env` file
- **Client Secret**: Configured in `.env` file

## Files Created

1. **`.env`** - Contains your Yahoo Finance API credentials (not committed to git)
2. **`yahoo_finance_client.py`** - Reusable client class for Yahoo Finance API
3. **`test_yahoo_credentials.py`** - Quick test to verify credentials are loaded
4. **`test_yahoo_oauth.py`** - Full OAuth flow test script
5. **`setup_yahoo_env.py`** - Helper script to set up credentials

## Quick Start

### 1. Verify Credentials Are Loaded

```bash
python3 test_yahoo_credentials.py
```

This will verify that all credentials are properly loaded from the `.env` file.

### 2. Test OAuth Authentication

To test if your credentials work with Yahoo Finance API:

```bash
python3 test_yahoo_oauth.py
```

This will:
1. Generate an authorization URL
2. Guide you to visit the URL and authorize the application
3. Exchange the authorization code for an access token
4. Test making an API call with the access token

**Note**: The OAuth flow requires manual interaction (visiting a URL in your browser).

## Using the Yahoo Finance Client

### Basic Usage

```python
from yahoo_finance_client import YahooFinanceClient

# Create client (automatically loads credentials from .env)
client = YahooFinanceClient()

# Get authorization URL for OAuth flow
auth_url = client.get_authorization_url()
print(f"Visit: {auth_url}")

# After getting authorization code from user
auth_code = input("Enter authorization code: ")
token_data = client.authenticate(auth_code)

# Now you can make authenticated API calls
quote = client.get_stock_quote("AAPL", use_auth=True)
print(quote)
```

### Using Without OAuth (Public Endpoints)

Many Yahoo Finance endpoints work without authentication:

```python
from yahoo_finance_client import YahooFinanceClient

client = YahooFinanceClient()

# Public endpoint - no authentication needed
quote = client.get_stock_quote("AAPL", use_auth=False)
print(quote)
```

## Important Notes

### OAuth vs Public API

- **OAuth Required**: User account data, portfolio data, higher rate limits
- **OAuth Optional**: Most market data endpoints (stock quotes, historical data, etc.)

The `yfinance` library (already used in this project) accesses Yahoo Finance's public endpoints without OAuth. The OAuth credentials are useful for:
- Accessing user-specific data
- Higher API rate limits
- Official API access (more reliable than scraping)

### Token Management

Access tokens expire after a certain time (typically 1 hour). The client includes automatic token refresh:

```python
# Tokens are automatically refreshed when expired
client._ensure_valid_token()  # Internal method, called automatically
```

To persist tokens between sessions:

```python
# Save tokens after authentication
access_token = client.access_token
refresh_token = client.refresh_token

# Restore tokens later
client.set_tokens(access_token, refresh_token, expires_in=3600)
```

## Integration with Existing Code

The existing codebase uses `yfinance` for fetching stock prices (see `python-boilerplate/src/backtesting/weekly-monthly/monthly.py`). 

You can optionally replace or supplement `yfinance` calls with the OAuth-authenticated client:

```python
# Old way (using yfinance)
import yfinance as yf
ticker = yf.Ticker("AAPL")
hist = ticker.history(start="2023-01-01", end="2023-12-31")

# New way (using OAuth client - for authenticated endpoints)
from yahoo_finance_client import YahooFinanceClient
client = YahooFinanceClient()
# ... authenticate first ...
quote = client.get_stock_quote("AAPL", use_auth=True)
```

## Troubleshooting

### Credentials Not Found

If you see "Missing Yahoo Finance credentials":
1. Ensure `.env` file exists in the project root
2. Run `python3 setup_yahoo_env.py` to recreate it
3. Verify the file contains all three credentials

### OAuth Authentication Fails

If OAuth authentication fails:
1. Verify credentials are correct in Yahoo Developer Console
2. Check that redirect URI matches (should be "oob" for desktop apps)
3. Ensure the application is properly configured in Yahoo Developer Console
4. Authorization codes expire quickly - use them immediately

### API Calls Fail

If API calls fail:
1. Try without authentication first (`use_auth=False`)
2. Check if the endpoint requires OAuth
3. Verify your access token hasn't expired
4. Check Yahoo Finance API documentation for endpoint requirements

## Next Steps

1. ✅ Credentials are configured
2. ✅ Test scripts are ready
3. 🔄 Test OAuth flow: `python3 test_yahoo_oauth.py`
4. 🔄 Integrate into your application as needed

## References

- [Yahoo OAuth 2.0 Guide](https://developer.yahoo.com/oauth2/guide/)
- [Yahoo Developer Network](https://developer.yahoo.com/)

