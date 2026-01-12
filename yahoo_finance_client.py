#!/usr/bin/env python3
"""
Yahoo Finance API Client with OAuth 2.0 support.
This module provides a client for interacting with Yahoo Finance API using OAuth credentials.
"""

import os
import base64
import requests
from urllib.parse import urlencode
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Yahoo Finance OAuth endpoints
YAHOO_AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
YAHOO_TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
YAHOO_API_BASE = "https://query1.finance.yahoo.com"


class YahooFinanceClient:
    """Client for Yahoo Finance API with OAuth 2.0 authentication."""
    
    def __init__(self, app_id: Optional[str] = None, 
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 redirect_uri: str = "oob"):
        """
        Initialize Yahoo Finance API client.
        
        Args:
            app_id: Yahoo App ID (defaults to YAHOO_APP_ID env var)
            client_id: Yahoo Client ID (defaults to YAHOO_CLIENT_ID env var)
            client_secret: Yahoo Client Secret (defaults to YAHOO_CLIENT_SECRET env var)
            redirect_uri: OAuth redirect URI (defaults to "oob" for desktop apps)
        """
        self.app_id = app_id or os.getenv("YAHOO_APP_ID")
        self.client_id = client_id or os.getenv("YAHOO_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("YAHOO_CLIENT_SECRET")
        self.redirect_uri = redirect_uri
        
        if not all([self.app_id, self.client_id, self.client_secret]):
            raise ValueError(
                "Missing Yahoo Finance credentials. Please set YAHOO_APP_ID, "
                "YAHOO_CLIENT_ID, and YAHOO_CLIENT_SECRET environment variables"
            )
        
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
    
    def get_authorization_url(self) -> str:
        """
        Generate the authorization URL for OAuth 2.0 flow.
        
        Returns:
            Authorization URL that user needs to visit
        """
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "language": "en-us"
        }
        
        auth_url = f"{YAHOO_AUTH_URL}?{urlencode(params)}"
        return auth_url
    
    def authenticate(self, auth_code: str) -> Dict[str, Any]:
        """
        Exchange authorization code for access token.
        
        Args:
            auth_code: Authorization code from OAuth flow
        
        Returns:
            Dictionary with access_token, refresh_token, expires_in, etc.
        """
        # Create Basic Auth header
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri
        }
        
        try:
            response = requests.post(YAHOO_TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Store tokens
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            
            if self.access_token:
                import time
                self.token_expires_at = time.time() + expires_in
            
            return token_data
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP Error: {e}"
            try:
                error_msg += f"\nResponse: {response.text}"
            except:
                pass
            raise Exception(error_msg) from e
    
    def refresh_access_token(self) -> Dict[str, Any]:
        """
        Refresh an expired access token using refresh token.
        
        Returns:
            Dictionary with new access_token, refresh_token, expires_in, etc.
        """
        if not self.refresh_token:
            raise ValueError("No refresh token available. Please authenticate first.")
        
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }
        
        try:
            response = requests.post(YAHOO_TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            token_data = response.json()
            
            # Update tokens
            self.access_token = token_data.get("access_token")
            new_refresh_token = token_data.get("refresh_token")
            if new_refresh_token:
                self.refresh_token = new_refresh_token
            expires_in = token_data.get("expires_in", 3600)
            
            if self.access_token:
                import time
                self.token_expires_at = time.time() + expires_in
            
            return token_data
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP Error refreshing token: {e}"
            try:
                error_msg += f"\nResponse: {response.text}"
            except:
                pass
            raise Exception(error_msg) from e
    
    def _ensure_valid_token(self):
        """Ensure we have a valid access token, refreshing if necessary."""
        if not self.access_token:
            raise ValueError("No access token. Please authenticate first.")
        
        # Check if token is expired (with 60 second buffer)
        if self.token_expires_at:
            import time
            if time.time() >= (self.token_expires_at - 60):
                if self.refresh_token:
                    self.refresh_access_token()
                else:
                    raise ValueError("Access token expired and no refresh token available.")
    
    def get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests with authentication.
        
        Returns:
            Dictionary with Authorization header
        """
        self._ensure_valid_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }
    
    def get_stock_quote(self, symbol: str, use_auth: bool = False, retry: bool = True, delay: float = 2.0) -> Dict[str, Any]:
        """
        Get stock quote for a symbol.
        
        Args:
            symbol: Stock symbol (e.g., "AAPL")
            use_auth: Whether to use OAuth authentication (some endpoints may not require it)
            retry: Whether to retry on rate limit errors
            delay: Delay in seconds between retries
        
        Returns:
            Stock quote data
        """
        url = f"{YAHOO_API_BASE}/v8/finance/chart/{symbol}"
        
        headers = {}
        if use_auth and self.access_token:
            headers = self.get_headers()
        else:
            headers = {"Accept": "application/json"}
        
        max_retries = 3 if retry else 1
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers)
                
                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = delay * (attempt + 1)
                        print(f"   ⚠️  Rate limited. Waiting {wait_time:.1f} seconds before retry {attempt + 2}/{max_retries}...")
                        import time
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(
                            f"Rate limited after {max_retries} attempts. "
                            f"Consider using OAuth authentication for higher rate limits."
                        )
                
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429 and attempt < max_retries - 1:
                    wait_time = delay * (attempt + 1)
                    print(f"   ⚠️  Rate limited. Waiting {wait_time:.1f} seconds before retry {attempt + 2}/{max_retries}...")
                    import time
                    time.sleep(wait_time)
                    continue
                error_msg = f"Error fetching stock quote: {e}"
                try:
                    error_msg += f"\nResponse: {response.text}"
                except:
                    pass
                raise Exception(error_msg) from e
    
    def set_tokens(self, access_token: str, refresh_token: Optional[str] = None, 
                   expires_in: Optional[int] = None):
        """
        Set access and refresh tokens directly (useful for persisting tokens).
        
        Args:
            access_token: OAuth access token
            refresh_token: OAuth refresh token (optional)
            expires_in: Token expiration time in seconds (optional)
        """
        self.access_token = access_token
        if refresh_token:
            self.refresh_token = refresh_token
        if expires_in:
            import time
            self.token_expires_at = time.time() + expires_in


def get_client() -> YahooFinanceClient:
    """
    Convenience function to get a YahooFinanceClient instance with credentials from environment.
    
    Returns:
        YahooFinanceClient instance
    """
    return YahooFinanceClient()

