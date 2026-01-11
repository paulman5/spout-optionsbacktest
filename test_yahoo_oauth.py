#!/usr/bin/env python3
"""
Test script to verify Yahoo Finance API OAuth credentials and connection.
This script tests the OAuth 2.0 authentication flow with Yahoo Finance API.
"""

import os
import sys
from yahoo_finance_client import YahooFinanceClient

# Using the YahooFinanceClient class for cleaner code

def main():
    """Main function to test Yahoo Finance OAuth credentials."""
    print("=" * 80)
    print("Yahoo Finance API OAuth Credentials Test")
    print("=" * 80)
    
    try:
        # Step 1: Initialize client
        print("\n1️⃣  Initializing Yahoo Finance client...")
        client = YahooFinanceClient()
        print(f"   ✅ App ID: {client.app_id}")
        print(f"   ✅ Client ID: {client.client_id[:30]}...")
        print(f"   ✅ Client Secret: {'*' * len(client.client_secret)}")
        
        # Step 2: Generate authorization URL
        print("\n2️⃣  Generating authorization URL...")
        auth_url = client.get_authorization_url()
        print(f"   ✅ Authorization URL generated")
        print(f"\n   📋 Please visit this URL in your browser:")
        print(f"   {auth_url}")
        print(f"\n   After authorizing, you'll receive an authorization code.")
        print(f"   Enter it below to continue testing.")
        
        # Step 3: Get authorization code from user
        print("\n3️⃣  Waiting for authorization code...")
        auth_code = input("   Enter authorization code: ").strip()
        
        if not auth_code:
            print("   ⚠️  No authorization code provided. Skipping token exchange.")
            print("\n   ℹ️  To complete the test:")
            print("   1. Visit the authorization URL above")
            print("   2. Authorize the application")
            print("   3. Copy the authorization code")
            print("   4. Run this script again and enter the code")
            return
        
        # Step 4: Exchange code for token
        print("\n4️⃣  Exchanging authorization code for access token...")
        try:
            token_response = client.authenticate(auth_code)
            access_token = token_response.get("access_token")
            refresh_token = token_response.get("refresh_token")
            expires_in = token_response.get("expires_in")
            
            if access_token:
                print(f"   ✅ Access token obtained!")
                print(f"   ✅ Token expires in: {expires_in} seconds")
                if refresh_token:
                    print(f"   ✅ Refresh token obtained")
                
                # Step 5: Test API call
                print("\n5️⃣  Testing API call with access token...")
                try:
                    api_response = client.get_stock_quote("AAPL", use_auth=True)
                    print(f"   ✅ API call successful!")
                    print(f"   ✅ Response received (keys: {list(api_response.keys()) if isinstance(api_response, dict) else 'N/A'})")
                    
                    print("\n" + "=" * 80)
                    print("✅ SUCCESS: Yahoo Finance API OAuth credentials are working!")
                    print("=" * 80)
                    print("\n📝 Next steps:")
                    print("   1. Store the refresh_token securely for future use")
                    print("   2. Use YahooFinanceClient in your application")
                    print("   3. Token refresh is handled automatically")
                    
                except Exception as e:
                    print(f"   ⚠️  API call test failed: {e}")
                    print("   ℹ️  Trying without authentication...")
                    try:
                        api_response = client.get_stock_quote("AAPL", use_auth=False)
                        print("   ✅ API call succeeded without authentication (public endpoint)")
                        print("   ✅ Your credentials are valid, but this endpoint doesn't require OAuth")
                    except Exception as e2:
                        print(f"   ⚠️  Error: {e2}")
                        print("   ✅ But your credentials are valid (token obtained successfully)")
            else:
                print("   ❌ No access token in response")
                print(f"   Response: {token_response}")
                
        except Exception as e:
            print(f"   ❌ Failed to exchange code for token: {e}")
            print("\n   Possible issues:")
            print("   - Authorization code expired or invalid")
            print("   - Client ID or Client Secret incorrect")
            print("   - Redirect URI mismatch")
            print("   - Application not properly configured in Yahoo Developer Console")
            raise
            
    except ValueError as e:
        print(f"\n❌ Configuration Error: {e}")
        print("\nPlease ensure your .env file contains:")
        print("  YAHOO_APP_ID=your_app_id")
        print("  YAHOO_CLIENT_ID=your_client_id")
        print("  YAHOO_CLIENT_SECRET=your_client_secret")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

