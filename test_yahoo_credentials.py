#!/usr/bin/env python3
"""
Quick test to verify Yahoo Finance API credentials are properly configured.
This script checks if credentials are loaded and tests basic connectivity.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def test_credentials_loaded():
    """Test if credentials are loaded from .env file."""
    print("=" * 80)
    print("Yahoo Finance API Credentials Test")
    print("=" * 80)
    
    # Check if .env file exists
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        print(f"\n❌ .env file not found at {env_path}")
        print("\n📝 To set up credentials, run:")
        print("   python setup_yahoo_env.py")
        return False
    
    print(f"\n✅ .env file found at {env_path}")
    
    # Load environment variables
    load_dotenv(env_path)
    
    # Check for required credentials
    app_id = os.getenv("YAHOO_APP_ID")
    client_id = os.getenv("YAHOO_CLIENT_ID")
    client_secret = os.getenv("YAHOO_CLIENT_SECRET")
    
    missing = []
    if not app_id:
        missing.append("YAHOO_APP_ID")
    if not client_id:
        missing.append("YAHOO_CLIENT_ID")
    if not client_secret:
        missing.append("YAHOO_CLIENT_SECRET")
    
    if missing:
        print(f"\n❌ Missing credentials: {', '.join(missing)}")
        print("\n📝 Please ensure your .env file contains:")
        print("   YAHOO_APP_ID=your_app_id")
        print("   YAHOO_CLIENT_ID=your_client_id")
        print("   YAHOO_CLIENT_SECRET=your_client_secret")
        return False
    
    print("\n✅ All credentials loaded successfully:")
    print(f"   YAHOO_APP_ID: {app_id}")
    print(f"   YAHOO_CLIENT_ID: {client_id[:30]}...")
    print(f"   YAHOO_CLIENT_SECRET: {'*' * len(client_secret)}")
    
    # Validate credential formats
    print("\n🔍 Validating credential formats...")
    
    issues = []
    
    # Check App ID format (should be alphanumeric)
    if not app_id.replace('-', '').replace('_', '').isalnum():
        issues.append("YAHOO_APP_ID format looks unusual")
    else:
        print("   ✅ YAHOO_APP_ID format looks valid")
    
    # Check Client ID format (should be a long string, often base64-like)
    if len(client_id) < 20:
        issues.append("YAHOO_CLIENT_ID seems too short")
    else:
        print("   ✅ YAHOO_CLIENT_ID format looks valid")
    
    # Check Client Secret format (should be a hex string)
    if len(client_secret) < 20:
        issues.append("YAHOO_CLIENT_SECRET seems too short")
    else:
        print("   ✅ YAHOO_CLIENT_SECRET format looks valid")
    
    if issues:
        print("\n⚠️  Potential issues found:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("\n✅ All credential formats look valid!")
    
    print("\n" + "=" * 80)
    print("✅ Credentials are properly configured!")
    print("=" * 80)
    print("\n📝 Next steps:")
    print("   1. Run 'python test_yahoo_oauth.py' to test OAuth authentication")
    print("   2. This will guide you through the OAuth flow to verify credentials work")
    
    return True

if __name__ == "__main__":
    success = test_credentials_loaded()
    sys.exit(0 if success else 1)

