#!/usr/bin/env python3
"""
Fetch EPS (Earnings Per Share) data for all tickers using yfinance.
Saves quarterly and annual EPS data to CSV files in results/earnings_data/
"""

import yfinance as yf
import pandas as pd
from pathlib import Path
import time
from datetime import datetime

# Base directories
base_dir = Path("python-boilerplate/data")
output_dir = Path("results/earnings_data")
output_dir.mkdir(parents=True, exist_ok=True)

# Get all ticker symbols from the data directory
def get_all_tickers():
    """Get all ticker symbols from the data directory."""
    tickers = []
    for ticker_dir in sorted(base_dir.iterdir()):
        if ticker_dir.is_dir() and not ticker_dir.name.startswith('.'):
            tickers.append(ticker_dir.name)
    return tickers

def fetch_eps_data(ticker):
    """
    Fetch EPS data for a ticker using yfinance.
    Returns a dictionary with quarterly and annual EPS data.
    """
    try:
        stock = yf.Ticker(ticker)
        
        # Get quarterly financials
        quarterly_financials = stock.quarterly_financials
        quarterly_eps = None
        if quarterly_financials is not None and not quarterly_financials.empty:
            # Look for EPS-related columns
            eps_cols = [col for col in quarterly_financials.index if 'EPS' in str(col).upper()]
            if eps_cols:
                quarterly_eps = quarterly_financials.loc[eps_cols].T
                quarterly_eps.index.name = 'Date'
                quarterly_eps = quarterly_eps.reset_index()
        
        # Get annual financials
        annual_financials = stock.financials
        annual_eps = None
        if annual_financials is not None and not annual_financials.empty:
            # Look for EPS-related columns
            eps_cols = [col for col in annual_financials.index if 'EPS' in str(col).upper()]
            if eps_cols:
                annual_eps = annual_financials.loc[eps_cols].T
                annual_eps.index.name = 'Date'
                annual_eps = annual_eps.reset_index()
        
        # Get earnings history (more detailed EPS data)
        earnings_history = None
        try:
            earnings_history = stock.earnings_history
            if earnings_history is not None and not earnings_history.empty:
                earnings_history = earnings_history.reset_index()
        except:
            pass
        
        # Get quarterly earnings (simplified)
        quarterly_earnings = None
        try:
            quarterly_earnings = stock.quarterly_earnings
            if quarterly_earnings is not None and not quarterly_earnings.empty:
                quarterly_earnings = quarterly_earnings.reset_index()
        except:
            pass
        
        return {
            'quarterly_financials_eps': quarterly_eps,
            'annual_financials_eps': annual_eps,
            'earnings_history': earnings_history,
            'quarterly_earnings': quarterly_earnings
        }
    except Exception as e:
        print(f"   ⚠️  Error fetching data for {ticker}: {str(e)}")
        return None

def save_eps_data(ticker, eps_data):
    """Save EPS data to CSV files."""
    if eps_data is None:
        return
    
    saved_files = []
    
    # Save quarterly financials EPS
    if eps_data['quarterly_financials_eps'] is not None:
        file_path = output_dir / f"{ticker}_quarterly_financials_eps.csv"
        eps_data['quarterly_financials_eps'].to_csv(file_path, index=False)
        saved_files.append(file_path.name)
        print(f"      ✓ Saved quarterly financials EPS ({len(eps_data['quarterly_financials_eps'])} rows)")
    
    # Save annual financials EPS
    if eps_data['annual_financials_eps'] is not None:
        file_path = output_dir / f"{ticker}_annual_financials_eps.csv"
        eps_data['annual_financials_eps'].to_csv(file_path, index=False)
        saved_files.append(file_path.name)
        print(f"      ✓ Saved annual financials EPS ({len(eps_data['annual_financials_eps'])} rows)")
    
    # Save earnings history
    if eps_data['earnings_history'] is not None:
        file_path = output_dir / f"{ticker}_earnings_history.csv"
        eps_data['earnings_history'].to_csv(file_path, index=False)
        saved_files.append(file_path.name)
        print(f"      ✓ Saved earnings history ({len(eps_data['earnings_history'])} rows)")
    
    # Save quarterly earnings
    if eps_data['quarterly_earnings'] is not None:
        file_path = output_dir / f"{ticker}_quarterly_earnings.csv"
        eps_data['quarterly_earnings'].to_csv(file_path, index=False)
        saved_files.append(file_path.name)
        print(f"      ✓ Saved quarterly earnings ({len(eps_data['quarterly_earnings'])} rows)")
    
    if not saved_files:
        print(f"      ⚠️  No EPS data found for {ticker}")

def main():
    """Main function to fetch EPS data for all tickers."""
    print("=" * 80)
    print("FETCHING EPS DATA FOR ALL TICKERS USING YFINANCE")
    print("=" * 80)
    print()
    
    tickers = get_all_tickers()
    print(f"Found {len(tickers)} tickers to process")
    print(f"Output directory: {output_dir}")
    print()
    
    successful = 0
    failed = 0
    skipped = 0
    
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Processing {ticker}...")
        
        try:
            eps_data = fetch_eps_data(ticker)
            
            if eps_data:
                save_eps_data(ticker, eps_data)
                successful += 1
            else:
                print(f"   ⚠️  No data available for {ticker}")
                skipped += 1
            
            # Rate limiting - be nice to Yahoo Finance API
            time.sleep(0.5)
            
        except Exception as e:
            print(f"   ❌ Error processing {ticker}: {str(e)}")
            failed += 1
        
        print()
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total tickers: {len(tickers)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Skipped (no data): {skipped}")
    print(f"Output directory: {output_dir}")
    print()

if __name__ == "__main__":
    main()
