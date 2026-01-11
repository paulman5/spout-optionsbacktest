"""
Test the accuracy of ITM probability predictions for weekly options.

This script analyzes how often options with 5-6% ITM probability actually
expire in the money across all stocks over the last 3 years.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import glob
try:
    from scipy.stats import binom
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

def analyze_itm_probability_accuracy():
    """
    Analyze ITM hit rate for weekly options with 5-6% ITM probability.
    """
    data_dir = Path(__file__).parent / "data"
    
    # Last 3 years: 2022, 2023, 2024 (and 2025 if available)
    current_year = datetime.now().year
    years_to_analyze = [2022, 2023, 2024]
    if current_year >= 2025:
        years_to_analyze.append(2025)
    
    print("=" * 80)
    print("TESTING ITM PROBABILITY ACCURACY FOR WEEKLY OPTIONS")
    print("=" * 80)
    print(f"Analyzing years: {years_to_analyze}")
    print(f"Target probability range: 5-6% (0.05-0.06)")
    print()
    
    # Find all weekly CSV files
    weekly_files = []
    for year in years_to_analyze:
        pattern = str(data_dir / "*" / "weekly" / f"{year}_options_pessimistic.csv")
        files = glob.glob(pattern)
        weekly_files.extend(files)
    
    print(f"Found {len(weekly_files)} weekly option files")
    print()
    
    # Statistics
    total_options = 0
    options_in_range = 0
    options_with_expiry_data = 0
    actually_itm = 0
    
    results_by_stock = defaultdict(lambda: {'total': 0, 'itm': 0})
    results_by_year = defaultdict(lambda: {'total': 0, 'itm': 0})
    
    all_results = []
    
    # Process each file
    for file_path in sorted(weekly_files):
        try:
            # Extract symbol and year from path
            parts = Path(file_path).parts
            symbol = parts[-3]  # e.g., 'AAPL' from 'data/AAPL/weekly/2022_options_pessimistic.csv'
            year = int(parts[-1].split('_')[0])  # Extract year from filename
            
            # Load data
            df = pd.read_csv(file_path)
            
            if len(df) == 0:
                continue
            
            # Filter to calls only
            df = df[df['option_type'] == 'C'].copy()
            
            if len(df) == 0:
                continue
            
            # Check if probability_itm column exists
            if 'probability_itm' not in df.columns:
                # Skip files without probability_itm column
                continue
            
            # Filter to options with probability_itm between 5% and 6%
            # probability_itm is stored as decimal (0.05 = 5%)
            df_filtered = df[
                (df['probability_itm'] >= 0.05) & 
                (df['probability_itm'] <= 0.06)
            ].copy()
            
            total_options += len(df)
            options_in_range += len(df_filtered)
            
            if len(df_filtered) == 0:
                continue
            
            # Check which ones actually expired ITM
            # For calls: ITM if strike < underlying_spot_at_expiry
            df_filtered = df_filtered.dropna(subset=['underlying_spot_at_expiry', 'strike'])
            
            options_with_expiry_data += len(df_filtered)
            
            if len(df_filtered) == 0:
                continue
            
            # Determine if actually ITM
            df_filtered['actually_itm'] = df_filtered['strike'] < df_filtered['underlying_spot_at_expiry']
            
            itm_count = df_filtered['actually_itm'].sum()
            actually_itm += itm_count
            
            # Store results
            results_by_stock[symbol]['total'] += len(df_filtered)
            results_by_stock[symbol]['itm'] += itm_count
            
            results_by_year[year]['total'] += len(df_filtered)
            results_by_year[year]['itm'] += itm_count
            
            # Store individual results for detailed analysis
            for _, row in df_filtered.iterrows():
                all_results.append({
                    'symbol': symbol,
                    'year': year,
                    'date_only': row.get('date_only'),
                    'expiration_date': row.get('expiration_date'),
                    'strike': row['strike'],
                    'underlying_spot': row.get('underlying_spot'),
                    'underlying_spot_at_expiry': row['underlying_spot_at_expiry'],
                    'probability_itm': row['probability_itm'],
                    'actually_itm': row['actually_itm'],
                    'premium': row.get('premium'),
                    'premium_yield_pct': row.get('premium_yield_pct'),
                })
            
        except Exception as e:
            # Only print errors for unexpected issues (not missing columns)
            if 'probability_itm' not in str(e):
                print(f"Error processing {file_path}: {e}")
            continue
    
    # Print summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total weekly options analyzed: {total_options:,}")
    print(f"Options with 5-6% ITM probability: {options_in_range:,}")
    print(f"Options with expiry data: {options_with_expiry_data:,}")
    print(f"Options that actually expired ITM: {actually_itm:,}")
    
    if options_with_expiry_data > 0:
        hit_rate = (actually_itm / options_with_expiry_data) * 100
        print(f"\n🎯 ITM HIT RATE: {hit_rate:.2f}%")
        print(f"   Expected: 5-6% (average: 5.5%)")
        print(f"   Actual: {hit_rate:.2f}%")
        print(f"   Difference: {hit_rate - 5.5:.2f} percentage points")
        
        # Calculate 95% confidence interval using binomial distribution
        if HAS_SCIPY:
            n = options_with_expiry_data
            p = actually_itm / n
            # Standard error for proportion
            se = np.sqrt(p * (1 - p) / n)
            # 95% CI (z = 1.96)
            ci_lower = (p - 1.96 * se) * 100
            ci_upper = (p + 1.96 * se) * 100
            print(f"   95% Confidence Interval: [{ci_lower:.2f}%, {ci_upper:.2f}%]")
            
            # Check if expected range (5-6%) falls within confidence interval
            if 5.0 <= ci_lower and ci_upper <= 6.0:
                print(f"   ✅ Expected range (5-6%) is within confidence interval")
            elif 5.0 <= ci_upper and ci_lower <= 6.0:
                print(f"   ⚠️  Expected range (5-6%) partially overlaps with confidence interval")
            else:
                print(f"   ⚠️  Expected range (5-6%) does not overlap with confidence interval")
    else:
        print("\n⚠️  No options with expiry data found!")
    
    # Print by year
    print("\n" + "=" * 80)
    print("RESULTS BY YEAR")
    print("=" * 80)
    for year in sorted(results_by_year.keys()):
        stats = results_by_year[year]
        if stats['total'] > 0:
            hit_rate = (stats['itm'] / stats['total']) * 100
            print(f"{year}: {stats['itm']:,}/{stats['total']:,} = {hit_rate:.2f}%")
    
    # Print by stock (top 20)
    print("\n" + "=" * 80)
    print("RESULTS BY STOCK (Top 20 by count)")
    print("=" * 80)
    sorted_stocks = sorted(results_by_stock.items(), key=lambda x: x[1]['total'], reverse=True)
    for symbol, stats in sorted_stocks[:20]:
        if stats['total'] > 0:
            hit_rate = (stats['itm'] / stats['total']) * 100
            print(f"{symbol:6s}: {stats['itm']:4,}/{stats['total']:4,} = {hit_rate:5.2f}%")
    
    # Save detailed results
    if all_results:
        results_df = pd.DataFrame(all_results)
        output_file = Path(__file__).parent / "results" / "itm_probability_accuracy_5_6pct.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(output_file, index=False)
        print(f"\n💾 Detailed results saved to: {output_file}")
        print(f"   Total records: {len(results_df):,}")
    
    # Additional statistics
    if all_results:
        results_df = pd.DataFrame(all_results)
        print("\n" + "=" * 80)
        print("ADDITIONAL STATISTICS")
        print("=" * 80)
        print(f"Average probability_itm: {results_df['probability_itm'].mean():.4f} ({results_df['probability_itm'].mean()*100:.2f}%)")
        print(f"Median probability_itm: {results_df['probability_itm'].median():.4f} ({results_df['probability_itm'].median()*100:.2f}%)")
        print(f"Min probability_itm: {results_df['probability_itm'].min():.4f} ({results_df['probability_itm'].min()*100:.2f}%)")
        print(f"Max probability_itm: {results_df['probability_itm'].max():.4f} ({results_df['probability_itm'].max()*100:.2f}%)")
        
        if 'premium_yield_pct' in results_df.columns:
            results_df['premium_yield_pct'] = pd.to_numeric(results_df['premium_yield_pct'], errors='coerce')
            print(f"\nAverage premium yield: {results_df['premium_yield_pct'].mean():.2f}%")
            print(f"Median premium yield: {results_df['premium_yield_pct'].median():.2f}%")
        
        # Compare ITM vs OTM premium yields
        itm_results = results_df[results_df['actually_itm'] == True]
        otm_results = results_df[results_df['actually_itm'] == False]
        
        if len(itm_results) > 0 and 'premium_yield_pct' in itm_results.columns:
            print(f"\nPremium yield for ITM options: {itm_results['premium_yield_pct'].mean():.2f}%")
        if len(otm_results) > 0 and 'premium_yield_pct' in otm_results.columns:
            print(f"Premium yield for OTM options: {otm_results['premium_yield_pct'].mean():.2f}%")
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    analyze_itm_probability_accuracy()

