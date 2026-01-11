"""
Plot ITM probability accuracy timeline similar to weekly_apy_monthly_timeline_3years.

Shows monthly hit rates for options with 5-6% ITM probability over the last 3 years.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from datetime import datetime
import matplotlib.dates as mdates

def plot_itm_probability_timeline():
    """
    Create timeline graph showing ITM hit rates by month for weekly options
    with 5-6% ITM probability.
    """
    # Load the results
    results_file = Path(__file__).parent / "results" / "itm_probability_accuracy_5_6pct.csv"
    
    if not results_file.exists():
        print(f"Error: Results file not found: {results_file}")
        print("Please run test_itm_probability_accuracy.py first.")
        return
    
    print("=" * 80)
    print("GENERATING ITM PROBABILITY TIMELINE GRAPH")
    print("=" * 80)
    
    df = pd.read_csv(results_file)
    print(f"  Loaded {len(df):,} records")
    
    # Convert date columns
    df['date_only'] = pd.to_datetime(df['date_only'])
    df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    # Create year-month column for grouping
    df['year_month'] = df['date_only'].dt.to_period('M')
    df['year_month_date'] = df['date_only'].dt.to_period('M').dt.to_timestamp()
    
    # Calculate monthly statistics
    monthly_stats = df.groupby('year_month_date').agg({
        'actually_itm': ['sum', 'count'],
        'probability_itm': 'mean',
        'premium_yield_pct': 'mean'
    }).reset_index()
    
    # Flatten column names
    monthly_stats.columns = ['year_month', 'itm_count', 'total_count', 'avg_probability', 'avg_premium_yield']
    
    # Calculate hit rate
    monthly_stats['hit_rate'] = (monthly_stats['itm_count'] / monthly_stats['total_count']) * 100
    
    # Filter to last 3 years (2022, 2023, 2024, 2025)
    # Since we're in 2025, last 3 years would be 2022-2024, but include 2025 if available
    current_year = datetime.now().year
    if current_year >= 2025:
        # Include 2022, 2023, 2024, and 2025
        monthly_stats = monthly_stats[
            (monthly_stats['year_month'].dt.year >= 2022) & 
            (monthly_stats['year_month'].dt.year <= 2025)
        ]
    else:
        # Fallback for other years
        three_years_ago = datetime.now().replace(year=datetime.now().year - 3)
        monthly_stats = monthly_stats[monthly_stats['year_month'] >= three_years_ago]
    
    print(f"  Monthly periods: {len(monthly_stats)}")
    print(f"  Date range: {monthly_stats['year_month'].min()} to {monthly_stats['year_month'].max()}")
    
    # Create the plot
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle('ITM Probability Accuracy: Weekly Options (5-6% ITM Probability)\nLast 3 Years', 
                 fontsize=16, fontweight='bold', y=0.995)
    
    # Plot 1: Hit Rate Timeline
    ax1 = axes[0]
    
    # Plot bars for hit rate
    bars = ax1.bar(monthly_stats['year_month'], monthly_stats['hit_rate'], 
                   width=20, alpha=0.7, color='#2E86AB', edgecolor='black', linewidth=1)
    
    # Add expected range lines
    ax1.axhline(y=5.0, color='green', linestyle='--', linewidth=2, alpha=0.7, label='Expected: 5%')
    ax1.axhline(y=6.0, color='green', linestyle='--', linewidth=2, alpha=0.7, label='Expected: 6%')
    ax1.axhline(y=5.5, color='green', linestyle='-', linewidth=1.5, alpha=0.5, label='Expected: 5.5% (avg)')
    
    # Calculate and plot overall average
    overall_avg = (monthly_stats['itm_count'].sum() / monthly_stats['total_count'].sum()) * 100
    ax1.axhline(y=overall_avg, color='red', linestyle='-', linewidth=2, alpha=0.8, 
                label=f'Actual Average: {overall_avg:.2f}%')
    
    ax1.set_ylabel('ITM Hit Rate (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Monthly ITM Hit Rate vs Expected (5-6%)', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.legend(loc='upper left', fontsize=10)
    # Set y-axis limit to show full range, but cap at reasonable max
    max_hit_rate = monthly_stats['hit_rate'].max()
    y_max = max(max_hit_rate * 1.15, 15)  # Show up to 15% or 15% above max
    ax1.set_ylim(0, y_max)
    
    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Add count annotations on bars
    for i, (bar, row) in enumerate(zip(bars, monthly_stats.itertuples())):
        if row.total_count > 0:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'n={int(row.total_count)}',
                    ha='center', va='bottom', fontsize=8, alpha=0.8)
    
    # Plot 2: Count of Options by Month
    ax2 = axes[1]
    
    bars2 = ax2.bar(monthly_stats['year_month'], monthly_stats['total_count'],
                   width=20, alpha=0.7, color='#A23B72', edgecolor='black', linewidth=1)
    
    ax2.set_ylabel('Number of Options', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Month', fontsize=12, fontweight='bold')
    ax2.set_title('Number of Weekly Options with 5-6% ITM Probability by Month', 
                 fontsize=13, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Format x-axis
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Add value labels on bars
    for bar, count in zip(bars2, monthly_stats['total_count']):
        if count > 0:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(count)}',
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = Path(__file__).parent / "results" / "itm_probability_timeline_3years.png"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n💾 Graph saved to: {output_file}")
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Overall hit rate: {overall_avg:.2f}%")
    print(f"Expected range: 5-6% (avg: 5.5%)")
    print(f"Difference: {overall_avg - 5.5:.2f} percentage points")
    print(f"\nMonthly hit rate statistics:")
    print(f"  Mean: {monthly_stats['hit_rate'].mean():.2f}%")
    print(f"  Median: {monthly_stats['hit_rate'].median():.2f}%")
    print(f"  Min: {monthly_stats['hit_rate'].min():.2f}%")
    print(f"  Max: {monthly_stats['hit_rate'].max():.2f}%")
    print(f"  Std Dev: {monthly_stats['hit_rate'].std():.2f}%")
    
    print("\n" + "=" * 80)
    print("GRAPH GENERATION COMPLETE")
    print("=" * 80)
    
    plt.show()

if __name__ == "__main__":
    plot_itm_probability_timeline()

