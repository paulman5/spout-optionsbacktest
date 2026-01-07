#!/usr/bin/env python3
"""
TSLA 2016 Backtesting Graph - Monthly APY Version (Fixed)
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def create_monthly_apy_tsla_2016_graph():
    """Create TSLA 2016 graph with monthly APY percentages"""
    print("🔄 Creating TSLA 2016 graph with monthly APY...")
    
    # Load TSLA 2016 data
    data_path = Path("/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA")
    
    # Try monthly first, fallback to weekly
    monthly_path = data_path / "monthly" / "2016_options_pessimistic.csv"
    weekly_path = data_path / "weekly" / "2016_options_pessimistic.csv"
    
    if monthly_path.exists():
        df = pd.read_csv(monthly_path)
        print(f"   Loaded {len(df)} rows from monthly data")
    elif weekly_path.exists():
        df = pd.read_csv(weekly_path)
        print(f"   Loaded {len(df)} rows from weekly data")
    else:
        print("   ❌ No TSLA 2016 data found!")
        return
    
    # Filter for valid data
    df = df.dropna(subset=['otm_pct', 'premium_yield_pct', 'volume'])
    df = df[df['volume'] > 0]
    
    print(f"   Filtered to {len(df)} valid rows")
    
    # Convert annual APY to monthly APY
    # premium_yield_pct is annual, convert to monthly: (1 + annual_rate)^(1/12) - 1
    df['monthly_apy'] = ((1 + df['premium_yield_pct']/100) ** (1/12) - 1) * 100
    
    print(f"   Converted APY from annual to monthly percentages")
    print(f"   Sample: Annual {df['premium_yield_pct'].iloc[0]:.4f}% -> Monthly {df['monthly_apy'].iloc[0]:.4f}%")
    
    # Create figure
    fig, ax1 = plt.subplots(figsize=(12, 8))
    
    # Simple scatter plot: OTM% vs Monthly APY
    scatter = ax1.scatter(
        df['otm_pct'], 
        df['monthly_apy'], 
        c='steelblue', 
        alpha=0.6, 
        s=20,
        edgecolors='navy', 
        linewidth=0.5
    )
    
    # Labels and title
    ax1.set_xlabel('OTM% (Out of the Money Percentage)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Monthly APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('TSLA 2016 - Risk vs Return Analysis (Monthly APY)', 
                   fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3)
    
    # Add reference lines
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.7, linewidth=2, label='Break-even')
    ax1.axvline(x=0, color='green', linestyle='--', alpha=0.7, linewidth=2, label='At-the-money')
    
    # Set axis limits for better visualization
    ax1.set_xlim(df['otm_pct'].min() - 5, df['otm_pct'].max() + 5)
    ax1.set_ylim(df['monthly_apy'].min() - 1, df['monthly_apy'].max() + 1)
    
    # Format y-axis to show percentages clearly
    def format_apy(y, pos):
        return f'APY {y:.3f}%'
    
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(format_apy))
    
    # Add simple annotations
    # Find highest monthly APY point
    max_apy_idx = df['monthly_apy'].idxmax()
    max_apy = df.loc[max_apy_idx]
    
    ax1.annotate(
        f'Max APY {max_apy["monthly_apy"]:.4f}%\nOTM: {max_apy["otm_pct"]:.1f}%',
        xy=(max_apy['otm_pct'], max_apy['monthly_apy']),
        xytext=(10, 10), textcoords='offset points',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
        fontsize=9
    )
    
    # Add legend
    ax1.legend(loc='upper right')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_monthly_apy_graph.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📊 TSLA 2016 Summary:")
    print(f"   Total trades: {len(df):,}")
    print(f"   OTM% range: {df['otm_pct'].min():.1f}% to {df['otm_pct'].max():.1f}%")
    print(f"   Monthly APY range: {df['monthly_apy'].min():.4f}% to {df['monthly_apy'].max():.4f}%")
    print(f"   Average Monthly APY: {df['monthly_apy'].mean():.4f}%")
    print(f"   Average OTM%: {df['otm_pct'].mean():.1f}%")
    
    # Show the plot
    plt.show()
    
    return df

if __name__ == "__main__":
    create_monthly_apy_tsla_2016_graph()
