#!/usr/bin/env python3
"""
TSLA 2016 Backtesting Graph - Clean and Simple Version
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def create_clean_tsla_2016_graph():
    """Create clean TSLA 2016 graph like the reference image"""
    print("🔄 Creating clean TSLA 2016 backtesting graph...")
    
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
    
    # Create figure
    fig, ax1 = plt.subplots(figsize=(12, 8))
    
    # Simple scatter plot: OTM% vs APY
    scatter = ax1.scatter(
        df['otm_pct'], 
        df['premium_yield_pct'], 
        c='steelblue', 
        alpha=0.6, 
        s=20,
        edgecolors='navy', 
        linewidth=0.5
    )
    
    # Labels and title
    ax1.set_xlabel('OTM% (Out of the Money Percentage)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('TSLA 2016 - Risk vs Return Analysis', 
                   fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3)
    
    # Add reference lines
    ax1.axhline(y=0, color='red', linestyle='--', alpha=0.7, linewidth=2, label='Break-even')
    ax1.axvline(x=0, color='green', linestyle='--', alpha=0.7, linewidth=2, label='At-the-money')
    
    # Set axis limits for better visualization
    ax1.set_xlim(df['otm_pct'].min() - 5, df['otm_pct'].max() + 5)
    ax1.set_ylim(df['premium_yield_pct'].min() - 5, df['premium_yield_pct'].max() + 5)
    
    # Add simple annotations
    # Find highest APY point
    max_apy_idx = df['premium_yield_pct'].idxmax()
    max_apy = df.loc[max_apy_idx]
    
    ax1.annotate(
        f'Max APY: {max_apy["premium_yield_pct"]:.1f}%\nOTM: {max_apy["otm_pct"]:.1f}%',
        xy=(max_apy['otm_pct'], max_apy['premium_yield_pct']),
        xytext=(10, 10), textcoords='offset points',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
        fontsize=9
    )
    
    # Add legend
    ax1.legend(loc='upper right')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_clean_graph.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📊 TSLA 2016 Summary:")
    print(f"   Total trades: {len(df):,}")
    print(f"   OTM% range: {df['otm_pct'].min():.1f}% to {df['otm_pct'].max():.1f}%")
    print(f"   APY range: {df['premium_yield_pct'].min():.2f}% to {df['premium_yield_pct'].max():.2f}%")
    print(f"   Average APY: {df['premium_yield_pct'].mean():.2f}%")
    print(f"   Average OTM%: {df['otm_pct'].mean():.1f}%")
    
    # Show the plot
    plt.show()
    
    return df

if __name__ == "__main__":
    create_clean_tsla_2016_graph()
