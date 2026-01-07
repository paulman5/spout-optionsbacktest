#!/usr/bin/env python3
"""
TSLA 2016 Backtesting Graph: OTM% vs APY and Volume
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def create_tsla_2016_graph():
    """Create TSLA 2016 graph with OTM% on X-axis"""
    print("🔄 Creating TSLA 2016 backtesting graph...")
    
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
    df = df[df['volume'] > 0]  # Remove zero volume trades
    
    print(f"   Filtered to {len(df)} valid rows")
    
    # Create figure with dual y-axes
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    # Scatter plot: OTM% vs APY (color by volume)
    scatter = ax1.scatter(
        df['otm_pct'], 
        df['premium_yield_pct'], 
        c=df['volume'], 
        cmap='viridis', 
        alpha=0.6, 
        s=30,  # Point size
        edgecolors='black', 
        linewidth=0.5
    )
    
    # Add colorbar for volume
    cbar = plt.colorbar(scatter, ax=ax1)
    cbar.set_label('Volume', rotation=270, labelpad=15)
    
    # Left y-axis (APY)
    ax1.set_xlabel('OTM% (Out of the Money Percentage)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APY (%)', fontsize=12, fontweight='bold', color='blue')
    ax1.set_title('TSLA 2016 - Risk vs Return Analysis\n(OTM% vs APY with Volume)', 
                   fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='y', labelcolor='blue')
    
    # Right y-axis for average volume per OTM% bin
    ax2 = ax1.twinx()
    
    # Create volume bins and calculate average volume per OTM% range
    otm_bins = np.arange(df['otm_pct'].min(), df['otm_pct'].max() + 5, 5)
    volume_avg = []
    otm_centers = []
    
    for i in range(len(otm_bins) - 1):
        mask = (df['otm_pct'] >= otm_bins[i]) & (df['otm_pct'] < otm_bins[i + 1])
        if mask.sum() > 0:
            volume_avg.append(df[mask]['volume'].mean())
            otm_centers.append((otm_bins[i] + otm_bins[i + 1]) / 2)
        else:
            volume_avg.append(0)
            otm_centers.append((otm_bins[i] + otm_bins[i + 1]) / 2)
    
    # Plot volume bars on right axis
    bars = ax2.bar(otm_centers, volume_avg, width=4, alpha=0.3, color='red', label='Avg Volume')
    ax2.set_ylabel('Average Volume', fontsize=12, fontweight='bold', color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    
    # Add reference lines
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5, label='Break-even')
    ax1.axvline(x=0, color='black', linestyle='--', alpha=0.5, label='At-the-money')
    
    # Add annotations for key insights
    # Find highest APY point
    max_apy_idx = df['premium_yield_pct'].idxmax()
    max_apy = df.loc[max_apy_idx]
    
    ax1.annotate(
        f'Highest APY: {max_apy["premium_yield_pct"]:.2f}%\nOTM: {max_apy["otm_pct"]:.1f}%\nVolume: {max_apy["volume"]:,}',
        xy=(max_apy['otm_pct'], max_apy['premium_yield_pct']),
        xytext=(10, 10), textcoords='offset points',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
        fontsize=9
    )
    
    # Find highest volume point
    max_vol_idx = df['volume'].idxmax()
    max_vol = df.loc[max_vol_idx]
    
    ax1.annotate(
        f'Highest Volume: {max_vol["volume"]:,}\nAPY: {max_vol["premium_yield_pct"]:.2f}%\nOTM: {max_vol["otm_pct"]:.1f}%',
        xy=(max_vol['otm_pct'], max_vol['premium_yield_pct']),
        xytext=(10, -40), textcoords='offset points',
        bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8),
        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
        fontsize=9
    )
    
    # Add legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_risk_return_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📊 TSLA 2016 Summary Statistics:")
    print(f"   Total trades: {len(df):,}")
    print(f"   OTM% range: {df['otm_pct'].min():.1f}% to {df['otm_pct'].max():.1f}%")
    print(f"   APY range: {df['premium_yield_pct'].min():.2f}% to {df['premium_yield_pct'].max():.2f}%")
    print(f"   Volume range: {df['volume'].min():,} to {df['volume'].max():,}")
    print(f"   Average APY: {df['premium_yield_pct'].mean():.2f}%")
    print(f"   Average OTM%: {df['otm_pct'].mean():.1f}%")
    
    # Show the plot
    plt.show()
    
    return df

if __name__ == "__main__":
    create_tsla_2016_graph()
