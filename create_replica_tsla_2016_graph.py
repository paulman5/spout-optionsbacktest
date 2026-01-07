#!/usr/bin/env python3
"""
TSLA 2016 Graph - Exact Replica of Reference Image
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def create_replica_tsla_2016_graph():
    """Create exact replica of the TSLA 2016 reference graph"""
    print("🔄 Creating exact replica of TSLA 2016 reference graph...")
    
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
    df['monthly_apy'] = ((1 + df['premium_yield_pct']/100) ** (1/12) - 1) * 100
    
    # Create figure with clean styling like reference image
    fig, ax1 = plt.subplots(figsize=(12, 8))
    
    # Simple scatter plot: OTM% vs Monthly APY
    scatter = ax1.scatter(
        df['otm_pct'], 
        df['monthly_apy'], 
        c='blue', 
        alpha=0.6, 
        s=15,
        edgecolors='darkblue', 
        linewidth=0.5
    )
    
    # Clean labels and title
    ax1.set_xlabel('OTM% (Out of the Money Percentage)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Monthly APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('TSLA 2016 - Risk vs Return Analysis', 
                   fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    
    # Set axis limits for better visualization
    ax1.set_xlim(df['otm_pct'].min() - 5, df['otm_pct'].max() + 5)
    ax1.set_ylim(df['monthly_apy'].min() - 0.5, df['monthly_apy'].max() + 0.5)
    
    # Format y-axis to show percentages clearly
    def format_apy(y, pos):
        return f'{y:.3f}%'
    
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(format_apy))
    
    # Add volume as secondary axis (like reference image)
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
    bars = ax2.bar(otm_centers, volume_avg, width=4, alpha=0.3, color='gray', label='Volume')
    ax2.set_ylabel('Volume', fontsize=12, fontweight='bold', color='gray')
    ax2.tick_params(axis='y', labelcolor='gray')
    
    # Add simple legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_replica_graph.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📊 TSLA 2016 Summary:")
    print(f"   Total trades: {len(df):,}")
    print(f"   OTM% range: {df['otm_pct'].min():.1f}% to {df['otm_pct'].max():.1f}%")
    print(f"   Monthly APY range: {df['monthly_apy'].min():.3f}% to {df['monthly_apy'].max():.3f}%")
    print(f"   Average Monthly APY: {df['monthly_apy'].mean():.3f}%")
    print(f"   Average OTM%: {df['otm_pct'].mean():.1f}%")
    
    # Show the plot
    plt.show()
    
    return df

if __name__ == "__main__":
    create_replica_tsla_2016_graph()
