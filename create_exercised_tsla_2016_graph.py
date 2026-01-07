#!/usr/bin/env python3
"""
TSLA 2016 Backtesting Graph - Monthly APY for Exercised ITM Calls
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def create_exercised_tsla_2016_graph():
    """Create TSLA 2016 graph for exercised ITM calls only with monthly APY"""
    print("🔄 Creating TSLA 2016 graph for exercised ITM calls...")
    
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
    
    # Filter for exercised ITM calls only
    df = df.dropna(subset=['otm_pct', 'premium_yield_pct', 'volume'])
    df = df[df['volume'] > 0]
    df = df[df['otm_pct'] > 0]  # Only ITM calls (positive OTM%)
    
    # Filter for exercised options (those that were likely exercised)
    # Exercised options typically have higher volume and are deeper ITM
    # Let's use a threshold: deeper ITM (higher OTM%) and reasonable volume
    otm_threshold = df['otm_pct'].quantile(0.5)  # Median OTM%
    volume_threshold = df['volume'].quantile(0.25)  # 25th percentile volume
    
    df_exercised = df[
        (df['otm_pct'] >= otm_threshold) & 
        (df['volume'] >= volume_threshold)
    ]
    
    print(f"   Filtered to {len(df_exercised)} exercised ITM call rows")
    print(f"   OTM threshold: {otm_threshold:.1f}%")
    print(f"   Volume threshold: {volume_threshold:,.0f}")
    
    # Convert annual APY to monthly APY
    df_exercised['monthly_apy'] = ((1 + df_exercised['premium_yield_pct']/100) ** (1/12) - 1) * 100
    
    print(f"   Converted APY from annual to monthly percentages")
    if len(df_exercised) > 0:
        print(f"   Sample: Annual {df_exercised['premium_yield_pct'].iloc[0]:.4f}% -> Monthly {df_exercised['monthly_apy'].iloc[0]:.4f}%")
    
    # Create figure
    fig, ax1 = plt.subplots(figsize=(12, 8))
    
    # Simple scatter plot: OTM% vs Monthly APY
    scatter = ax1.scatter(
        df_exercised['otm_pct'], 
        df_exercised['monthly_apy'], 
        c='steelblue', 
        alpha=0.6, 
        s=20,
        edgecolors='navy', 
        linewidth=0.5
    )
    
    # Labels and title
    ax1.set_xlabel('OTM% (In the Money Percentage)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Monthly APY (%)', fontsize=12, fontweight='bold')
    ax1.set_title('TSLA 2016 - Exercised ITM Calls Risk vs Return Analysis', 
                   fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3)
    
    # Set axis limits for better visualization
    if len(df_exercised) > 0:
        ax1.set_xlim(df_exercised['otm_pct'].min() - 1, df_exercised['otm_pct'].max() + 1)
        ax1.set_ylim(df_exercised['monthly_apy'].min() - 0.5, df_exercised['monthly_apy'].max() + 0.5)
    
    # Format y-axis to show percentages clearly
    def format_apy(y, pos):
        return f'APY {y:.3f}%'
    
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(format_apy))
    
    # Add simple annotations
    if len(df_exercised) > 0:
        # Find highest monthly APY point
        max_apy_idx = df_exercised['monthly_apy'].idxmax()
        max_apy = df_exercised.loc[max_apy_idx]
        
        ax1.annotate(
            f'Max APY {max_apy["monthly_apy"]:.4f}%\nOTM: {max_apy["otm_pct"]:.1f}%',
            xy=(max_apy['otm_pct'], max_apy['monthly_apy']),
            xytext=(10, 10), textcoords='offset points',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.8),
            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
            fontsize=9
        )
        
        # Add volume information as secondary axis
        ax2 = ax1.twinx()
        
        # Create volume bins and calculate average volume per OTM% range
        otm_bins = np.arange(df_exercised['otm_pct'].min(), df_exercised['otm_pct'].max() + 2, 2)
        volume_avg = []
        otm_centers = []
        
        for i in range(len(otm_bins) - 1):
            mask = (df_exercised['otm_pct'] >= otm_bins[i]) & (df_exercised['otm_pct'] < otm_bins[i + 1])
            if mask.sum() > 0:
                volume_avg.append(df_exercised[mask]['volume'].mean())
                otm_centers.append((otm_bins[i] + otm_bins[i + 1]) / 2)
            else:
                volume_avg.append(0)
                otm_centers.append((otm_bins[i] + otm_bins[i + 1]) / 2)
        
        # Plot volume bars on right axis
        bars = ax2.bar(otm_centers, volume_avg, width=1.5, alpha=0.3, color='red', label='Avg Volume')
        ax2.set_ylabel('Average Volume', fontsize=12, fontweight='bold', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        
        # Add legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_exercised_graph.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Graph saved to: {output_path}")
    
    # Print summary statistics
    print(f"\n📊 TSLA 2016 Exercised ITM Calls Summary:")
    print(f"   Total exercised trades: {len(df_exercised):,}")
    if len(df_exercised) > 0:
        print(f"   OTM% range: {df_exercised['otm_pct'].min():.1f}% to {df_exercised['otm_pct'].max():.1f}%")
        print(f"   Monthly APY range: {df_exercised['monthly_apy'].min():.4f}% to {df_exercised['monthly_apy'].max():.4f}%")
        print(f"   Average Monthly APY: {df_exercised['monthly_apy'].mean():.4f}%")
        print(f"   Average OTM%: {df_exercised['otm_pct'].mean():.1f}%")
    
    # Show the plot
    plt.show()
    
    return df_exercised

if __name__ == "__main__":
    create_exercised_tsla_2016_graph()
