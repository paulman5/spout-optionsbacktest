#!/usr/bin/env python3
"""
TSLA Yield Curve Analysis - Correct Format
X-axis: OTM from 0% to 40%
Primary Y-axis (left): APY as yield curve line
Secondary Y-axis (right): Volume as bars
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime

def load_tsla_data():
    """Load all TSLA yearly data"""
    print("📊 Loading TSLA data for yield curve analysis...")
    
    all_data = []
    years = range(2016, 2026)  # 2016-2025
    
    for year in years:
        file_path = f"/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/{year}_options_pessimistic.csv"
        
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            print(f"   Loaded {year}: {len(df):,} contracts")
            all_data.append(df)
        else:
            print(f"   ⚠️  {year} file not found")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"   Total contracts: {len(combined_df):,}")
        return combined_df
    else:
        return None

def calculate_apy(df):
    """Calculate APY from premium yield"""
    # Convert premium_yield_pct to APY (assuming monthly to annual)
    # premium_yield_pct is already in percentage form, so we need to annualize properly
    # For options: APY = (premium/strike) * (365/days_to_expiry) * 100
    df['apy'] = ((df['premium_yield_pct'] / 100) * (365 / df['days_to_expiry']) * 100).round(2)
    
    # Cap APY at reasonable range (0-100%)
    df['apy'] = df['apy'].clip(0, 100)
    
    return df

def prepare_yield_data(df):
    """Prepare data for yield curve visualization"""
    print("📈 Preparing TSLA yield curve data...")
    
    # Filter for valid data and APY range 0-5%
    df_clean = df[
        (df['otm_pct'] >= 0) & (df['otm_pct'] <= 40) &  # OTM 0% to 40%
        (df['volume'] > 0) & (df['premium_yield_pct'] > 0) &  # Valid volume and premium
        (df['days_to_expiry'] > 0) &  # Valid expiry
        (df['apy'] <= 5)  # APY 0-5% range
    ].copy()
    
    if len(df_clean) == 0:
        print("   ❌ No valid data for yield curve")
        return None
    
    # Calculate APY
    df_clean = calculate_apy(df_clean)
    
    # Create OTM bins for aggregation (0-5%, 5-10%, etc.)
    df_clean['otm_bin'] = pd.cut(df_clean['otm_pct'], 
                                     bins=np.arange(0, 41, 5),
                                     labels=[f"{i}-{i+5}" for i in range(0, 40, 5)])
    
    print(f"   Processed {len(df_clean):,} valid contracts")
    print(f"   OTM range: {df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%")
    print(f"   APY range: {df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%")
    
    return df_clean

def create_yield_curve_plot(df_clean):
    """Create yield curve plot with dual Y-axis"""
    print("🎨 Creating TSLA yield curve visualization...")
    
    # Aggregate data by OTM bins
    agg_data = df_clean.groupby('otm_bin').agg({
        'apy': 'mean',  # Average APY for yield curve
        'volume': 'sum',  # Total volume for bars
        'otm_pct': 'mean'  # Average OTM for positioning
    }).reset_index()
    
    # Create figure and primary axis for APY
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    # Plot 1: APY as yield curve line (primary Y-axis)
    line1 = ax1.plot(range(len(agg_data)), agg_data['apy'], 
                     'o-', color='#FF6B6B', linewidth=3, markersize=8, 
                     label='APY Yield Curve', markerfacecolor='white', 
                     markeredgewidth=2, markeredgecolor='#FF6B6B')
    
    # Set up primary Y-axis (APY)
    ax1.set_xlabel('OTM Percentage (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APY (%)', fontsize=12, fontweight='bold', color='#FF6B6B')
    ax1.set_title('TSLA Options Yield Curve: APY and Volume Analysis', 
                  fontsize=14, fontweight='bold', pad=20)
    ax1.set_xticks(range(len(agg_data)))
    ax1.set_xticklabels([f"{label}%" for label in agg_data['otm_bin']], fontsize=10)
    ax1.tick_params(axis='y', labelcolor='#FF6B6B')
    ax1.grid(True, alpha=0.3)
    
    # Set Y-axis limits for APY (0-5% range)
    ax1.set_ylim(0, max(5, agg_data['apy'].max() * 1.1))
    
    # Create secondary Y-axis for Volume
    ax2 = ax1.twinx()
    
    # Plot 2: Volume as bars (secondary Y-axis)
    bars = ax2.bar(range(len(agg_data)), agg_data['volume'], 
                   alpha=0.3, color='#4ECDC4', edgecolor='#4ECDC4', 
                   linewidth=1, label='Volume')
    
    # Set up secondary Y-axis (Volume)
    ax2.set_ylabel('Volume', fontsize=12, fontweight='bold', color='#4ECDC4')
    ax2.tick_params(axis='y', labelcolor='#4ECDC4')
    
    # Add value labels on the yield curve
    for i, (x, y) in enumerate(zip(range(len(agg_data)), agg_data['apy'])):
        ax1.annotate(f'{y:.1f}%', (x, y), textcoords="offset points", 
                    xytext=(0,10), ha='center', fontsize=9, fontweight='bold', 
                    color='#FF6B6B')
    
    # Add legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    plt.tight_layout()
    
    # Save plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_yield_curve_correct.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved plot: {output_path}")
    
    plt.show()
    return output_path, agg_data

def generate_summary_statistics(df_clean, agg_data):
    """Generate summary statistics"""
    print("📊 Generating TSLA yield curve statistics...")
    
    stats = {
        'total_contracts': len(df_clean),
        'date_range': f"{df_clean['date_only'].min()} to {df_clean['date_only'].max()}",
        'otm_range': f"{df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%",
        'apy_range': f"{df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%",
        'volume_total': df_clean['volume'].sum(),
        'avg_volume': df_clean['volume'].mean(),
        'avg_underlying_price': df_clean['underlying_spot'].mean(),
        'avg_days_to_expiry': df_clean['days_to_expiry'].mean()
    }
    
    # OTM bin statistics
    print("\n📈 OTM Bin Analysis:")
    print("=" * 60)
    print(f"{'OTM Range':<12} {'Avg APY':<10} {'Total Volume':<15} {'Contracts':<10}")
    print("=" * 60)
    
    for _, row in agg_data.iterrows():
        otm_range = row['otm_bin']
        avg_apy = row['apy']
        volume = row['volume']
        
        # Count contracts in this bin
        contracts = len(df_clean[df_clean['otm_bin'] == otm_range])
        
        print(f"{otm_range:<12} {avg_apy:<10.2f}% {volume:<15,} {contracts:<10,}")
    
    print("=" * 60)
    
    print("\n📈 TSLA Yield Curve Summary:")
    print("=" * 50)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title():25}: {value}")
    print("=" * 50)
    
    return stats

def main():
    """Main analysis function"""
    print("🚀 Starting TSLA Yield Curve Analysis...")
    print("📊 Creating yield curve: APY line + Volume bars (dual Y-axis)")
    
    # Load data
    df = load_tsla_data()
    
    if df is None:
        print("❌ No data loaded")
        return 1
    
    # Prepare data
    df_clean = prepare_yield_data(df)
    
    if df_clean is None:
        return 1
    
    # Create visualization
    plot_path, agg_data = create_yield_curve_plot(df_clean)
    
    # Generate statistics
    stats = generate_summary_statistics(df_clean, agg_data)
    
    print(f"\n🎉 TSLA Yield Curve Analysis Complete!")
    print(f"📊 Summary:")
    print(f"   Total contracts analyzed: {stats['total_contracts']:,}")
    print(f"   Date range: {stats['date_range']}")
    print(f"   APY range: {stats['apy_range']}")
    print(f"   Volume analyzed: {stats['volume_total']:,}")
    print(f"   Visualization saved: {plot_path}")
    
    return 0

if __name__ == "__main__":
    exit(main())
