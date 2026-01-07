#!/usr/bin/env python3
"""
TSLA Yield Curve Analysis - Single Graph with Dual Y-Axis
X-axis: OTM from 0% to 40%
Primary Y-axis: APY ranges (0-1%, 1-2%, 2-3%, 3-4%, 4-5%)
Secondary Y-axis: Volume bars
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
    df['apy'] = (df['premium_yield_pct'] * (365 / df['days_to_expiry'])).round(2)
    return df

def prepare_yield_data(df):
    """Prepare data for yield curve visualization"""
    print("📈 Preparing TSLA yield curve data...")
    
    # Filter for valid data
    df_clean = df[
        (df['otm_pct'] >= 0) & (df['otm_pct'] <= 40) &  # OTM 0% to 40%
        (df['volume'] > 0) & (df['premium_yield_pct'] > 0) &  # Valid volume and premium
        (df['days_to_expiry'] > 0)  # Valid expiry
    ].copy()
    
    if len(df_clean) == 0:
        print("   ❌ No valid data for yield curve")
        return None
    
    # Calculate APY
    df_clean = calculate_apy(df_clean)
    
    # Create OTM bins (0-5%, 5-10%, etc.)
    df_clean['otm_bin'] = pd.cut(df_clean['otm_pct'], 
                                     bins=np.arange(0, 41, 5),
                                     labels=[f"{i}-{i+5}" for i in range(0, 40, 5)])
    
    # Create APY bins (0-1%, 1-2%, 2-3%, 3-4%, 4-5%)
    df_clean['apy_bin'] = pd.cut(df_clean['apy'], 
                                   bins=[0, 1, 2, 3, 4, 5],
                                   labels=['0-1%', '1-2%', '2-3%', '3-4%', '4-5%'],
                                   include_lowest=True)
    
    print(f"   Processed {len(df_clean):,} valid contracts")
    print(f"   OTM range: {df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%")
    print(f"   APY range: {df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%")
    
    return df_clean

def create_yield_curve_plot(df_clean):
    """Create single yield curve plot with dual Y-axis"""
    print("🎨 Creating TSLA yield curve visualization...")
    
    # Create figure and primary axis
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    # Aggregate data by OTM bins and APY bins
    pivot_data = df_clean.groupby(['otm_bin', 'apy_bin']).agg({
        'volume': 'sum'
    }).reset_index()
    
    # Create pivot table for heatmap-style visualization
    pivot_matrix = pivot_data.pivot(index='otm_bin', columns='apy_bin', values='volume').fillna(0)
    
    # Ensure all APY bins are present
    apy_bins = ['0-1%', '1-2%', '2-3%', '3-4%', '4-5%']
    for col in apy_bins:
        if col not in pivot_matrix.columns:
            pivot_matrix[col] = 0
    pivot_matrix = pivot_matrix[apy_bins]
    
    # Create stacked bar chart for volume by APY ranges
    bottom = np.zeros(len(pivot_matrix))
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    for i, apy_range in enumerate(apy_bins):
        values = pivot_matrix[apy_range].values
        ax1.bar(range(len(pivot_matrix)), values, bottom=bottom, 
                label=apy_range, color=colors[i], alpha=0.8, edgecolor='black', linewidth=0.5)
        bottom += values
    
    # Set up primary Y-axis (Volume)
    ax1.set_xlabel('OTM Percentage (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Volume', fontsize=12, fontweight='bold', color='black')
    ax1.set_title('TSLA Options Yield Curve: Volume Distribution by APY Ranges', 
                  fontsize=14, fontweight='bold', pad=20)
    ax1.set_xticks(range(len(pivot_matrix)))
    ax1.set_xticklabels([f"{label}%" for label in pivot_matrix.index], fontsize=10)
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.legend(title='APY Ranges', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Create secondary Y-axis for APY reference
    ax2 = ax1.twinx()
    ax2.set_ylabel('APY (%)', fontsize=12, fontweight='bold', color='darkblue')
    ax2.set_ylim(0, 5)
    ax2.set_yticks([0, 1, 2, 3, 4, 5])
    ax2.set_yticklabels(['0%', '1%', '2%', '3%', '4%', '5%'], fontsize=10)
    
    # Add reference lines for APY ranges
    for i in range(1, 6):
        ax2.axhline(y=i, color='darkblue', linestyle='--', alpha=0.3, linewidth=0.5)
    
    plt.tight_layout()
    
    # Save plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_yield_curve_dual_axis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved plot: {output_path}")
    
    plt.show()
    return output_path

def generate_summary_statistics(df_clean):
    """Generate summary statistics for the analysis"""
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
    
    # APY distribution
    apy_dist = df_clean['apy_bin'].value_counts().sort_index()
    print("\n📈 APY Distribution:")
    for apy_range, count in apy_dist.items():
        percentage = (count / len(df_clean)) * 100
        print(f"   {apy_range}: {count:,} contracts ({percentage:.1f}%)")
    
    # OTM distribution
    otm_dist = df_clean['otm_bin'].value_counts().sort_index()
    print("\n📊 OTM Distribution:")
    for otm_range, count in otm_dist.items():
        percentage = (count / len(df_clean)) * 100
        print(f"   {otm_range}%: {count:,} contracts ({percentage:.1f}%)")
    
    print("\n📈 TSLA Yield Curve Summary:")
    print("=" * 50)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title():25}: {value}")
    print("=" * 50)
    
    return stats

def main():
    """Main analysis function"""
    print("🚀 Starting TSLA Yield Curve Analysis...")
    print("📊 Creating dual-axis plot: OTM (0-40%) with APY ranges and Volume")
    
    # Load data
    df = load_tsla_data()
    
    if df is None:
        print("❌ No data loaded")
        return 1
    
    # Prepare data
    df_clean = prepare_yield_data(df)
    
    if df_clean is None:
        return 1
    
    # Generate statistics
    stats = generate_summary_statistics(df_clean)
    
    # Create visualization
    plot_path = create_yield_curve_plot(df_clean)
    
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
