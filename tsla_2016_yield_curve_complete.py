#!/usr/bin/env python3
"""
TSLA 2016 Yield Curve Analysis - ALL Data Points with premium_yield_pct_low
X-axis: OTM from 0% to 40%
Y-axis: APY calculated from premium_yield_pct_low
Green dots: Non-exercised options (ITM = "NO")
Red dots: Exercised options (ITM = "YES")
Blue line: Yield curve trend (linear regression)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from datetime import datetime
from scipy import stats

def load_tsla_2016_data():
    """Load TSLA 2016 data only"""
    print("📊 Loading TSLA 2016 data for yield curve analysis...")
    
    file_path = "/Users/paulvanmierlo/spout-optionsbacktest/python-boilerplate/data/TSLA/monthly/2016_options_pessimistic.csv"
    
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        print(f"   Loaded 2016: {len(df):,} contracts")
        return df
    else:
        print(f"   ❌ 2016 file not found")
        return None

def calculate_apy_from_low(df):
    """Calculate APY from premium_yield_pct_low"""
    # premium_yield_pct_low is already annualized, so we don't need to multiply by 365/days
    # It's already in percentage form, so just use it directly
    df['apy'] = df['premium_yield_pct_low'].round(2)
    
    # Cap APY at reasonable range (0-12.5%)
    df['apy'] = df['apy'].clip(0, 12.5)
    
    return df

def prepare_yield_data(df):
    """Prepare data for yield curve visualization - SHOW ALL DATA"""
    print("📈 Preparing TSLA 2016 yield curve data...")
    
    # Calculate APY from premium_yield_pct_low first
    df = calculate_apy_from_low(df)
    
    # Filter for valid data but KEEP ALL OTM ranges and filter APY <= 12%
    df_clean = df[
        (df['volume'] > 0) & (df['premium_yield_pct_low'] > 0) &  # Valid volume and premium
        (df['days_to_expiry'] > 0) &  # Valid expiry
        (df['otm_pct'] >= 0) & (df['otm_pct'] <= 40) &  # OTM 0% to 40%
        (df['apy'] <= 12)  # APY <= 12% threshold
    ].copy()
    
    if len(df_clean) == 0:
        print("   ❌ No valid data for yield curve")
        return None
    
    print(f"   Processed {len(df_clean):,} valid contracts")
    print(f"   OTM range: {df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%")
    print(f"   APY range: {df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%")
    print(f"   Premium_yield_pct_low range: {df_clean['premium_yield_pct_low'].min():.2f}% to {df_clean['premium_yield_pct_low'].max():.2f}%")
    
    # Count ITM vs non-ITM
    itm_count = len(df_clean[df_clean['ITM'] == 'YES'])
    non_itm_count = len(df_clean[df_clean['ITM'] == 'NO'])
    print(f"   Exercised (ITM=YES): {itm_count:,}")
    print(f"   Non-exercised (ITM=NO): {non_itm_count:,}")
    
    return df_clean

def create_yield_curve_plot(df_clean):
    """Create yield curve plot with ALL individual points, trend line, and volume bars"""
    print("🎨 Creating TSLA 2016 yield curve visualization...")
    
    # Separate data by ITM status
    exercised = df_clean[df_clean['ITM'] == 'YES']  # Red dots
    non_exercised = df_clean[df_clean['ITM'] == 'NO']  # Green dots
    
    # Create figure with secondary axis for volume
    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax2 = ax1.twinx()
    
    # Plot ALL non-exercised options (green dots)
    if len(non_exercised) > 0:
        ax1.scatter(non_exercised['otm_pct'], non_exercised['apy'], 
                  alpha=0.6, s=8, c='green', edgecolors='darkgreen', 
                  linewidth=0.5, label=f'Non-exercised (ITM=NO): {len(non_exercised):,}')
    
    # Plot ALL exercised options (red dots)
    if len(exercised) > 0:
        ax1.scatter(exercised['otm_pct'], exercised['apy'], 
                  alpha=0.8, s=10, c='red', edgecolors='darkred', 
                  linewidth=0.5, label=f'Exercised (ITM=YES): {len(exercised):,}')
    
    # Calculate and plot trend line using linear regression on ALL data
    all_x = df_clean['otm_pct'].values
    all_y = df_clean['apy'].values
    
    # Remove any NaN or infinite values
    valid_mask = np.isfinite(all_x) & np.isfinite(all_y)
    x_clean = all_x[valid_mask]
    y_clean = all_y[valid_mask]
    
    if len(x_clean) > 1:
        # Calculate linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(x_clean, y_clean)
        
        # Generate trend line
        x_trend = np.linspace(0, 40, 100)
        y_trend = slope * x_trend + intercept
        
        # Plot trend line
        ax1.plot(x_trend, y_trend, 'b-', linewidth=3, alpha=0.8, 
                label=f'Yield Curve Trend (r²={r_value**2:.3f})')
    
    # Add volume bars for most significant volumes only
    # Group by OTM ranges and calculate total volume
    otm_bins = pd.cut(df_clean['otm_pct'], bins=np.arange(0, 41, 2),  # 2% bins
                     labels=[f"{i}-{i+2}%" for i in range(0, 40, 2)])
    
    volume_by_otm = df_clean.groupby(otm_bins)['volume'].sum().reset_index()
    
    # Filter for significant volumes (top 70% or minimum threshold)
    volume_threshold = volume_by_otm['volume'].quantile(0.3)  # Lower threshold to show more bars
    significant_volumes = volume_by_otm[volume_by_otm['volume'] >= volume_threshold]
    
    # Calculate bar positions (center of each OTM bin)
    bin_centers = np.arange(1, 40, 2)  # Center of 0-2%, 2-4%, etc.
    
    # Plot volume bars only for significant volumes
    significant_mask = volume_by_otm['volume'] >= volume_threshold
    significant_centers = bin_centers[significant_mask]
    significant_volumes_values = volume_by_otm[significant_mask]['volume'].values
    
    ax2.bar(significant_centers, significant_volumes_values, 
            width=1.8, alpha=0.3, color='orange', 
            label=f'Significant Volume (≥{volume_threshold:,.0f})')
    
    # Set up axes
    ax1.set_xlabel('OTM Percentage (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APY (%)', fontsize=12, fontweight='bold', color='black')
    ax2.set_ylabel('Volume', fontsize=12, fontweight='bold', color='orange')
    
    ax1.set_title('TSLA 2016 Options Yield Curve: ALL Contract Analysis with Volume (OTM 0-40%, APY from premium_yield_pct_low)', 
                  fontsize=14, fontweight='bold', pad=20)
    
    # Set axis limits
    ax1.set_xlim(0, 40)
    ax1.set_ylim(0, 12.5)
    
    # Set axis colors
    ax1.tick_params(axis='y', labelcolor='black')
    ax2.tick_params(axis='y', labelcolor='orange')
    
    # Add grid
    ax1.grid(True, alpha=0.3)
    
    # Add reference lines for APY ranges (0-2.5%, 2.5-5%, 5-7.5%, 7.5-10%, 10-12.5%)
    apy_ranges = [2.5, 5, 7.5, 10, 12.5]
    for apy_level in apy_ranges:
        ax1.axhline(y=apy_level, color='gray', linestyle='--', alpha=0.3, linewidth=0.5)
    
    # Add reference lines for OTM ranges (every 5%)
    for i in range(5, 41, 5):
        ax1.axvline(x=i, color='gray', linestyle='--', alpha=0.3, linewidth=0.5)
    
    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', framealpha=0.9)
    
    plt.tight_layout()
    
    # Save plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_2016_yield_curve_with_volume.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved plot: {output_path}")
    
    plt.show()
    return output_path

def generate_summary_statistics(df_clean):
    """Generate summary statistics for 2016"""
    print("📊 Generating TSLA 2016 yield curve statistics...")
    
    # Separate by ITM status
    exercised = df_clean[df_clean['ITM'] == 'YES']
    non_exercised = df_clean[df_clean['ITM'] == 'NO']
    
    # Create APY ranges for statistics
    df_clean['apy_range'] = pd.cut(df_clean['apy'], 
                                 bins=[0, 2.5, 5, 7.5, 10, 12.5],
                                 labels=['0-2.5%', '2.5-5%', '5-7.5%', '7.5-10%', '10-12.5%'],
                                 include_lowest=True)
    
    stats = {
        'total_contracts': len(df_clean),
        'exercised_contracts': len(exercised),
        'non_exercised_contracts': len(non_exercised),
        'date_range': f"{df_clean['date_only'].min()} to {df_clean['date_only'].max()}",
        'otm_range': f"{df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%",
        'apy_range': f"{df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%",
        'premium_yield_low_range': f"{df_clean['premium_yield_pct_low'].min():.2f}% to {df_clean['premium_yield_pct_low'].max():.2f}%",
        'volume_total': df_clean['volume'].sum(),
        'avg_underlying_price': df_clean['underlying_spot'].mean(),
        'avg_days_to_expiry': df_clean['days_to_expiry'].mean()
    }
    
    # Statistics by ITM status
    print("\n📈 Statistics by Exercise Status:")
    print("=" * 75)
    print(f"{'Status':<20} {'Contracts':<12} {'Avg APY':<10} {'Avg OTM':<10} {'Avg Premium Low':<15} {'Volume':<10}")
    print("=" * 75)
    
    if len(exercised) > 0:
        print(f"{'Exercised (ITM=YES)':<20} {len(exercised):<12,} {exercised['apy'].mean():<10.2f}% {exercised['otm_pct'].mean():<10.1f}% {exercised['premium_yield_pct_low'].mean():<15.2f}% {exercised['volume'].sum():<10,}")
    
    if len(non_exercised) > 0:
        print(f"{'Non-exercised (ITM=NO)':<20} {len(non_exercised):<12,} {non_exercised['apy'].mean():<10.2f}% {non_exercised['otm_pct'].mean():<10.1f}% {non_exercised['premium_yield_pct_low'].mean():<15.2f}% {non_exercised['volume'].sum():<10,}")
    
    print("=" * 75)
    
    # OTM distribution
    print(f"\n📊 OTM Distribution (0-40% range):")
    otm_bins = pd.cut(df_clean['otm_pct'], bins=np.arange(0, 41, 5), 
                     labels=[f"{i}-{i+5}%" for i in range(0, 40, 5)])
    otm_dist = otm_bins.value_counts().sort_index()
    
    for otm_range, count in otm_dist.items():
        percentage = (count / len(df_clean)) * 100
        print(f"   {otm_range}: {count:,} contracts ({percentage:.1f}%)")
    
    print("\n📈 TSLA 2016 Yield Curve Summary:")
    print("=" * 50)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title():25}: {value}")
    print("=" * 50)
    
    return stats

def main():
    """Main analysis function"""
    print("🚀 Starting TSLA 2016 Yield Curve Analysis...")
    print("📊 Creating ALL data points plot: OTM (0-40%) vs APY (from premium_yield_pct_low)")
    print("🔴 Red dots: Exercised options (ITM=YES)")
    print("🟢 Green dots: Non-exercised options (ITM=NO)")
    print("🔵 Blue line: Yield curve trend (linear regression)")
    print("📊 APY ranges: 0-2.5%, 2.5-5%, 5-7.5%, 7.5-10%, 10-12.5%")
    
    # Load 2016 data
    df = load_tsla_2016_data()
    
    if df is None:
        print("❌ No 2016 data loaded")
        return 1
    
    # Prepare data
    df_clean = prepare_yield_data(df)
    
    if df_clean is None:
        return 1
    
    # Create visualization
    plot_path = create_yield_curve_plot(df_clean)
    
    # Generate statistics
    stats = generate_summary_statistics(df_clean)
    
    print(f"\n🎉 TSLA 2016 Yield Curve Analysis Complete!")
    print(f"📊 Summary:")
    print(f"   Total contracts analyzed: {stats['total_contracts']:,}")
    print(f"   Exercised: {stats['exercised_contracts']:,}")
    print(f"   Non-exercised: {stats['non_exercised_contracts']:,}")
    print(f"   Date range: {stats['date_range']}")
    print(f"   APY range: {stats['apy_range']}")
    print(f"   Premium_yield_pct_low range: {stats['premium_yield_low_range']}")
    print(f"   Visualization saved: {plot_path}")
    
    return 0

if __name__ == "__main__":
    exit(main())
