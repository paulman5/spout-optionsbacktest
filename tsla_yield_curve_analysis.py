#!/usr/bin/env python3
"""
TSLA Statistical Analysis with Yield Curve Visualization
X-axis: OTM from 0% to 40%
Y-axis: APY (Annual Percentage Yield)
Second Y-axis: Volume
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import matplotlib.dates as mdates

def load_tsla_data():
    """Load all TSLA yearly data"""
    print("📊 Loading TSLA data for statistical analysis...")
    
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
    # For options, we need to consider days to expiry
    df['apy'] = (df['premium_yield_pct'] * (365 / df['days_to_expiry'])).round(2)
    return df

def create_yield_curve(df):
    """Create yield curve visualization"""
    print("📈 Creating TSLA yield curve...")
    
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
    
    # Create OTM bins for analysis
    df_clean['otm_bin'] = pd.cut(df_clean['otm_pct'], 
                                     bins=np.arange(0, 41, 5),  # 0-5%, 5-10%, etc.
                                     labels=[f"{i}-{i+5}%" for i in range(0, 40, 5)])
    
    # Aggregate by OTM bins
    agg_data = df_clean.groupby('otm_bin').agg({
        'apy': 'mean',
        'volume': 'sum',
        'premium_yield_pct': 'mean',
        'otm_pct': 'mean',
        'strike': 'mean',
        'underlying_spot': 'mean'
    }).reset_index()
    
    print(f"   Processed {len(df_clean):,} valid contracts")
    print(f"   OTM range: {df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%")
    print(f"   APY range: {df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%")
    
    return df_clean, agg_data

def plot_yield_curve(df_clean, agg_data):
    """Create yield curve plot"""
    print("🎨 Generating yield curve visualization...")
    
    # Set style
    plt.style.use('default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Color palette
    colors = sns.color_palette("husl", 8)
    
    # Plot 1: APY vs OTM (scatter with trend line)
    scatter = ax1.scatter(df_clean['otm_pct'], df_clean['apy'], 
                       alpha=0.6, s=20, c=colors[0], edgecolors='black', linewidth=0.5)
    
    # Add trend line
    z = np.polyfit(df_clean['otm_pct'], df_clean['apy'], 1)
    p = np.poly1d(z)
    ax1.plot(df_clean['otm_pct'], p(df_clean['otm_pct']), 
            colors[1], linewidth=2, alpha=0.8, label='Trend Line')
    
    ax1.set_xlabel('OTM Percentage (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('APY (%)', fontsize=12, fontweight='bold', color=colors[0])
    ax1.set_title('TSLA Options: APY vs OTM', fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlim(0, 40)
    ax1.legend()
    
    # Plot 2: Volume vs OTM (bar chart)
    # Use binned data for cleaner visualization
    ax2.bar(range(len(agg_data)), agg_data['volume'], 
             color=colors[2], alpha=0.7, edgecolor='black')
    
    ax2.set_xlabel('OTM Bins (%)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Total Volume', fontsize=12, fontweight='bold', color=colors[2])
    ax2.set_title('TSLA Options: Volume Distribution by OTM', fontsize=14, fontweight='bold', pad=20)
    ax2.set_xticks(range(len(agg_data)))
    ax2.set_xticklabels([label.replace('%', '') for label in agg_data['otm_bin']], rotation=45)
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Main title
    fig.suptitle('TSLA Options Statistical Analysis & Yield Curves', 
                  fontsize=16, fontweight='bold', y=0.98)
    
    plt.tight_layout()
    
    # Save plot
    output_path = "/Users/paulvanmierlo/spout-optionsbacktest/tsla_yield_curve_analysis.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved plot: {output_path}")
    
    plt.show()
    return output_path

def generate_statistics(df):
    """Generate comprehensive TSLA statistics"""
    print("📊 Generating TSLA statistical summary...")
    
    # Filter valid data
    df_clean = df[
        (df['volume'] > 0) & 
        (df['premium_yield_pct'] > 0) & 
        (df['days_to_expiry'] > 0)
    ].copy()
    
    if len(df_clean) == 0:
        return None
    
    df_clean = calculate_apy(df_clean)
    
    stats = {
        'total_contracts': len(df_clean),
        'date_range': f"{df_clean['date_only'].min()} to {df_clean['date_only'].max()}",
        'strike_range': f"${df_clean['strike'].min():.2f} to ${df_clean['strike'].max():.2f}",
        'otm_range': f"{df_clean['otm_pct'].min():.1f}% to {df_clean['otm_pct'].max():.1f}%",
        'premium_yield_range': f"{df_clean['premium_yield_pct'].min():.2f}% to {df_clean['premium_yield_pct'].max():.2f}%",
        'apy_range': f"{df_clean['apy'].min():.1f}% to {df_clean['apy'].max():.1f}%",
        'volume_total': df_clean['volume'].sum(),
        'avg_volume': df_clean['volume'].mean(),
        'itm_contracts': len(df_clean[df_clean['ITM'] == 'YES']),
        'otm_contracts': len(df_clean[df_clean['ITM'] == 'NO']),
        'avg_days_to_expiry': df_clean['days_to_expiry'].mean(),
        'avg_underlying_price': df_clean['underlying_spot'].mean()
    }
    
    # Print statistics
    print("\n📈 TSLA Options Statistical Summary:")
    print("=" * 50)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').title():25}: {value}")
    print("=" * 50)
    
    return stats

def main():
    """Main analysis function"""
    print("🚀 Starting TSLA Statistical Analysis...")
    print("📊 Creating yield curves with OTM (0-40%) and APY analysis")
    
    # Load data
    df = load_tsla_data()
    
    if df is None:
        print("❌ No data loaded")
        return 1
    
    # Create yield curve
    df_clean, agg_data = create_yield_curve(df)
    
    if df_clean is None:
        return 1
    
    # Generate statistics
    stats = generate_statistics(df)
    
    # Create visualization
    plot_path = plot_yield_curve(df_clean, agg_data)
    
    print(f"\n🎉 TSLA Analysis Complete!")
    print(f"📊 Summary:")
    print(f"   Total contracts analyzed: {stats['total_contracts']:,}")
    print(f"   Date range: {stats['date_range']}")
    print(f"   APY range: {stats['apy_range']}")
    print(f"   Volume analyzed: {stats['volume_total']:,}")
    print(f"   Visualization saved: {plot_path}")
    
    return 0

if __name__ == "__main__":
    exit(main())
