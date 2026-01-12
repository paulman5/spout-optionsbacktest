"""
Create dedicated volume analysis visualizations for backtest results.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse


def plot_volume_analysis(
    data_file: str,
    output_file: str = None
):
    """
    Create comprehensive volume analysis plots.
    
    Args:
        data_file: Path to consolidated backtest results CSV
        output_file: Path to save the plot (optional)
    """
    df = pd.read_csv(data_file)
    
    print("=" * 80)
    print("GENERATING VOLUME ANALYSIS VISUALIZATION")
    print("=" * 80)
    print(f"  Data file: {data_file}")
    print(f"  Total trades: {len(df):,}")
    print(f"  Total volume: {df['volume'].sum():,} contracts")
    
    # Calculate statistics by OTM range
    summary = df.groupby('otm_range').agg({
        'otm_pct': ['mean', 'count'],
        'volume': ['sum', 'mean', 'std', 'min', 'max'],
        'premium_yield_close': 'mean',
        'assigned': 'mean'
    }).round(2)
    
    # Flatten column names
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    summary = summary.reset_index()
    
    # Extract OTM midpoint for x-axis
    otm_extract = summary['otm_range'].str.extract(r'(\d+\.?\d*)-(\d+\.?\d*)%')
    summary['otm_midpoint'] = (
        otm_extract.iloc[:, 0].astype(float) + 
        otm_extract.iloc[:, 1].astype(float)
    ) / 2
    
    # Sort by OTM midpoint
    summary = summary.sort_values('otm_midpoint')
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Covered Call Backtest - Volume Analysis', fontsize=16, fontweight='bold')
    
    x_pos = np.arange(len(summary))
    
    # Plot 1: Total Volume by OTM Range
    ax1 = axes[0, 0]
    bars1 = ax1.bar(x_pos, summary['volume_sum'], color='#2E86AB', alpha=0.8, edgecolor='black', linewidth=1.5)
    ax1.set_xlabel('OTM Range', fontsize=12)
    ax1.set_ylabel('Total Volume (Contracts)', fontsize=12)
    ax1.set_title('Total Volume by OTM Range', fontsize=13, fontweight='bold')
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(summary['otm_range'], rotation=45, ha='right')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, vol in zip(bars1, summary['volume_sum']):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(vol):,}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Plot 2: Average Volume per Trade
    ax2 = axes[0, 1]
    bars2 = ax2.bar(x_pos, summary['volume_mean'], color='#A23B72', alpha=0.8, edgecolor='black', linewidth=1.5)
    ax2.set_xlabel('OTM Range', fontsize=12)
    ax2.set_ylabel('Average Volume per Trade (Contracts)', fontsize=12)
    ax2.set_title('Average Volume per Trade by OTM Range', fontsize=13, fontweight='bold')
    ax2.set_xticks(x_pos)
    ax2.set_xticklabels(summary['otm_range'], rotation=45, ha='right')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, vol in zip(bars2, summary['volume_mean']):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(vol):.0f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Plot 3: Volume vs Premium Yield (Scatter with size)
    ax3 = axes[1, 0]
    scatter = ax3.scatter(summary['otm_midpoint'], summary['premium_yield_close_mean'],
                         s=summary['volume_sum']/10,  # Size proportional to total volume
                         alpha=0.6, c=summary['volume_mean'], cmap='viridis',
                         edgecolors='black', linewidth=1.5)
    ax3.set_xlabel('OTM Percentage (%)', fontsize=12)
    ax3.set_ylabel('Average Premium Yield (%)', fontsize=12)
    ax3.set_title('Volume vs Premium Yield (Bubble Size = Total Volume)', fontsize=13, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    
    # Add colorbar
    cbar = plt.colorbar(scatter, ax=ax3)
    cbar.set_label('Avg Volume per Trade', fontsize=10)
    
    # Add OTM range labels
    for idx, row in summary.iterrows():
        if pd.notna(row['otm_midpoint']) and pd.notna(row['premium_yield_close_mean']):
            ax3.annotate(row['otm_range'], 
                        xy=(row['otm_midpoint'], row['premium_yield_close_mean']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.8)
    
    # Plot 4: Volume Distribution (Box plot style)
    ax4 = axes[1, 1]
    
    # Prepare data for box plot
    volume_data = []
    labels = []
    for idx, row in summary.iterrows():
        otm_range = row['otm_range']
        volumes = df[df['otm_range'] == otm_range]['volume'].values
        volume_data.append(volumes)
        labels.append(otm_range)
    
    bp = ax4.boxplot(volume_data, tick_labels=labels, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('#F18F01')
        patch.set_alpha(0.7)
    
    ax4.set_xlabel('OTM Range', fontsize=12)
    ax4.set_ylabel('Volume (Contracts)', fontsize=12)
    ax4.set_title('Volume Distribution by OTM Range', fontsize=13, fontweight='bold')
    ax4.tick_params(axis='x', rotation=45)
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # Save or show
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n✅ Saved volume analysis plot to: {output_file}")
    else:
        plt.show()
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("VOLUME STATISTICS BY OTM RANGE:")
    print("=" * 80)
    print(summary[['otm_range', 'otm_pct_count', 'volume_sum', 'volume_mean', 
                   'volume_min', 'volume_max', 'premium_yield_close_mean']].to_string(index=False))
    
    return fig, summary


def main():
    parser = argparse.ArgumentParser(description="Visualize volume metrics from backtest results")
    parser.add_argument("--data-file", 
                       default="data/TSLA/monthly/backtest_results/backtest_results_consolidated.csv",
                       help="Path to consolidated backtest results CSV")
    parser.add_argument("--output-dir", 
                       default="data/TSLA/monthly/backtest_results/",
                       help="Directory to save plots")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate volume analysis plots
    fig, summary = plot_volume_analysis(
        data_file=args.data_file,
        output_file=str(output_dir / "volume_analysis.png")
    )
    
    print(f"\n✅ Volume analysis visualization saved to: {output_dir}")


if __name__ == "__main__":
    main()











