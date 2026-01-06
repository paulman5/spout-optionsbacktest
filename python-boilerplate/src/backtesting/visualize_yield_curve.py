"""
Visualize covered call backtest results as a yield curve.

Creates graphs showing premium yield across OTM percentage ranges.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse


def plot_yield_curve(
    data_file: str,
    output_file: str = None,
    show_volume: bool = True
):
    """
    Plot yield curve from backtest results.
    
    Args:
        data_file: Path to consolidated backtest results CSV
        output_file: Path to save the plot (optional)
        show_volume: Whether to show volume data
    """
    # Load data
    df = pd.read_csv(data_file)
    
    print("=" * 80)
    print("GENERATING YIELD CURVE VISUALIZATION")
    print("=" * 80)
    print(f"  Data file: {data_file}")
    print(f"  Total trades: {len(df):,}")
    
    # Calculate statistics by OTM range
    summary = df.groupby('otm_range').agg({
        'otm_pct': ['mean', 'min', 'max', 'count'],
        'premium_yield_close': ['mean', 'std', 'min', 'max'],
        'premium_yield_low': ['mean', 'std', 'min', 'max'],
        'pnl_per_contract': ['mean', 'sum'],
        'assigned': 'mean',
        'volume': 'sum'
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
    
    # Create figure with subplots (3x2 if showing volume, 2x2 otherwise)
    if show_volume:
        fig, axes = plt.subplots(3, 2, figsize=(16, 16))
    else:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Covered Call Backtest - Yield Curve Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Premium Yield Curve (Close Price)
    ax1 = axes[0, 0]
    ax1.plot(summary['otm_midpoint'], summary['premium_yield_close_mean'], 
             marker='o', linewidth=2, markersize=8, label='Average Yield', color='#2E86AB')
    ax1.fill_between(summary['otm_midpoint'], 
                     summary['premium_yield_close_mean'] - summary['premium_yield_close_std'],
                     summary['premium_yield_close_mean'] + summary['premium_yield_close_std'],
                     alpha=0.2, color='#2E86AB', label='±1 Std Dev')
    ax1.set_xlabel('OTM Percentage (%)', fontsize=11)
    ax1.set_ylabel('Premium Yield (%)', fontsize=11)
    ax1.set_title('Premium Yield Curve (Close Price)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend()
    ax1.set_xticks(summary['otm_midpoint'])
    ax1.set_xticklabels([f"{x:.0f}%" for x in summary['otm_midpoint']])
    
    # Add trade count annotations
    for idx, row in summary.iterrows():
        if pd.notna(row['otm_midpoint']) and pd.notna(row['premium_yield_close_mean']):
            ax1.annotate(f"n={int(row['otm_pct_count'])}", 
                        xy=(row['otm_midpoint'], row['premium_yield_close_mean']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.7)
    
    # Plot 2: Premium Yield Curve (Low Price - Pessimistic)
    ax2 = axes[0, 1]
    ax2.plot(summary['otm_midpoint'], summary['premium_yield_low_mean'], 
             marker='s', linewidth=2, markersize=8, label='Average Yield (Low)', color='#A23B72')
    ax2.fill_between(summary['otm_midpoint'], 
                     summary['premium_yield_low_mean'] - summary['premium_yield_low_std'],
                     summary['premium_yield_low_mean'] + summary['premium_yield_low_std'],
                     alpha=0.2, color='#A23B72', label='±1 Std Dev')
    ax2.set_xlabel('OTM Percentage (%)', fontsize=11)
    ax2.set_ylabel('Premium Yield (%)', fontsize=11)
    ax2.set_title('Premium Yield Curve (Low Price - Pessimistic)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    ax2.set_xticks(summary['otm_midpoint'])
    ax2.set_xticklabels([f"{x:.0f}%" for x in summary['otm_midpoint']])
    
    # Add trade count annotations
    for idx, row in summary.iterrows():
        if pd.notna(row['otm_midpoint']) and pd.notna(row['premium_yield_low_mean']):
            ax2.annotate(f"n={int(row['otm_pct_count'])}", 
                        xy=(row['otm_midpoint'], row['premium_yield_low_mean']),
                        xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.7)
    
    # Plot 3: Comparison of Close vs Low Premium Yields
    ax3 = axes[1, 0]
    x_pos = np.arange(len(summary))
    width = 0.35
    
    ax3.bar(x_pos - width/2, summary['premium_yield_close_mean'], width, 
            label='Close Price', color='#2E86AB', alpha=0.8)
    ax3.bar(x_pos + width/2, summary['premium_yield_low_mean'], width, 
            label='Low Price (Pessimistic)', color='#A23B72', alpha=0.8)
    
    ax3.set_xlabel('OTM Range', fontsize=11)
    ax3.set_ylabel('Premium Yield (%)', fontsize=11)
    ax3.set_title('Premium Yield Comparison: Close vs Low Price', fontsize=12, fontweight='bold')
    ax3.set_xticks(x_pos)
    ax3.set_xticklabels(summary['otm_range'], rotation=45, ha='right')
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Average P&L per Contract
    ax4 = axes[1, 1]
    ax4.plot(summary['otm_midpoint'], summary['pnl_per_contract_mean'], 
             marker='D', linewidth=2, markersize=8, label='Avg P&L per Contract', color='#F18F01')
    ax4.set_xlabel('OTM Percentage (%)', fontsize=11)
    ax4.set_ylabel('Average P&L per Contract ($)', fontsize=11)
    ax4.set_title('Average P&L per Contract by OTM Range', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    ax4.legend()
    ax4.set_xticks(summary['otm_midpoint'])
    ax4.set_xticklabels([f"{x:.0f}%" for x in summary['otm_midpoint']])
    
    # Add value annotations
    for idx, row in summary.iterrows():
        if pd.notna(row['otm_midpoint']) and pd.notna(row['pnl_per_contract_mean']):
            ax4.annotate(f"${row['pnl_per_contract_mean']:.0f}", 
                        xy=(row['otm_midpoint'], row['pnl_per_contract_mean']),
                        xytext=(0, 10), textcoords='offset points', fontsize=9, 
                        ha='center', fontweight='bold')
    
    # Plot 5: Volume Distribution by OTM Range (if show_volume is True)
    if show_volume:
        ax5 = axes[2, 0]
        x_pos = np.arange(len(summary))
        bars = ax5.bar(x_pos, summary['volume_sum'], color='#6A994E', alpha=0.8, edgecolor='black', linewidth=1)
        ax5.set_xlabel('OTM Range', fontsize=11)
        ax5.set_ylabel('Total Volume (Contracts)', fontsize=11)
        ax5.set_title('Total Volume by OTM Range', fontsize=12, fontweight='bold')
        ax5.set_xticks(x_pos)
        ax5.set_xticklabels(summary['otm_range'], rotation=45, ha='right')
        ax5.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar, vol in zip(bars, summary['volume_sum']):
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(vol):,}',
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Plot 6: Average Volume per Trade by OTM Range
        ax6 = axes[2, 1]
        avg_volume = summary['volume_sum'] / summary['otm_pct_count']
        bars = ax6.bar(x_pos, avg_volume, color='#BC4749', alpha=0.8, edgecolor='black', linewidth=1)
        ax6.set_xlabel('OTM Range', fontsize=11)
        ax6.set_ylabel('Average Volume per Trade (Contracts)', fontsize=11)
        ax6.set_title('Average Volume per Trade by OTM Range', fontsize=12, fontweight='bold')
        ax6.set_xticks(x_pos)
        ax6.set_xticklabels(summary['otm_range'], rotation=45, ha='right')
        ax6.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bar, vol in zip(bars, avg_volume):
            height = bar.get_height()
            ax6.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(vol):.0f}',
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    
    # Save or show
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\n✅ Saved plot to: {output_file}")
    else:
        plt.show()
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS BY OTM RANGE:")
    print("=" * 80)
    print(summary[['otm_range', 'otm_pct_count', 'premium_yield_close_mean', 
                   'premium_yield_low_mean', 'pnl_per_contract_mean', 
                   'assigned_mean']].to_string(index=False))
    
    return fig, summary


def plot_individual_trades_scatter(
    data_file: str,
    output_file: str = None
):
    """
    Create a scatter plot showing individual trades with yield vs OTM percentage.
    
    Args:
        data_file: Path to consolidated backtest results CSV
        output_file: Path to save the plot (optional)
    """
    df = pd.read_csv(data_file)
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Color code by assignment status
    assigned = df[df['assigned'] == True]
    not_assigned = df[df['assigned'] == False]
    
    ax.scatter(not_assigned['otm_pct'], not_assigned['premium_yield_close'],
               alpha=0.6, s=50, label='Not Assigned', color='#06A77D', edgecolors='black', linewidth=0.5)
    ax.scatter(assigned['otm_pct'], assigned['premium_yield_close'],
               alpha=0.6, s=50, label='Assigned', color='#D00000', edgecolors='black', linewidth=0.5)
    
    # Add trend line
    z = np.polyfit(df['otm_pct'], df['premium_yield_close'], 1)
    p = np.poly1d(z)
    ax.plot(df['otm_pct'].sort_values(), p(df['otm_pct'].sort_values()), 
            "r--", alpha=0.8, linewidth=2, label=f'Trend Line (slope={z[0]:.3f})')
    
    ax.set_xlabel('OTM Percentage (%)', fontsize=12)
    ax.set_ylabel('Premium Yield (%)', fontsize=12)
    ax.set_title('Individual Trades: Premium Yield vs OTM Percentage', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend()
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✅ Saved scatter plot to: {output_file}")
    else:
        plt.show()
    
    return fig


def main():
    parser = argparse.ArgumentParser(description="Visualize covered call backtest yield curves")
    parser.add_argument("--data-file", 
                       default="data/TSLA/monthly/backtest_results/backtest_results_consolidated.csv",
                       help="Path to consolidated backtest results CSV")
    parser.add_argument("--output-dir", 
                       default="data/TSLA/monthly/backtest_results/",
                       help="Directory to save plots")
    parser.add_argument("--show-volume", action="store_true", default=True,
                       help="Show volume data")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate main yield curve plots
    fig, summary = plot_yield_curve(
        data_file=args.data_file,
        output_file=str(output_dir / "yield_curve_analysis.png"),
        show_volume=args.show_volume
    )
    
    # Generate scatter plot
    plot_individual_trades_scatter(
        data_file=args.data_file,
        output_file=str(output_dir / "yield_scatter_plot.png")
    )
    
    print(f"\n✅ All visualizations saved to: {output_dir}")


if __name__ == "__main__":
    main()




