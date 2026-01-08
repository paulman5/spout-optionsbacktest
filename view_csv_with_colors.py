#!/usr/bin/env python3
"""
Script to generate an HTML file from a CSV with strike prices displayed in blue.
"""

import pandas as pd
import sys
from pathlib import Path

def csv_to_html_with_colors(csv_path, output_path=None):
    """
    Convert CSV to HTML with strike price column in blue.
    """
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Find strike column index
    strike_col = 'strike'
    if strike_col not in df.columns:
        print(f"Warning: 'strike' column not found. Available columns: {list(df.columns)}")
        return
    
    # Generate HTML
    html = ['<!DOCTYPE html>']
    html.append('<html><head>')
    html.append('<title>CSV Viewer - Options Data</title>')
    html.append('<style>')
    html.append('''
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 20px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }
        th {
            padding: 10px;
            text-align: left;
            font-weight: bold;
            background-color: #333;
            color: white;
            border: 1px solid #555;
            position: sticky;
            top: 0;
        }
        td {
            padding: 8px;
            border: 1px solid #ddd;
        }
        tbody tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        tbody tr:hover {
            background-color: #f0f0f0;
        }
        /* Strike price column - bright blue with background */
        .strike-price {
            background-color: #e3f2fd !important;
            color: #1565c0 !important;
            font-weight: bold;
            border-left: 3px solid #1976d2 !important;
            border-right: 3px solid #1976d2 !important;
        }
        .strike-header {
            background-color: #1976d2 !important;
            color: #ffffff !important;
            font-weight: bold;
        }
        
        /* Premium columns - orange */
        .premium {
            background-color: #fff3e0 !important;
            color: #e65100 !important;
            font-weight: bold;
        }
        .premium-header {
            background-color: #f57c00 !important;
            color: #ffffff !important;
        }
        
        /* Volume column - green */
        .volume {
            background-color: #f1f8e9 !important;
            color: #33691e !important;
        }
        .volume-header {
            background-color: #689f38 !important;
            color: #ffffff !important;
        }
        
        /* Price columns - pink */
        .price {
            background-color: #fce4ec !important;
            color: #880e4f !important;
        }
        .price-header {
            background-color: #c2185b !important;
            color: #ffffff !important;
        }
        
        /* Yield columns - purple */
        .yield {
            background-color: #f3e5f5 !important;
            color: #4a148c !important;
        }
        .yield-header {
            background-color: #7b1fa2 !important;
            color: #ffffff !important;
        }
        
        /* OTM column - teal */
        .otm {
            background-color: #e0f2f1 !important;
            color: #004d40 !important;
        }
        .otm-header {
            background-color: #00695c !important;
            color: #ffffff !important;
        }
    ''')
    html.append('</style>')
    html.append('</head><body>')
    html.append('<div class="container">')
    html.append(f'<h1>CSV Viewer: {Path(csv_path).name}</h1>')
    html.append('<div style="overflow-x: auto; max-height: 80vh; overflow-y: auto;">')
    html.append('<table>')
    
    # Define column color mappings
    def get_column_class(col_name):
        col_lower = col_name.lower()
        if 'strike' in col_lower:
            return 'strike'
        elif 'premium' in col_lower:
            return 'premium'
        elif 'volume' in col_lower:
            return 'volume'
        elif any(x in col_lower for x in ['open_price', 'close_price', 'high_price', 'low_price', 'underlying_open', 'underlying_close', 'underlying_high', 'underlying_low', 'underlying_spot']):
            return 'price'
        elif 'yield' in col_lower:
            return 'yield'
        elif 'otm' in col_lower:
            return 'otm'
        return None
    
    # Header row
    html.append('<thead><tr>')
    for col in df.columns:
        col_class = get_column_class(col)
        if col_class:
            html.append(f'<th class="{col_class}-header">{col}</th>')
        else:
            html.append(f'<th>{col}</th>')
    html.append('</tr></thead>')
    
    # Data rows
    html.append('<tbody>')
    for _, row in df.iterrows():
        html.append('<tr>')
        for col in df.columns:
            value = row[col]
            # Format numeric values to 2 decimals if they're floats
            if pd.api.types.is_float_dtype(df[col]) and pd.notna(value):
                if col != 'window_start':  # Don't format window_start
                    value = f"{float(value):.2f}"
            
            col_class = get_column_class(col)
            if col_class:
                html.append(f'<td class="{col_class}">{value}</td>')
            else:
                html.append(f'<td>{value}</td>')
        html.append('</tr>')
    html.append('</tbody>')
    
    html.append('</table>')
    html.append('</div>')
    html.append('</div>')
    html.append('</body></html>')
    
    # Write to file
    if output_path is None:
        output_path = Path(csv_path).with_suffix('.html')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(html))
    
    print(f"HTML file generated: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python view_csv_with_colors.py <csv_file> [output_html_file]")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    csv_to_html_with_colors(csv_path, output_path)

