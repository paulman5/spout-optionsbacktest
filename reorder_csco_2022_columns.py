#!/usr/bin/env python3
"""
Reorder CSCO 2022 monthly file columns to match 2024 file order.
"""

import pandas as pd
from pathlib import Path

# Read reference file (2024) to get column order
ref_file = Path("python-boilerplate/data/CSCO/monthly/2024_options_pessimistic.csv")
target_file = Path("python-boilerplate/data/CSCO/monthly/2022_options_pessimistic.csv")

print("=" * 80)
print("REORDERING CSCO 2022 COLUMNS TO MATCH 2024")
print("=" * 80)

# Read reference file to get column order
df_ref = pd.read_csv(ref_file)
ref_columns = list(df_ref.columns)

print(f"\nReference file (2024) has {len(ref_columns)} columns:")
print(ref_columns)

# Read target file
df_target = pd.read_csv(target_file)
target_columns = list(df_target.columns)

print(f"\nTarget file (2022) has {len(target_columns)} columns:")
print(target_columns)

# Find columns in target but not in reference
extra_cols = [c for c in target_columns if c not in ref_columns]
print(f"\nExtra columns in 2022 (not in 2024): {extra_cols}")

# Find columns in reference but not in target
missing_cols = [c for c in ref_columns if c not in target_columns]
print(f"Missing columns in 2022 (in 2024): {missing_cols}")

# Create new column order: start with reference order, then add any extra columns
new_order = []
for col in ref_columns:
    if col in df_target.columns:
        new_order.append(col)

# Add any extra columns that exist in target but not in reference (at the end)
for col in extra_cols:
    if col in df_target.columns:
        new_order.append(col)

print(f"\nNew column order ({len(new_order)} columns):")
print(new_order)

# Reorder the dataframe
df_target = df_target[new_order]

# Save the file
df_target.to_csv(target_file, index=False)
print(f"\n✅ Reordered and saved {target_file.name}")

print("=" * 80)

#!/usr/bin/env python3
"""
Reorder CSCO 2022 monthly file columns to match 2024 file order.
"""

import pandas as pd
from pathlib import Path

# Read reference file (2024) to get column order
ref_file = Path("python-boilerplate/data/CSCO/monthly/2024_options_pessimistic.csv")
target_file = Path("python-boilerplate/data/CSCO/monthly/2022_options_pessimistic.csv")

print("=" * 80)
print("REORDERING CSCO 2022 COLUMNS TO MATCH 2024")
print("=" * 80)

# Read reference file to get column order
df_ref = pd.read_csv(ref_file)
ref_columns = list(df_ref.columns)

print(f"\nReference file (2024) has {len(ref_columns)} columns:")
print(ref_columns)

# Read target file
df_target = pd.read_csv(target_file)
target_columns = list(df_target.columns)

print(f"\nTarget file (2022) has {len(target_columns)} columns:")
print(target_columns)

# Find columns in target but not in reference
extra_cols = [c for c in target_columns if c not in ref_columns]
print(f"\nExtra columns in 2022 (not in 2024): {extra_cols}")

# Find columns in reference but not in target
missing_cols = [c for c in ref_columns if c not in target_columns]
print(f"Missing columns in 2022 (in 2024): {missing_cols}")

# Create new column order: start with reference order, then add any extra columns
new_order = []
for col in ref_columns:
    if col in df_target.columns:
        new_order.append(col)

# Add any extra columns that exist in target but not in reference (at the end)
for col in extra_cols:
    if col in df_target.columns:
        new_order.append(col)

print(f"\nNew column order ({len(new_order)} columns):")
print(new_order)

# Reorder the dataframe
df_target = df_target[new_order]

# Save the file
df_target.to_csv(target_file, index=False)
print(f"\n✅ Reordered and saved {target_file.name}")

print("=" * 80)

