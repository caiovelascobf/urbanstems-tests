"""
This script compares a Redshift audit CSV with a list of tables not used by DBT
and flags rows accordingly.

Each row from the main audit file is checked against the deleted rows list (not used by DBT).
If the row is found in the deleted CSV, it's flagged as 'N' in the 'Is Used by DBT' column.
Otherwise, it's flagged as 'Y'.

This comparison accounts for empty/null values in the last 3 columns.

Inputs:
- raw\bf_us_google_sheet_redshift_audit.csv (main data)
- raw\bf_us_google_sheet_redshift_not_used_by_dbt_audit (rows not used by DBT)
"""

import pandas as pd
import numpy as np

# File paths
main_path = r"raw\bf_us_google_sheet_redshift_audit.csv"
deleted_path = r"raw\bf_us_google_sheet_redshift_not_used_by_dbt_audit.csv"
output_path = "script_01-redshift_audit_with_dbt_flag.csv"

# Load CSVs with missing values handled uniformly
main_df = pd.read_csv(main_path, keep_default_na=False).fillna('')
deleted_df = pd.read_csv(deleted_path, keep_default_na=False).fillna('')

# Columns used for comparison
key_columns = [
    'Ingestion Tool', 'Schema Name', 'Table Name', 'Is Used in Redshift',
    'Queried By', 'Last Query Time', 'Scan Count'
]

# Filter to comparison columns only
main_df = main_df[key_columns]
deleted_df = deleted_df[key_columns]

# Create tuple keys from rows to compare, accounting for missing values
main_df['_merge_key'] = main_df.apply(lambda row: tuple(row.values), axis=1)
deleted_keys = set(deleted_df.apply(lambda row: tuple(row.values), axis=1))

# Determine usage flag
main_df['Is Used by DBT'] = main_df['_merge_key'].apply(lambda x: 'N' if x in deleted_keys else 'Y')

# Drop helper key column
main_df.drop(columns=['_merge_key'], inplace=True)

# Save result
main_df.to_csv(output_path, index=False)

print(f"Flagged CSV saved to: {output_path}")
