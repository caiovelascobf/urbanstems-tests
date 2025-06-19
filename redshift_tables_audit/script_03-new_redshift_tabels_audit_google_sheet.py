r"""
Compare two CSV files using only 'schema_name' and 'object_name', and merge selected columns from the old file if rows match.

Inputs:
- raw\redshift_objects_audit_google_sheet_2025_06_19.csv (old CSV)
- raw\dbeaver_2025_06_19_redshift_objects_usage_audit.csv (new CSV)

Output:
- script_03-new_redshit_objects_audit_google_sheet.csv

Comparison:
- Use only ['schema_name', 'object_name'] to compare.
- Ignore 'object_type', 'queried_by', etc. during comparison.
- Format 'last_query_time' from new CSV from DD/MM/YYYY to YYYY-MM-DD.
- If matched, merge in ['ingestion_tool', 'is_used_by_dbt'] from the old file.
- Add a 'match' column indicating 'Y' (match) or 'N' (no match).
- Final output contains all columns from new CSV + merged columns + match flag.
"""

import pandas as pd

# File paths
old_csv = r"raw\redshift_objects_audit_google_sheet_2025_06_19.csv"
new_csv = r"raw\dbeaver_2025_06_19_redshift_objects_usage_audit.csv"
output_csv = 'script_03-new_redshit_objects_audit_google_sheet.csv'

# Columns for comparison
compare_cols = ['schema_name', 'object_name']

# Load both CSVs
df_old = pd.read_csv(old_csv)
df_new = pd.read_csv(new_csv)

# Normalize comparison fields
for col in compare_cols:
    df_old[col] = df_old[col].astype(str).str.strip().str.lower()
    df_new[col] = df_new[col].astype(str).str.strip().str.lower()

# Select only the needed columns from old CSV
columns_to_merge = compare_cols + ['ingestion_tool', 'is_used_by_dbt']
df_old_reduced = df_old[columns_to_merge]

# Merge and flag matches
merged_df = df_new.merge(
    df_old_reduced,
    on=compare_cols,
    how='left',
    indicator=True
)

merged_df['match'] = merged_df['_merge'].apply(lambda x: 'Y' if x == 'both' else 'N')
merged_df.drop(columns=['_merge'], inplace=True)

# Reorder columns for final output
ordered_columns = [
    'schema_name', 'object_name', 'object_type', 'is_used', 'queried_by',
    'last_query_time', 'scan_count', 'ingestion_tool', 'is_used_by_dbt', 'match'
]
# Ensure only columns that exist are selected
ordered_columns = [col for col in ordered_columns if col in merged_df.columns]
merged_df = merged_df[ordered_columns]

# Save to file
merged_df.to_csv(output_csv, index=False)

# Summary
total_old = len(df_old)
total_new = len(df_new)
matched = (merged_df['match'] == 'Y').sum()
unmatched = (merged_df['match'] == 'N').sum()

print(f"Output written to: {output_csv}")
print("=== Summary ===")
print(f"Total rows in OLD CSV       : {total_old}")
print(f"Total rows in NEW CSV       : {total_new}")
print(f"Matched rows                : {matched}")
print(f"Unmatched rows              : {unmatched}")
