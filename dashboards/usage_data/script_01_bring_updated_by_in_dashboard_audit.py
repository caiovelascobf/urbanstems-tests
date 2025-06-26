r"""
Merge user update names into the main dashboard audit CSV.

Inputs:
- dashboards_audit_google_sheet_2025_06_19.csv (main audit with 'dashboard', 'id')
- dashboards_audit_google_sheet_2025_06_19_user_updated_by.csv (updates with 'dashboard', 'id', 'updated_by_name')

Output:
- script_01_bring_updated_by_in_dashboard_audit.csv

Comparison:
- Match 'dashboard' and 'id' from both CSVs (case-insensitive, trimmed).
- Add 'updated_by_name' and 'was_merged' columns.
- 'was_merged' = 1 if match found, else 0.
"""

import pandas as pd

# File paths
main_csv = r"raw\dashboards_audit_google_sheet_2025_06_19.csv"
user_csv = r"raw\dashboards_audit_google_sheet_2025_06_19_user_updated_by.csv"
output_csv = "script_01_bring_updated_by_in_dashboard_audit.csv"

# Clean function: lowercase and trim
def clean_str(value):
    return str(value).strip().lower()

# Load data
df_main = pd.read_csv(main_csv)
df_user = pd.read_csv(user_csv)

# Normalize 'dashboard' and 'id'
df_main['dashboard_clean'] = df_main['dashboard'].apply(clean_str)
df_main['id_clean'] = df_main['id'].apply(clean_str)

df_user['dashboard_clean'] = df_user['dashboard'].apply(clean_str)
df_user['id_clean'] = df_user['id'].apply(clean_str)

# Select relevant columns from user CSV
df_user_cleaned = df_user[['dashboard_clean', 'id_clean', 'updated_by_name']].drop_duplicates()

# Merge on both cleaned fields
merged_df = df_main.merge(
    df_user_cleaned,
    on=['dashboard_clean', 'id_clean'],
    how='left',
    indicator=True
)

# Add merge flag
merged_df['was_merged'] = merged_df['_merge'].apply(lambda x: 1 if x == 'both' else 0)

# Drop helper columns
merged_df.drop(columns=['dashboard_clean', 'id_clean', '_merge'], inplace=True)

# Save result
merged_df.to_csv(output_csv, index=False)

# Summary
total_main = len(df_main)
total_merged = (merged_df['was_merged'] == 1).sum()
not_merged = (merged_df['was_merged'] == 0).sum()

print(f"Output written to: {output_csv}")
print("=== Summary ===")
print(f"Total rows in MAIN CSV     : {total_main}")
print(f"Rows merged with user info : {total_merged}")
print(f"Rows not merged            : {not_merged}")
