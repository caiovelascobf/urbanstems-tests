r"""
Compare potential Redshift paths from one CSV with paths in the main audit CSV.

Inputs:
- raw\dbt_audit_not_accurate_dbt_models.csv (inaccurate DBT model paths)
- raw\redshift_table_audit.csv (main table audit data)

Output:
- script_04-merge_redshift_tables_with_dbt_accuracy.csv

Comparison:
- Match 'potential_redshift_path' (in file 1) with 'redshift_path' (in file 2).
- Normalize case and trim leading/trailing whitespace for both columns.
- Add '_merge' column: 'both' if matched, 'left_only' if not.
- Add 'accuracy_flag': 'Y' if not matched (accurate), 'N' if matched (not accurate).
- Final output is the original audit CSV with '_merge' and 'accuracy_flag' columns.
"""

import pandas as pd

# File paths
audit_issues_csv = r"raw\dbt_audit_not_accurate_dbt_models.csv"
main_audit_csv = r"raw\redshift_table_audit.csv"
output_csv = 'script_04-merge_redshift_tables_with_dbt_accuracy.csv'

# Function to clean path: lowercased and trimmed (preserves internal whitespace)
def clean_path(path):
    return str(path).strip().lower()

# Load CSVs
df_issues = pd.read_csv(audit_issues_csv)
df_main = pd.read_csv(main_audit_csv)

# Normalize path columns
df_issues['potential_redshift_path'] = df_issues['potential_redshift_path'].apply(clean_path)
df_main['redshift_path'] = df_main['redshift_path'].apply(clean_path)

# Minimal set for merge
df_issues_flags = df_issues[['potential_redshift_path']].drop_duplicates()

# Merge on redshift path
merged_df = df_main.merge(
    df_issues_flags,
    how='left',
    left_on='redshift_path',
    right_on='potential_redshift_path',
    indicator=True
)

# Assign accuracy_flag: 'N' for match (inaccurate), 'Y' for not matched (accurate)
merged_df['accuracy_flag'] = merged_df['_merge'].apply(lambda x: 'N' if x == 'both' else 'Y')

# Drop helper column
merged_df.drop(columns=['potential_redshift_path'], inplace=True)

# Save output
merged_df.to_csv(output_csv, index=False)

# Summary
total_issues = len(df_issues)
total_main = len(df_main)
inaccurate = (merged_df['accuracy_flag'] == 'N').sum()
accurate = (merged_df['accuracy_flag'] == 'Y').sum()

print(f"Output written to: {output_csv}")
print("=== Summary ===")
print(f"Total rows in ISSUE CSV     : {total_issues}")
print(f"Total rows in MAIN CSV      : {total_main}")
print(f"Inaccurate (flagged) rows   : {inaccurate}")
print(f"Accurate (clean) rows       : {accurate}")
