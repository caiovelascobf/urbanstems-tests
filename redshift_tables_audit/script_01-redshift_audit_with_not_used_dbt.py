"""
This script flags Redshift objects as either used or not used by DBT,
based on a comparison between two CSV files:

1. Main audit file: Contains all Redshift objects.
   - File: raw\bf_us_google_sheet_redshift_audit.csv
   - Expected columns: 'Schema Name', 'Object Name', 'Object Type'

2. Not used by DBT file: Contains objects known to be unused by DBT.
   - File: raw\bf_us_google_sheet_redshift_not_used_by_dbt_audit.csv
   - Expected columns: 'Schema Name', 'Object Name'

For each row in the main file, the script checks whether the
(Schema Name, Object Name) pair exists in the "not used by DBT" list.

If a match is found, it flags the object with 'N' in the 'Is Used by DBT' column
(indicating it is *not* used by DBT). Otherwise, it flags it with 'Y' (used by DBT).

Output:
- CSV file with an additional 'Is Used by DBT' column.
- Output file: script_01-redshift_audit_with_not_used_dbt.csv
"""

import pandas as pd

# File paths
main_path = r"raw\bf_us_google_sheet_redshift_audit.csv"
deleted_path = r"raw\bf_us_google_sheet_redshift_not_used_by_dbt_audit.csv"
output_path = "script_01-redshift_audit_with_not_used_dbt.csv"

# Load CSVs, treat blanks as empty strings
main_df = pd.read_csv(main_path, keep_default_na=False).fillna('')
deleted_df = pd.read_csv(deleted_path, keep_default_na=False).fillna('')

# Use only Schema Name and Object Name for comparison
main_df['_key'] = main_df[['Schema Name', 'Object Name']].apply(tuple, axis=1)
deleted_keys = set(deleted_df[['Schema Name', 'Object Name']].apply(tuple, axis=1))

# Assign flag: N if found in deleted list (not used), Y otherwise
main_df['Is Used by DBT'] = main_df['_key'].apply(lambda x: 'N' if x in deleted_keys else 'Y')

# Drop helper column
main_df.drop(columns=['_key'], inplace=True)

# Save output
main_df.to_csv(output_path, index=False)

print(f"Flagged CSV saved to: {output_path}")
