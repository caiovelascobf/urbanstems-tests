r"""
Compare known inaccurate dashboards against the main dashboard audit list.

Inputs:
- script_01_unique_dashboards_dbt_not_accurate.csv (list of flagged dashboards)
- raw\dashboards_audit.csv (main audit with full dashboard list)

Output:
- script_02_unique_dash_dbt_not_accurate.csv

Matching:
- Use 'dashboard_name' from the inaccurate list to match against 'dashboard' in the main audit.
- Normalize by stripping leading/trailing whitespace and converting to lowercase.
- Add:
    - 'deprecate': 'Y' if flagged as inaccurate, else 'N'
    - 'match': 'Y' if matched, else 'N'
"""

import pandas as pd

# File paths
flagged_dash_csv = r"script_01_unique_dashboards_dbt_not_accurate.csv"
main_dash_csv = r"raw\dashboards_audit.csv"
output_csv = r"script_02_unique_dash_dbt_not_accurate.csv"

# Load CSVs
df_flagged = pd.read_csv(flagged_dash_csv)
df_main = pd.read_csv(main_dash_csv)

# Normalize for matching
df_flagged['dashboard_name_clean'] = df_flagged['dashboard_name'].astype(str).str.strip().str.lower()
df_main['dashboard_clean'] = df_main['dashboard'].astype(str).str.strip().str.lower()

# Merge to flag dashboards
merged_df = df_main.merge(
    df_flagged[['dashboard_name_clean']],
    how='left',
    left_on='dashboard_clean',
    right_on='dashboard_name_clean',
    indicator=True
)

# Assign match and deprecate flags
merged_df['match'] = merged_df['_merge'].apply(lambda x: 'Y' if x == 'both' else 'N')
merged_df['deprecate'] = merged_df['match'].apply(lambda x: 'Y' if x == 'Y' else 'N')

# Drop helper columns
merged_df.drop(columns=['dashboard_clean', 'dashboard_name_clean', '_merge'], inplace=True)

# Save output
merged_df.to_csv(output_csv, index=False)

# Summary
total_flagged = len(df_flagged)
total_main = len(df_main)
matched = (merged_df['match'] == 'Y').sum()
unmatched = (merged_df['match'] == 'N').sum()

print(f"Output written to: {output_csv}")
print("=== Summary ===")
print(f"Total dashboards flagged     : {total_flagged}")
print(f"Total dashboards in audit    : {total_main}")
print(f"Matched (deprecate=Y)        : {matched}")
print(f"Unmatched (deprecate=N)      : {unmatched}")
