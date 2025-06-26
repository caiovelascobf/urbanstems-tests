"""
Extract a list of unique dashboards from a CSV where the dashboards are comma-separated in one column.

Input:
- raw\dbt_audit_not_accurate_dbt_models_and_dashboards.csv

Output:
- script_01_unique_dashboards_dbt_not_accurate.csv

Details:
- Column 'dashboards' contains multiple dashboards per row, comma-separated.
- Extract, normalize (trim spaces), deduplicate, and sort the list.
- Save as one-column CSV: 'dashboard_name'
"""

import pandas as pd

# File paths
input_csv = r"raw\dbt_audit_not_accurate_dbt_models_and_dashboards.csv"
output_csv = "script_01_unique_dashboards_dbt_not_accurate.csv"

# Load data
df = pd.read_csv(input_csv)

# Extract and split dashboards
dashboards_series = df['dashboards'].dropna().astype(str).str.split(',')

# Flatten and normalize
dashboard_list = [dash.strip() for sublist in dashboards_series for dash in sublist]

# Remove empty strings, deduplicate, and sort
unique_dashboards = sorted(set(d for d in dashboard_list if d))

# Create final DataFrame
df_unique = pd.DataFrame(unique_dashboards, columns=['dashboard_name'])

# Save to CSV
df_unique.to_csv(output_csv, index=False)

# Summary
print(f"Extracted {len(df_unique)} unique dashboards.")
print(f"Output written to: {output_csv}")
