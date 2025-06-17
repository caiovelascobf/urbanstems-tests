"""
📄 Sort DBT Model Match Results by Model Name

This script reads the model match results CSV (from Redshift), sorts the models
by `dbt_model_name` in ascending order, and writes a new sorted CSV.

Input:
- script_02-test_dbt_models_match_in_redshift_202506171116.csv

Output:
- script_03-test_dbt_models_match_in_redshift_sorted.csv
"""

import pandas as pd

# File paths
input_csv = "script_02-test_dbt_models_match_in_redshift_202506171116.csv"
output_csv = "script_03-test_dbt_models_match_in_redshift_sorted.csv"

# Load the CSV
df = pd.read_csv(input_csv)

# Sort by dbt_model_name ASC
df_sorted = df.sort_values(by="dbt_model_name", ascending=True)

# Write to new CSV
df_sorted.to_csv(output_csv, index=False)

print(f"✅ Sorted file saved as: {output_csv}")
