"""
📦 DBT Model Full Path Resolver

This script processes a list of dbt model filenames to:
1. Remove file extensions.
2. Assign `analytics_staging` schema to a specific list of Zendesk staging models.
3. Assign `analytics` schema to all other models (even if "zendesk" is in the name).
4. Construct the full materialization path: `analytics.schema.model_name`.
5. Generate a Redshift-ready VALUES clause row for each model.
6. Output the full model path list and SQL lines to a CSV file.
7. Also outputs a second CSV file with only the sql_values_row column (no quotes around the row values).

Input:
- script_01-dbt_models_list.csv → A CSV with one dbt model filename per row (no header)

Output:
- script_01-dbt_models_full_path.csv → A structured list of resolved dbt model paths + SQL row strings
- script_01-dbt_models_redshift_values.csv → A single-column CSV with just the Redshift VALUES clause rows (unquoted)
"""

import pandas as pd
import os

# File paths (same directory as script)
input_file = "script_01-dbt_models_list.csv"
output_file_full = "script_01-dbt_models_full_path.csv"
output_file_values_only = "script_01-dbt_models_redshift_values.csv"

# Load input CSV
df = pd.read_csv(input_file, header=None, names=["model_file"])

# Strip .sql extension
df["model_name"] = df["model_file"].str.replace(".sql", "", regex=False)

# Define hardcoded staging models
staging_models = {
    "zendesk_field_additions",
    "zendesk_field_consolidation",
    "zendesk_org_url",
    "zendesk_tags_xf",
    "zendesk_ticket_fields_boolean",
    "zendesk_ticket_fields_string",
    "zendesk_tickets_xf"
}

# Schema assignment
df["schema"] = df["model_name"].apply(lambda name: "analytics_staging" if name in staging_models else "analytics")
df["database"] = "analytics"
df["full_path"] = df["database"] + "." + df["schema"] + "." + df["model_name"]

# Create SQL VALUES clause row with single-quoted strings
df["sql_values_row"] = df.apply(
    lambda row: f"('{row['database']}', '{row['schema']}', '{row['model_name']}')",
    axis=1
)

# Save values-only CSV manually (with single quotes and trailing comma)
with open(output_file_values_only, "w", newline='') as f:
    f.write("sql_values_row\n")
    for val in df["sql_values_row"]:
        f.write(f"{val},\n")

# Save full output CSV manually to avoid quotes/escapes
with open(output_file_full, "w", newline='') as f:
    f.write("model_file,model_name,schema,database,full_path,sql_values_row\n")
    for _, row in df.iterrows():
        f.write(f"{row['model_file']},{row['model_name']},{row['schema']},{row['database']},{row['full_path']},{row['sql_values_row']}\n")

# Save values-only CSV manually (with single quotes and trailing comma)
with open(output_file_values_only, "w", newline='') as f:
    f.write("sql_values_row\n")
    for val in df["sql_values_row"]:
        f.write(f"{val},\n")

# Print result summary
print(f"✅ Processed {len(df)} models.")
print(f"📤 Full output saved to: {output_file_full}")
print(f"📤 Values-only output saved to: {output_file_values_only}")