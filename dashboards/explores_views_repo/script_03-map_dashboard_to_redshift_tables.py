"""
Description:
    This script maps dbt models (from a .csv listing all .sql files)
    to dashboards that use those models, based on an exploded Redshift table → dashboard mapping.

    Enhancements:
        - Adds a column `potential_full_path` = "analytics." + dbt_model_name
        - Matches dashboards using redshift_table values ending with this full path

Inputs:
    - script_01-dbt_models_list.csv (e.g. `orders.sql`)
    - script_02-dashboards_to_views_to_redshift_exploded.csv
        Required columns: redshift_table, dashboard_title

Output:
    - script_03-dbt_models_to_dashboards.csv
        Columns:
            - dbt_model_file
            - dbt_model_name
            - potential_full_path
            - associated_dashboards
"""

import pandas as pd

# === INPUT FILES ===
DBT_MODELS_CSV = r"raw\script_01-dbt_models_list.csv"
EXPLODED_CSV = "script_02-dashboards_to_views_to_redshift_exploded.csv"
OUTPUT_CSV = "script_03-redshift_tables_to_dashboards.csv"

# === LOAD DATA ===
dbt_models = pd.read_csv(DBT_MODELS_CSV, header=None, names=["dbt_model_file"])
dashboards = pd.read_csv(EXPLODED_CSV)

# === PREPARE DBT MODEL COLUMNS ===
dbt_models["dbt_model_name"] = dbt_models["dbt_model_file"].str.replace(".sql$", "", regex=True)
dbt_models["potential_full_path"] = "analytics." + dbt_models["dbt_model_name"]

# === MAP DBT MODELS TO DASHBOARDS USING potential_full_path ===
dashboard_map = {}

for _, row in dbt_models.iterrows():
    full_path = row["potential_full_path"]
    matched_dashboards = dashboards[
        dashboards["redshift_table"].str.strip().eq(full_path)
    ]["dashboard_title"].dropna().unique()

    dashboard_map[full_path] = ", ".join(sorted(matched_dashboards)) if len(matched_dashboards) > 0 else None

dbt_models["associated_dashboards"] = dbt_models["potential_full_path"].map(dashboard_map)

# === EXPORT FINAL RESULT ===
output = dbt_models[["dbt_model_file", "dbt_model_name", "potential_full_path", "associated_dashboards"]]
output.to_csv(OUTPUT_CSV, index=False)

# === SUMMARY ===
linked_count = output["associated_dashboards"].notnull().sum()
print(f"\n✅ DBT model to dashboard mapping saved to: {OUTPUT_CSV}")
print(f"📄 Total dbt models processed: {len(output)}")
print(f"🔗 Models linked to dashboards: {linked_count}")
