"""
Description:
    This script joins two CSV files:
      1. A dashboard metadata file (from Looker System Activity → Dashboards) which includes
         dashboard titles and the `query_explore` used in each tile.
      2. A parsed LookML views mapping file (output of script_01) that lists each view's
         associated Redshift tables via either `sql_table_name` or `derived_table_sources`.

    Outputs:
      - A full joined CSV mapping dashboards → explores → views → Redshift tables.
      - A second exploded version where redshift_tables are unflattened into individual rows.

Inputs:
    - system__activity_dashboard_explores_models_2025-06-16T1959.csv
    - script_01-looker_views_and_its_tables_mapping.csv

Outputs:
    - script_02-dashboards_to_views_to_redshift.csv
    - script_02-dashboards_to_views_to_redshift_exploded.csv

Usage:
    Place the input CSVs in the same directory and run the script.
"""

import pandas as pd

# === INPUT FILES ===
DASHBOARD_CSV = r"raw\system__activity_dashboard_explores_models_2025-06-16T1959.csv"
VIEWS_CSV = "script_01-looker_views_and_its_tables_mapping.csv"
OUTPUT_CSV = "script_02-dashboards_to_views_to_redshift.csv"
OUTPUT_EXPLODED = "script_02-dashboards_to_views_to_redshift_exploded.csv"

# === LOAD DATA ===
dashboards = pd.read_csv(DASHBOARD_CSV)
views = pd.read_csv(VIEWS_CSV)

# === CLEAN COLUMN NAMES ===
dashboards.columns = dashboards.columns.str.strip().str.lower().str.replace(" ", "_")
views.columns = views.columns.str.strip().str.lower()

# === JOIN ON QUERY_EXPLORE <-> VIEW_NAME ===
merged = dashboards.merge(
    views,
    how="left",
    left_on="query_explore",
    right_on="view_name"
)

# === BUILD redshift_tables COLUMN ===
def combine_sources(row):
    tables = set()
    if pd.notnull(row.get("sql_table_name")) and row["sql_table_name"].strip():
        tables.add(row["sql_table_name"].strip())
    if pd.notnull(row.get("derived_table_sources")) and row["derived_table_sources"].strip():
        derived_split = [t.strip() for t in row["derived_table_sources"].split(",")]
        tables.update(filter(None, derived_split))
    return ", ".join(sorted(tables)) if tables else None

merged["redshift_tables"] = merged.apply(combine_sources, axis=1)

# === DROP sql_table_name and derived_table_sources ===
merged_clean = merged.drop(columns=["sql_table_name", "derived_table_sources"], errors="ignore")

# === EXPORT MAIN CSV ===
merged_clean.to_csv(OUTPUT_CSV, index=False)

# === EXPLODE redshift_tables INTO MULTIPLE ROWS ===
exploded = merged_clean.copy()
exploded["redshift_tables"] = exploded["redshift_tables"].fillna("")

# Split and explode
exploded = exploded.assign(
    redshift_table=exploded["redshift_tables"].str.split(r",\s*")
).explode("redshift_table")

# Drop original combined list column
exploded = exploded.drop(columns=["redshift_tables"])

# === EXPORT EXPLODED CSV ===
exploded.to_csv(OUTPUT_EXPLODED, index=False)

# === SUMMARY ===
print(f"\n✅ Final dashboard-to-Redshift mapping saved to: {OUTPUT_CSV}")
print(f"🪄 Exploded version saved to: {OUTPUT_EXPLODED}")
print(f"📊 Total dashboards mapped: {len(merged_clean)}")
print(f"📈 Total exploded rows: {len(exploded)}")
