"""
Description:
    This script merges:
    - Dashboard lineage from `script_02-dashboards_to_views_to_redshift.csv`
    - Explore + View usage flags from `script_04-flag_unused_explores_and_views.csv`

Output:
    A single table showing each dashboard → explore → view → table,
    enriched with usage flags for explore and view.

Inputs:
    - script_02-dashboards_to_views_to_redshift.csv
    - script_04-flag_unused_explores_and_views.csv

Output:
    - script_05-dashboards_explores_views_usage.csv
"""

import pandas as pd

# === FILE PATHS ===
DASHBOARDS_PATH = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_data\script_02-dashboards_to_views_to_redshift.csv"
USAGE_FLAGS_PATH = "script_04-flag_unused_explores_and_views.csv"
OUTPUT_PATH = "script_05-dashboards_explores_views_usage.csv"

# === LOAD DATA ===
dashboards_df = pd.read_csv(DASHBOARDS_PATH)
usage_df = pd.read_csv(USAGE_FLAGS_PATH)

# === CLEAN COLUMNS ===
dashboards_df.columns = dashboards_df.columns.str.strip().str.lower()
usage_df.columns = usage_df.columns.str.strip().str.lower()

# === RENAME FOR MERGE SAFETY ===
dashboards_df = dashboards_df.rename(columns={
    "query_model": "model_name",
    "query_explore": "explore_name",
    "view_or_model_name": "view_name"
})

# === MERGE ON (model, explore, view) ===
merged = dashboards_df.merge(
    usage_df,
    how="left",
    on=["model_name", "explore_name", "view_name"]
)

# === EXPORT ===
merged.to_csv(OUTPUT_PATH, index=False)

# === SUMMARY ===
print(f"\n✅ Final dashboard + usage matrix saved to: {OUTPUT_PATH}")
print(f"🔗 Total rows: {len(merged)}")
print(f"📊 Unique dashboards: {merged['dashboard_title'].nunique()}")
print(f"📈 Unique explores: {merged['explore_name'].nunique()}")
