"""
Description:
    This script generates a clean mapping of:
        - dashboard_id
        - dashboard_title
        - explore_name
        - view_name
        - model_name

    It uses the dashboard lineage file that links dashboards to LookML explores and views.
    Some dashboards may appear multiple times — this happens when a dashboard contains
    multiple tiles pointing to different explores/views.

    This version filters out entries with missing explore_name to exclude dashboard tiles
    that do not directly reference a LookML explore (e.g., text tiles, broken links, or
    legacy content without lineage).

Input:
    - script_02-dashboards_to_views_to_redshift.csv

Output:
    - script_05-dashboard_explore_view_mapping.csv
"""

import pandas as pd

# === FILE PATHS ===
DASHBOARDS_PATH = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_02-dashboards_to_views_to_redshift.csv"
OUTPUT_MAPPING = "script_05-dashboard_explore_view_mapping.csv"

# === LOAD DATA ===
dash_df = pd.read_csv(DASHBOARDS_PATH)

# === CLEAN COLUMN NAMES ===
dash_df.columns = (
    dash_df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("(", "")
    .str.replace(")", "")
    .str.replace("-", "_")
)

# === RENAME RELEVANT COLUMNS ===
dash_df.rename(columns={
    "dashboard_id_user_defined_only": "dashboard_id",
    "query_model": "model_name",
    "query_explore": "explore_name",
    "view_or_model_name": "view_name"
}, inplace=True)

# === FILTER OUT ROWS WITH NO EXPLORE ===
dash_df = dash_df[dash_df["explore_name"].notna()]

# === SELECT AND DEDUPE ===
mapping_df = dash_df[[
    "dashboard_id", "dashboard_title", "model_name", "explore_name", "view_name"
]].drop_duplicates()

# === EXPORT ===
mapping_df.to_csv(OUTPUT_MAPPING, index=False)

print(f"\n✅ Dashboard → Explore → View mapping saved to: {OUTPUT_MAPPING}")
print(f"🔢 Unique dashboards: {mapping_df['dashboard_id'].nunique()}")
print(f"🔗 Total dashboard-explore-view combinations: {len(mapping_df)}")
