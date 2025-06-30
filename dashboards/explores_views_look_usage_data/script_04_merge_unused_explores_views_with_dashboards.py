"""
Description:
    This script merges:
    - Dashboard lineage (explore + view usage per dashboard)
    - Explore usage flags
    - View usage flags

    The goal is to produce a single dashboard-level table with:
        - dashboard_id (user-defined only)
        - dashboard_title
        - Whether all explores are safe to deprecate
        - Whether all views are safe to deprecate
        - Whether the dashboard itself is a candidate for deprecation

Inputs:
    - script_02-dashboards_to_views_to_redshift.csv
    - script_02-flag_unused_explores.csv
    - script_03-flag_unused_views.csv

Output:
    - script_05-dashboards_explores_views_usage.csv
"""

import pandas as pd

# === FILE PATHS ===
DASHBOARDS_PATH = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_02-dashboards_to_views_to_redshift.csv"
EXPLORES_FLAGS_PATH = "script_02-flag_unused_explores.csv"
VIEWS_FLAGS_PATH = "script_03-flag_unused_views.csv"
OUTPUT_PATH = "script_04-dashboards_explores_views_usage.csv"

# === LOAD DATA ===
dash_df = pd.read_csv(DASHBOARDS_PATH)
explore_flags = pd.read_csv(EXPLORES_FLAGS_PATH)
view_flags = pd.read_csv(VIEWS_FLAGS_PATH)

# === CLEAN + NORMALIZE COLUMN NAMES ===
dash_df.columns = dash_df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "").str.replace("-", "_")
explore_flags.columns = explore_flags.columns.str.strip().str.lower()
view_flags.columns = view_flags.columns.str.strip().str.lower()

# === RENAME COLUMNS FOR CONSISTENCY ===
dash_df.rename(columns={
    "dashboard_id_user_defined_only": "dashboard_id",
    "query_model": "model_name",
    "query_explore": "explore_name",
    "view_or_model_name": "view_name"
}, inplace=True)

# === JOIN EXPLORE FLAGS ===
dash_df = dash_df.merge(
    explore_flags[["model_name", "explore_name", "safe_to_deprecate_explore"]],
    on=["model_name", "explore_name"],
    how="left"
)

# === JOIN VIEW FLAGS ===
dash_df = dash_df.merge(
    view_flags[["view_name", "safe_to_deprecate_view"]],
    on="view_name",
    how="left"
)

# === AGGREGATE FLAGS PER DASHBOARD ===
agg = dash_df.groupby(["dashboard_id", "dashboard_title"]).agg({
    "safe_to_deprecate_explore": lambda x: all(x.fillna(False)),
    "safe_to_deprecate_view": lambda x: all(x.fillna(False))
}).reset_index()

agg["safe_to_deprecate_dashboard"] = (
    agg["safe_to_deprecate_explore"] | agg["safe_to_deprecate_view"]
)

# === EXPORT FINAL OUTPUT ===
agg.to_csv(OUTPUT_PATH, index=False)

# === SUMMARY OUTPUT ===
print("\n🎯 Dashboard Deprecation Summary")
print("--------------------------------")
print(f"📊 Total dashboards analyzed: {len(agg)}")
print(f"🧩 Dashboards with ALL deprecated explores: {agg['safe_to_deprecate_explore'].sum()}")
print(f"🔹 Dashboards with ALL deprecated views: {agg['safe_to_deprecate_view'].sum()}")
print(f"🚫 Dashboards safe to deprecate (explore OR view): {agg['safe_to_deprecate_dashboard'].sum()}")
