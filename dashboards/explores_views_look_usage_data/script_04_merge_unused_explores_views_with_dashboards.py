"""
Description:
    This script merges:
    - Dashboard lineage (explore + view usage per dashboard)
    - Explore usage flags
    - View usage flags

    Enhancements:
        - Uses base_view_name for accurate view tracking
        - Drops rows with missing model/explore/view names
        - Adds debug diagnostics for join mismatches
        - Provides both OR and AND logic for safe_to_deprecate_dashboard

Inputs:
    - script_02-dashboards_to_views_to_redshift.csv
    - script_02-flag_unused_explores.csv
    - script_03-flag_unused_views.csv

Output:
    - script_04-dashboards_explores_views_usage.csv
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

# === CLEAN COLUMN NAMES ===
dash_df.columns = (
    dash_df.columns.str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("(", "")
    .str.replace(")", "")
    .str.replace("-", "_")
)
explore_flags.columns = explore_flags.columns.str.strip().str.lower()
view_flags.columns = view_flags.columns.str.strip().str.lower()

# === RENAME FOR CONSISTENCY ===
dash_df.rename(columns={
    "dashboard_id_user_defined_only": "dashboard_id",
    "query_model": "model_name",
    "query_explore": "explore_name",
    "base_view_name": "view_name"  # use base_view_name instead of view_or_model_name
}, inplace=True)

# === DROP NULL OR EMPTY KEYS BEFORE JOIN ===
required_cols = ["model_name", "explore_name", "view_name"]
dash_df.dropna(subset=required_cols, inplace=True)
for col in required_cols:
    dash_df = dash_df[dash_df[col].astype(str).str.strip() != ""]

# === NORMALIZE JOIN FIELDS ===
for df in [dash_df, explore_flags]:
    df["explore_name"] = df["explore_name"].astype(str).str.strip().str.lower()
    df["model_name"] = df["model_name"].astype(str).str.strip().str.lower()
dash_df["view_name"] = dash_df["view_name"].astype(str).str.strip().str.lower()
view_flags["view_name"] = view_flags["view_name"].astype(str).str.strip().str.lower()

# === JOIN EXPLORE FLAGS ===
dash_df = dash_df.merge(
    explore_flags[["model_name", "explore_name", "safe_to_deprecate_explore"]],
    on=["model_name", "explore_name"],
    how="left"
)
print("🔍 Missing explore flag matches:", dash_df["safe_to_deprecate_explore"].isna().sum())

# === JOIN VIEW FLAGS ===
dash_df = dash_df.merge(
    view_flags[["view_name", "safe_to_deprecate_view"]],
    on="view_name",
    how="left"
)
print("🔍 Missing view flag matches:", dash_df["safe_to_deprecate_view"].isna().sum())

# === OPTIONAL: Show top unmatched items
unmatched_explores = dash_df[dash_df["safe_to_deprecate_explore"].isna()][["model_name", "explore_name"]].drop_duplicates()
print("🚫 Top unmatched explores:\n", unmatched_explores.head())

unmatched_views = dash_df[dash_df["safe_to_deprecate_view"].isna()][["view_name"]].drop_duplicates()
print("🚫 Top unmatched views:\n", unmatched_views.head())

# === AGGREGATE FLAGS PER DASHBOARD ===
agg = dash_df.groupby(["dashboard_id", "dashboard_title"]).agg({
    "safe_to_deprecate_explore": lambda x: all(x.fillna(False)),
    "safe_to_deprecate_view": lambda x: all(x.fillna(False))
}).reset_index()

# === Dashboard deprecation logic (OR vs AND)
agg["safe_to_deprecate_dashboard_or"] = (
    agg["safe_to_deprecate_explore"] | agg["safe_to_deprecate_view"]
)

agg["safe_to_deprecate_dashboard_and"] = (
    agg["safe_to_deprecate_explore"] & agg["safe_to_deprecate_view"]
)

# === EXPORT FINAL RESULT
agg.to_csv(OUTPUT_PATH, index=False)

# === SUMMARY OUTPUT ===
print("\n🎯 Dashboard Deprecation Summary")
print("--------------------------------")
print(f"📊 Total dashboards analyzed: {len(agg)}")
print(f"🧩 ALL explores deprecated: {agg['safe_to_deprecate_explore'].sum()}")
print(f"📚 ALL views deprecated: {agg['safe_to_deprecate_view'].sum()}")
print(f"🚫 Dashboards deprecated (OR logic): {agg['safe_to_deprecate_dashboard_or'].sum()}")
print(f"🛡️ Dashboards deprecated (AND logic): {agg['safe_to_deprecate_dashboard_and'].sum()}")
