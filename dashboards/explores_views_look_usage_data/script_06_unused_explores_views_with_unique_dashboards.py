"""
Description:
    This script merges explore/view usage flags from script_05 into a complete list of dashboards.
    It ensures all dashboards are represented — even if they have no usage records.

Inputs:
    - unique_looker_dashboards.csv           (columns: dashboard_name, dashboard_id)
    - script_05-dashboards_explores_views_usage.csv

Output:
    - script_06_unused_explores_views_with_unique_dashboards.csv
"""

import pandas as pd

# === INPUT / OUTPUT FILES ===
DASHBOARD_LIST = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_look_usage_data\raw\unique_looker_dashboards.csv"
USAGE_FLAGS = "script_05-dashboards_explores_views_usage.csv"
OUTPUT_CSV = "script_06_unused_explores_views_with_unique_dashboards.csv"

# === LOAD DATA ===
df_dash = pd.read_csv(DASHBOARD_LIST)
df_usage = pd.read_csv(USAGE_FLAGS)

# === CLEAN COLUMN NAMES ===
df_dash.columns = df_dash.columns.str.strip().str.lower()
df_usage.columns = df_usage.columns.str.strip().str.lower()

# === AGGREGATE USAGE FLAGS BY DASHBOARD ID ===
agg = df_usage.groupby("dashboard_id_(user-defined_only)").agg(
    has_unused_explores=("safe_to_deprecate_explore", lambda x: any(x == True)),
    has_unused_views=("safe_to_deprecate_view", lambda x: any(x == True)),
    all_explores_unused=("safe_to_deprecate_explore", lambda x: all(x == True)),
    all_views_unused=("safe_to_deprecate_view", lambda x: all(x == True))
).reset_index()

agg["safe_to_deprecate_dashboard"] = agg["all_explores_unused"] & agg["all_views_unused"]

# === CAST ID TYPES FOR MERGING ===
df_dash["dashboard_id"] = df_dash["dashboard_id"].astype(str)
agg["dashboard_id_(user-defined_only)"] = agg["dashboard_id_(user-defined_only)"].astype(str)

# === MERGE TO FULL DASHBOARD LIST ===
merged = df_dash.merge(
    agg,
    how="left",
    left_on="dashboard_id",
    right_on="dashboard_id_(user-defined_only)"
)

# === FLAG NO MATCHES ===
merged["has_usage_match"] = merged["has_unused_explores"].notnull()

# === SAVE OUTPUT ===
merged.to_csv(OUTPUT_CSV, index=False)

# === DIAGNOSTICS ===
print(f"\n✅ Dashboard-level usage flags saved to: {OUTPUT_CSV}")
print(f"📊 Dashboards in source list: {len(df_dash)}")
print(f"🔍 Matched usage records: {merged['has_usage_match'].sum()}")
print(f"🕳️ No usage data: {(~merged['has_usage_match']).sum()}")

# === DUPLICATE CHECK ON INPUT ===
dupes_input = df_dash["dashboard_id"].duplicated().sum()
if dupes_input == 0:
    print("✅ No duplicated dashboard IDs in input (unique dashboard list).")
else:
    print(f"⚠️ Warning: {dupes_input} duplicate dashboard IDs found in your input list!")

