"""
Description:
    This script merges three datasets to identify Looker explores that are defined
    in the LookML repo but are not used in any dashboard or saved Look.

    It flags each explore with:
        - Whether it is referenced by any dashboard tile
        - Whether it is referenced by any saved Look
        - Whether it is unused and can be considered for deprecation

Inputs:
    - script_01-extracting_looker_explores_from_models.csv
    - raw\dashboard_explore_look_01_system__activity_dashboard_YYYY-MM-DD.csv
    - raw\dashboard_explore_look_02_system__activity_look_YYYY-MM-DD.csv

Output:
    - script_02-flag_unused_explores.csv
"""

import pandas as pd

# === File paths ===
defined_explores_csv = "script_01-extracting_looker_explores_from_models.csv"
dashboard_usage_csv = r"raw\dashboard_explore_look_01_system__activity_dashboard_2025-06-25T1122.csv"
look_usage_csv = r"raw\dashboard_explore_look_02_system__activity look 2025-06-25T1131.csv"
output_csv = "script_02-flag_unused_explores.csv"

# === Load datasets ===
explores_df = pd.read_csv(defined_explores_csv)
dash_df = pd.read_csv(dashboard_usage_csv)
look_df = pd.read_csv(look_usage_csv)

# === Normalize usage datasets ===
dash_df.rename(columns={"Query Explore": "explore_name", "Query Model": "model_name"}, inplace=True)
look_df.rename(columns={"Query Explore": "explore_name", "Query Model": "model_name"}, inplace=True)

dash_df["source"] = "dashboard"
look_df["source"] = "look"

# === Combine and aggregate usage flags ===
usage_df = pd.concat([
    dash_df[["model_name", "explore_name", "source"]],
    look_df[["model_name", "explore_name", "source"]]
], ignore_index=True).drop_duplicates()

# Group by explore and flag usage
usage_flags = (
    usage_df
    .assign(
        used_in_dashboard=lambda df: df["source"] == "dashboard",
        used_in_look=lambda df: df["source"] == "look"
    )
    .groupby(["model_name", "explore_name"])
    .agg({
        "used_in_dashboard": "any",
        "used_in_look": "any"
    })
    .reset_index()
)

usage_flags["is_used_in_either"] = usage_flags["used_in_dashboard"] | usage_flags["used_in_look"]
usage_flags["safe_to_deprecate_explore"] = ~usage_flags["is_used_in_either"]

# === Merge with full explore metadata ===
final_df = explores_df.merge(
    usage_flags,
    on=["model_name", "explore_name"],
    how="left"
).fillna({
    "used_in_dashboard": False,
    "used_in_look": False,
    "is_used_in_either": False,
    "safe_to_deprecate_explore": True
})

# === Save final result ===
final_df.to_csv(output_csv, index=False)

# === Summary ===
print(f"\n✅ Explore deprecation matrix saved to: {output_csv}")
print(f"🔍 Total explores analyzed: {len(final_df)}")
print(f"🚫 Unused explores: {final_df['safe_to_deprecate_explore'].sum()}")
