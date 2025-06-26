"""
Description:
    This script cross-references explore and view usage by:
    - Extracting explore-to-view dependencies from script_01
    - Merging usage flags from script_02 (explores) and script_03 (views)
    - Outputting a detailed dependency + deprecation matrix, including lkml_file

Inputs:
- script_01-extracting_looker_tables_from_views_and_models.csv
- script_02-flag_unused_explores.csv
- script_03-flag_unused_views.csv

Output:
- script_04-flag_unused_explores_and_views.csv
"""

import csv

# === FILE PATHS ===
SCRIPT_01 = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_data\script_01-extracting_looker_tables_from_views_and_models.csv"
SCRIPT_02 = "script_02-flag_unused_explores.csv"
SCRIPT_03 = "script_03-flag_unused_views.csv"
OUTPUT_CSV = "script_04-flag_unused_explores_and_views.csv"

# === LOAD EXPLORE USAGE FLAGS ===
explore_usage = {}
with open(SCRIPT_02, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row["model_name"], row["explore_name"])
        explore_usage[key] = {
            "used_in_dashboard": row["used_in_dashboard"].lower() == "true",
            "used_in_look": row["used_in_look"].lower() == "true",
            "is_used_in_either": row["is_used_in_either"].lower() == "true"
        }

# === LOAD VIEW USAGE FLAGS ===
view_usage = {}
with open(SCRIPT_03, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        view_name = row["view_name"]
        view_usage[view_name] = row["used_in_explore"].lower() == "true"

# === LOAD EXPLORE-VIEW DEPENDENCIES AND LKML FILES FROM SCRIPT_01 ===
explore_view_map = []  # list of (model_name, explore_name, view_name, lkml_file)
with open(SCRIPT_01, mode="r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["lkml_file"].endswith(".model.lkml"):
            model = row["model_name"]
            explore = row["view_or_model_name"]
            lkml_file = row["lkml_file"]
            views = set()

            # Always include the base explore name
            if model and explore:
                views.add(explore)

            # Parse derived table source views
            if row["derived_table_sources"]:
                derived_views = [v.strip() for v in row["derived_table_sources"].split(",") if v.strip()]
                views.update(derived_views)

            for view in views:
                explore_view_map.append((model, explore, view, lkml_file))

# === COMBINE EVERYTHING INTO FINAL MATRIX ===
final_rows = []
for model, explore, view, lkml_file in explore_view_map:
    exp_flags = explore_usage.get((model, explore), {
        "used_in_dashboard": False,
        "used_in_look": False,
        "is_used_in_either": False
    })
    view_flag = view_usage.get(view, False)

    final_rows.append({
        "lkml_file": lkml_file,
        "model_name": model,
        "explore_name": explore,
        "view_name": view,
        "explore_used_in_dashboard": exp_flags["used_in_dashboard"],
        "explore_used_in_look": exp_flags["used_in_look"],
        "is_used_in_either": exp_flags["is_used_in_either"],
        "view_used_in_explore": view_flag,
        "safe_to_deprecate_explore": not exp_flags["is_used_in_either"],
        "safe_to_deprecate_view": not view_flag
    })

# === WRITE OUTPUT CSV ===
with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
    fieldnames = [
        "lkml_file",
        "model_name",
        "explore_name",
        "view_name",
        "explore_used_in_dashboard",
        "explore_used_in_look",
        "is_used_in_either",
        "view_used_in_explore",
        "safe_to_deprecate_explore",
        "safe_to_deprecate_view"
    ]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(final_rows)

# === SUMMARY ===
print(f"\n✅ Explore-view matrix saved to: {OUTPUT_CSV}")
print(f"🔍 Total explore-view pairs: {len(final_rows)}")
