"""
Description:
    This script identifies unused Looker views by analyzing which views
    are defined in .view.lkml files but never referenced in any explore.

    It uses the output from script_01, which scans the looker-master repo
    and contains both view definitions and explore-view relationships.

    It flags each view with:
        - Whether it is referenced in any explore
        - Whether it is safe to deprecate

Input:
    - script_01-extracting_looker_tables_from_views_and_models.csv

Output:
    - script_03-flag_unused_views.csv
"""

import csv
from collections import defaultdict

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_01-extracting_looker_tables_from_views_and_models.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []  # Views defined in .view.lkml files
referenced_views = set()  # All views referenced by explores

# === Parse input CSV ===
with open(INPUT_CSV, mode="r", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        file_path = row["lkml_file"]
        item_name = row["view_or_model_name"]
        derived_sources = row["derived_table_sources"]
        sql_table = row["sql_table_name"]

        if file_path.lower().endswith(".view.lkml"):
            defined_views.append({
                "view_name": item_name,
                "lkml_file": file_path,
                "sql_table_names": sql_table,
                "derived_table_sources": derived_sources
            })

        elif file_path.lower().endswith(".model.lkml"):
            explore_name = item_name
            referenced_views.add(explore_name)  # base view
            if derived_sources:
                for ref in derived_sources.split(","):
                    parts = ref.strip().split(".")
                    if len(parts) == 2:
                        referenced_views.add(parts[1])

# === Flag view usage ===
results = []
for view in defined_views:
    view_name = view["view_name"]
    is_used = view_name in referenced_views
    results.append({
        **view,
        "used_in_explore": is_used,
        "safe_to_deprecate_view": not is_used
    })

# === Write output ===
with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=[
        "view_name",
        "lkml_file",
        "sql_table_names",
        "derived_table_sources",
        "used_in_explore",
        "safe_to_deprecate_view"
    ])
    writer.writeheader()
    writer.writerows(results)

# === Summary ===
print(f"\n✅ View usage audit saved to: {OUTPUT_CSV}")
print(f"📄 Total views analyzed: {len(results)}")
print(f"🚫 Unused views: {sum(1 for r in results if not r['used_in_explore'])}")
