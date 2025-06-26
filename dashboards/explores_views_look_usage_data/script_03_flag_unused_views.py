"""
Description:
    This script identifies unused Looker views by analyzing which views
    are defined in .view.lkml files but never referenced in any explore.

    It uses the output from the script_01 from scanning looker-master (dashboards\explores_views_data), which contains both:
        - Views from .view.lkml files
        - Explores from .model.lkml files with their referenced views

    The result is a list of views with flags:
        - Whether the view is used in any explore
        - Whether it is safe to deprecate

Input:
    - script_01-extracting_looker_tables_from_views_and_models.csv

Output:
    - script_03-flag_unused_views.csv
"""

import csv
from collections import defaultdict

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_data\script_01-extracting_looker_tables_from_views_and_models.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []  # All views parsed from view files
explore_view_usage = defaultdict(set)  # view_name → set of explore_names that reference it
all_explore_sql_tables = set()  # Tracks all sql_table_names and derived sources from explores

# === Parse input CSV ===
with open(INPUT_CSV, mode="r", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        file_path = row["lkml_file"]
        view_or_model_name = row["view_or_model_name"]
        model_name = row["model_name"] or ""
        derived_sources = row["derived_table_sources"]
        sql_table_name = row.get("sql_table_name", "")

        if file_path.lower().endswith(".view.lkml"):
            # This is a view definition
            defined_views.append({
                "view_name": view_or_model_name,
                "lkml_file": file_path,
                "sql_table_names": sql_table_name,
                "derived_table_sources": derived_sources
            })

        elif file_path.lower().endswith(".model.lkml"):
            # This is an explore — extract view usage and table lineage
            explore_name = view_or_model_name
            all_views = set()

            # Base view (same as explore name by default)
            all_views.add(explore_name)

            # Join-derived views
            joined_views_raw = derived_sources.split(",") if derived_sources else []
            for ref in joined_views_raw:
                parts = ref.strip().split(".")
                if len(parts) == 2:
                    joined_view = parts[1]
                    if joined_view:
                        all_views.add(joined_view)

                # Also store full derived ref (e.g., schema.table)
                if ref.strip():
                    all_explore_sql_tables.add(ref.strip())

            # Also include direct sql_table_name
            if sql_table_name and sql_table_name.strip():
                all_explore_sql_tables.add(sql_table_name.strip())

            # Map explore → view usage
            for view in all_views:
                explore_view_usage[view].add(explore_name)

# === Flag unused views ===
results = []
for view in defined_views:
    name = view["view_name"]

    view_sql_tables = set()
    if view["sql_table_names"]:
        view_sql_tables.add(view["sql_table_names"].strip())
    if view["derived_table_sources"]:
        derived_split = [s.strip() for s in view["derived_table_sources"].split(",") if s.strip()]
        view_sql_tables.update(derived_split)

    is_referenced_by_table = any(table in all_explore_sql_tables for table in view_sql_tables)
    used_in_explore = (name in explore_view_usage) or is_referenced_by_table
    used_by = sorted(explore_view_usage[name]) if name in explore_view_usage else []

    results.append({
        "view_name": name,
        "lkml_file": view["lkml_file"],
        "used_in_explore": used_in_explore,
        "used_by_explores": ", ".join(used_by),
        "sql_table_names": view["sql_table_names"],
        "derived_table_sources": view["derived_table_sources"],
        "safe_to_deprecate_view": not used_in_explore
    })

# === Write output ===
with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=[
        "view_name",
        "lkml_file",
        "used_in_explore",
        "used_by_explores",
        "sql_table_names",
        "derived_table_sources",
        "safe_to_deprecate_view"
    ])
    writer.writeheader()
    writer.writerows(results)

print(f"\n✅ View usage audit saved to: {OUTPUT_CSV}")
print(f"📄 Total views analyzed: {len(defined_views)}")
print(f"🚫 Unused views: {sum(1 for r in results if not r['used_in_explore'])}")
