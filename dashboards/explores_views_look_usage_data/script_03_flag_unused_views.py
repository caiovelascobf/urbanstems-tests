"""
Description:
    This script identifies unused Looker views by analyzing which views
    are defined in .view.lkml files but never referenced in any explore.

    Enhancements:
        - Uses view_or_model_type == "view" to identify defined views
        - Uses base_view_name to identify actual view references from explores
        - Maintains detection of derived_table_sources as indirect view references

    Input:
        - script_01-extracting_looker_tables_from_views_and_models.csv

    Output:
        - script_03-flag_unused_views.csv
"""

import csv

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_01-extracting_looker_tables_from_views_and_models.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []        # Views defined in LookML
referenced_views = set()  # Views referenced by explores

# === Parse input CSV ===
with open(INPUT_CSV, mode="r", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        item_type = row.get("view_or_model_type", "").strip().lower()
        item_name = row["view_or_model_name"]
        file_path = row["lkml_file"]
        sql_table = row.get("sql_table_name", "")
        derived_sources = row.get("derived_table_sources", "")

        if item_type == "view":
            defined_views.append({
                "view_name": item_name,
                "lkml_file": file_path,
                "sql_table_names": sql_table,
                "derived_table_sources": derived_sources
            })

        elif item_type == "explore":
            base_view = row.get("base_view_name", "").strip()
            if base_view:
                referenced_views.add(base_view)

            # Add any derived views referenced by this explore
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

# === Write output CSV ===
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
