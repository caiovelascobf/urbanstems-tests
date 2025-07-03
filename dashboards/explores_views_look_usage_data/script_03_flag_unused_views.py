"""
Description:
    This script identifies unused Looker views by analyzing:
        - LookML explore references
        - Real query usage from System Activity export

    Enhancements:
        - Tracks views defined in .view.lkml files
        - Flags views used in explores (via base_view_name or derived_table_sources)
        - Flags views used in real Looker queries (via system_activity_queries.csv)
        - Adds two separate usage flags for traceability
        - Tracks last used date for views observed in query history

    Inputs:
        - script_01-extracting_looker_tables_from_views_and_models.csv
        - raw/system__activity_history_2025-07-03T1726.csv

    Output:
        - script_03-flag_unused_views.csv
"""

import csv
import ast
from datetime import datetime

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_01-extracting_looker_tables_from_views_and_models.csv"
SYSTEM_ACTIVITY_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_look_usage_data\raw\system__activity_history_2025-07-03T1726.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []                    # All defined views
referenced_views = set()             # Used in explores
system_activity_views = set()        # Used in real queries
system_activity_last_used = {}       # Latest usage date per view

# === Parse INPUT_CSV from script_01 ===
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

            if derived_sources:
                for ref in derived_sources.split(","):
                    parts = ref.strip().split(".")
                    if len(parts) == 2:
                        referenced_views.add(parts[1])

# === Parse SYSTEM_ACTIVITY_CSV ===
with open(SYSTEM_ACTIVITY_CSV, mode="r", encoding="utf-8") as usagefile:
    reader = csv.DictReader(usagefile)
    for row in reader:
        fields_raw = row.get("Query Fields Used", "")
        created_raw = row.get("Query Created Date", "").strip()
        try:
            fields_list = ast.literal_eval(fields_raw)
            query_date = datetime.strptime(created_raw[:10], "%Y-%m-%d") if created_raw else None

            for field in fields_list:
                if "." in field:
                    view_prefix = field.split(".")[0]
                    system_activity_views.add(view_prefix)

                    if query_date:
                        current_last = system_activity_last_used.get(view_prefix)
                        if not current_last or query_date > current_last:
                            system_activity_last_used[view_prefix] = query_date
        except Exception as e:
            print(f"⚠️  Warning: Failed to parse field usage: {fields_raw} — {e}")

# === Flag Usage ===
results = []
for view in defined_views:
    view_name = view["view_name"]
    is_used_in_explore = view_name in referenced_views
    is_used_in_system = view_name in system_activity_views
    is_used = is_used_in_explore or is_used_in_system

    results.append({
        **view,
        "used_in_explore": is_used_in_explore,
        "used_in_system_activity": is_used_in_system,
        "last_used_in_system_activity": system_activity_last_used.get(view_name, "").strftime("%Y-%m-%d") if view_name in system_activity_last_used else "",
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
        "used_in_system_activity",
        "last_used_in_system_activity",
        "safe_to_deprecate_view"
    ])
    writer.writeheader()
    writer.writerows(results)

# === Summary ===
print(f"\n✅ View usage audit saved to: {OUTPUT_CSV}")
print(f"📄 Total views analyzed: {len(results)}")
print(f"📊 Views used in explores: {len(referenced_views)}")
print(f"📊 Views used in system activity: {len(system_activity_views)}")
print(f"🚫 Unused views: {sum(1 for r in results if not r['used_in_explore'] and not r['used_in_system_activity'])}")
