"""
Description:
    This script identifies unused Looker views by analyzing:

        - View definitions collected from .view.lkml files
        - View usage in explores (base_view_name)
        - View usage in joins (join_view)
        - Field-level references to other views (${other_view.field})
        - Derived table SQL sources (e.g., postgres_agg.viewname)
        - Live query usage from Looker System Activity exports

    It produces a final report flagging which views are used or unused,
    along with system activity timestamps for traceability.

Key Features:
    - Captures all possible reference types to a view from LookML content
    - Tracks live usage based on the “Query Fields Used” from System Activity
    - Adds `safe_to_deprecate_view` flag for views unused in both LookML and queries
    - Uses deduplicated results from upstream `script_01` output

Inputs:
    - script_01-extracting_looker_tables_from_views_and_models.csv
        (Generated from the full LookML scan)
    - system__activity_history_*.csv
        (System Activity export from Looker, including “Query Fields Used”)

Output:
    - script_03-flag_unused_views.csv
        With the following columns:
            - view_name
            - lkml_file
            - sql_table_names
            - derived_table_sources
            - used_in_explore
            - used_in_system_activity
            - last_used_in_system_activity
            - safe_to_deprecate_view

Usage:
    - Ensure both input files are available and correctly referenced
    - Run this script after `script_01` has been executed
    - Review `safe_to_deprecate_view` column for cleanup decisions
"""

import csv
import ast
from datetime import datetime

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_01-extracting_looker_tables_from_views_and_models.csv"
SYSTEM_ACTIVITY_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_look_usage_data\raw\system__activity_history_2025-07-03T1726.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []                    # All defined views from .view.lkml files
referenced_views = set()             # Views referenced via explore, join_view, view_reference, or derived_table_sources
system_activity_views = set()        # Views seen in real Looker queries
system_activity_last_used = {}       # View name -> most recent usage date

# === Parse script_01 output CSV ===
with open(INPUT_CSV, mode="r", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        item_type = row.get("view_or_model_type", "").strip().lower()
        view_name = row["view_or_model_name"].strip()
        base_view = row.get("base_view_name", "").strip()
        derived_sources = row.get("derived_table_sources", "")
        file_path = row["lkml_file"]
        sql_table = row.get("sql_table_name", "")

        # Track defined views
        if item_type == "view":
            defined_views.append({
                "view_name": view_name,
                "lkml_file": file_path,
                "sql_table_names": sql_table,
                "derived_table_sources": derived_sources
            })

        # Track referenced views from explores, joins, and view references
        if item_type in ("explore", "join_view", "view_reference"):
            if base_view:
                referenced_views.add(base_view)
            if view_name:
                referenced_views.add(view_name)

        # Track views referenced in derived_table_sources (corrected)
        if derived_sources:
            for source in derived_sources.split(","):
                source = source.strip()
                if "." in source:
                    try:
                        _, view_ref = source.split(".", 1)
                        referenced_views.add(view_ref.strip())
                    except ValueError:
                        continue

# === Parse System Activity CSV ===
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
                    view_prefix = field.split(".")[0].strip()
                    system_activity_views.add(view_prefix)

                    if query_date:
                        current_last = system_activity_last_used.get(view_prefix)
                        if not current_last or query_date > current_last:
                            system_activity_last_used[view_prefix] = query_date
        except Exception as e:
            print(f"⚠️  Warning: Failed to parse field usage: {fields_raw} — {e}")

# === Flag usage status ===
results = []
for view in defined_views:
    name = view["view_name"]
    used_in_explore = name in referenced_views
    used_in_system = name in system_activity_views
    last_used = system_activity_last_used.get(name, "")
    results.append({
        **view,
        "used_in_explore": used_in_explore,
        "used_in_system_activity": used_in_system,
        "last_used_in_system_activity": last_used.strftime("%Y-%m-%d") if last_used else "",
        "safe_to_deprecate_view": not (used_in_explore or used_in_system)
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
print(f"📊 Views used in explores/joins/view-refs: {len(referenced_views)}")
print(f"📊 Views used in system activity: {len(system_activity_views)}")
print(f"🚫 Unused views: {sum(1 for r in results if not r['used_in_explore'] and not r['used_in_system_activity'])}")
