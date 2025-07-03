import csv
import ast
from datetime import datetime

# === File paths ===
INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\script_01-extracting_looker_tables_from_views_and_models.csv"
SYSTEM_ACTIVITY_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_look_usage_data\raw\system__activity_history_2025-07-03T1726.csv"
OUTPUT_CSV = "script_03-flag_unused_views.csv"

# === Containers ===
defined_views = []                    # All defined views from view files
referenced_views = set()             # Views referenced in explores, joins, derived_table, or ${view.field}
system_activity_views = set()        # Views seen in live queries
system_activity_last_used = {}       # View -> Most recent usage timestamp

# === Parse script_01 output CSV ===
with open(INPUT_CSV, mode="r", encoding="utf-8") as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        item_type = row.get("view_or_model_type", "").strip().lower()
        item_name = row["view_or_model_name"].strip()
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

        # Reference logic: explores, joins, view_references
        if item_type in ("explore", "join_view", "view_reference"):
            # From base_view_name if explore
            base_view = row.get("base_view_name", "").strip()
            if base_view:
                referenced_views.add(base_view)

            # Generic reference from the model or view name
            ref_view = item_name
            if ref_view:
                referenced_views.add(ref_view)

            # From derived_table_sources like postgres_agg.distributionpoints
            if derived_sources:
                for source in derived_sources.split(","):
                    source = source.strip()
                    if "." in source:
                        view_part = source.split(".")[0]
                        referenced_views.add(view_part)

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
print(f"📊 Views used in explores/joins/view-refs: {len(referenced_views)}")
print(f"📊 Views used in system activity: {len(system_activity_views)}")
print(f"🚫 Unused views: {sum(1 for r in results if not r['used_in_explore'] and not r['used_in_system_activity'])}")
