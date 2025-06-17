import os
import re
import pandas as pd

# === CONFIGURATION ===
LOOKML_ROOT = r"C:\jobs_repo\brainforge\urbanstems-tests\dbt_models_mapping\looker-master"
DASHBOARD_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\dbt_models_mapping\dashboard_models_system__activity_dashboard_2025-06-16T1959.csv"
OUTPUT_CSV = "dashboard_to_table_mapping.csv"

# === LOAD DASHBOARD CSV ===
dashboards = pd.read_csv(DASHBOARD_CSV)
dashboards.columns = dashboards.columns.str.strip().str.lower().str.replace(" ", "_")

# === STEP 1: Parse all model.lkml files to get explore -> from: view mappings ===
explore_to_view_map = {}

for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".model.lkml"):
            full_path = os.path.join(root, file)
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

                # Find all explore: name { from: other_name }
                explore_blocks = re.findall(r'explore:\s*([a-zA-Z0-9_]+)\s*{[^}]*?from:\s*([a-zA-Z0-9_]+)', content)
                for explore_name, from_view in explore_blocks:
                    explore_to_view_map[explore_name] = from_view

# === STEP 2: Parse all .lkml files to get view + sql_table_name ===
lookml_entries = []
total_lkml_files = 0

for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".lkml"):
            total_lkml_files += 1
            full_path = os.path.join(root, file)
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

                view_match = re.search(r'view:\s*["\']?([a-zA-Z0-9_]+)["\']?', content)
                sql_match = re.search(r'sql_table_name:\s*["\']?([a-zA-Z0-9_.]+)["\']?', content)

                if view_match:
                    lookml_entries.append({
                        "view_name": view_match.group(1),
                        "sql_table_name": sql_match.group(1) if sql_match else None,
                        "lkml_file": os.path.relpath(full_path, LOOKML_ROOT)
                    })

lookml_df = pd.DataFrame(lookml_entries)

# === STEP 3: Resolve dashboards.query_explore using explore_to_view_map ===
dashboards["resolved_explore"] = dashboards["query_explore"].map(explore_to_view_map).fillna(dashboards["query_explore"])

# === STEP 4: Merge dashboards with LookML views ===
merged = dashboards.merge(lookml_df, how="left", left_on="resolved_explore", right_on="view_name")

# === STEP 5: Output selected columns ===
output_df = merged[[
    "dashboard_id_(user-defined_only)", 
    "dashboard_title", 
    "lkml_file", 
    "query_explore", 
    "query_model", 
    "sql_table_name"
]].rename(columns={
    "dashboard_id_(user-defined_only)": "dashboard_id"
})

output_df.to_csv(OUTPUT_CSV, index=False)

# === SUMMARY ===
print(f"\n✅ Full mapping saved to: {OUTPUT_CSV}")
print(f"\n📄 Total .lkml files found: {total_lkml_files}")
print(f"🔗 Explore-to-view mappings found: {len(explore_to_view_map)}")
print(f"📊 Valid views parsed: {len(lookml_df)}")
print(f"📈 Dashboards mapped: {len(output_df)}")
