"""
Description:
    This script recursively scans a Looker project's `looker-master` folder to extract:
        - View definitions and their associated Redshift tables
        - Explore definitions (from model files), along with referenced views and their resolved tables

    Output columns:
        - lkml_file
        - view_or_model_name
        - model_name
        - sql_table_name
        - derived_table_sources

Dependencies:
    pip install sqlparse
"""

import os
import re
import csv
import sqlparse

# === CONFIGURATION ===
LOOKML_ROOT = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_data\looker-master"
OUTPUT_CSV = "script_01-extracting_looker_tables_from_views_and_models.csv"

# === REGEX PATTERNS ===
view_pattern = re.compile(r'view:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
explore_pattern = re.compile(r'explore:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
join_view_pattern = re.compile(r'join:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
from_clause_pattern = re.compile(r'from:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
view_name_override_pattern = re.compile(r'view_name:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
sql_table_pattern = re.compile(r'sql_table_name:\s*["\']?([a-zA-Z0-9_."]+)["\']?', re.IGNORECASE)
derived_sql_pattern = re.compile(r'derived_table\s*:\s*{[^}]*?sql\s*:\s*(.*?)\s*;;', re.DOTALL | re.IGNORECASE)

# === SQL PARSING UTILITIES ===
def extract_table_names_from_sql(sql):
    tables = set()
    pattern = re.compile(r'(?:from|join)\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)))', re.IGNORECASE)
    for match in pattern.findall(sql):
        cleaned = match.strip()
        if is_valid_table(cleaned):
            tables.add(cleaned)
    return sorted(tables)

def is_valid_table(text):
    if "." not in text or text.endswith("."):
        return False
    column_like_suffixes = {
        "created_at", "updated_at", "completed_time", "order_number", "success",
        "short_id", "address", "amount", "rate", "price", "city", "name", "id"
    }
    if text.split(".")[-1].lower() in column_like_suffixes:
        return False
    if any(fn in text.lower() for fn in ["(", ")", "case", "when", "select", "datediff", "coalesce", "greatest", "extract"]):
        return False
    return True

# === PASS 1: Collect View Metadata ===
view_metadata = {}

for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.lower().endswith(".lkml"):
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                rel_path = os.path.relpath(full_path, os.path.dirname(LOOKML_ROOT))

                view_match = view_pattern.search(content)
                if view_match:
                    view_name = view_match.group(1)
                    sql_table_match = sql_table_pattern.search(content)
                    derived_sql_match = derived_sql_pattern.search(content)

                    sql_table_name = sql_table_match.group(1) if sql_table_match else None
                    derived_tables = []

                    if derived_sql_match:
                        derived_sql = derived_sql_match.group(1)
                        derived_tables = extract_table_names_from_sql(derived_sql)

                    view_metadata[view_name] = {
                        "sql_table_name": sql_table_name,
                        "derived_table_sources": ", ".join(derived_tables),
                        "lkml_file": rel_path
                    }

# === PASS 2: Combine View and Explore Results ===
results = []

# First, write the view rows directly
for view_name, meta in view_metadata.items():
    results.append({
        "lkml_file": meta["lkml_file"],
        "view_or_model_name": view_name,
        "model_name": None,
        "sql_table_name": meta["sql_table_name"],
        "derived_table_sources": meta["derived_table_sources"]
    })

# Now, scan again for explore definitions and map their views
for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.lower().endswith(".model.lkml"):
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                rel_path = os.path.relpath(full_path, os.path.dirname(LOOKML_ROOT))
                model_name = file.split(".")[0]

                for explore_match in explore_pattern.finditer(content):
                    explore_name = explore_match.group(1)

                    # Find views in this explore block
                    block_start = explore_match.start()
                    block_end = content.find("explore:", block_start + 1)
                    explore_block = content[block_start:block_end] if block_end != -1 else content[block_start:]

                    base_view = view_name_override_pattern.search(explore_block)
                    base_view_name = base_view.group(1) if base_view else explore_name

                    joined_views = set()
                    for join_match in join_view_pattern.finditer(explore_block):
                        join_name = join_match.group(1)
                        join_block = content[join_match.start():]
                        from_match = from_clause_pattern.search(join_block)
                        joined_view = from_match.group(1) if from_match else join_name
                        joined_views.add(joined_view)

                    all_views = set([base_view_name]) | joined_views

                    # Aggregate all Redshift tables from those views
                    sql_tables = set()
                    derived_sources = set()
                    for view in all_views:
                        if view in view_metadata:
                            if view_metadata[view]["sql_table_name"]:
                                sql_tables.add(view_metadata[view]["sql_table_name"])
                            if view_metadata[view]["derived_table_sources"]:
                                derived_sources.update(view_metadata[view]["derived_table_sources"].split(","))

                    results.append({
                        "lkml_file": rel_path,
                        "view_or_model_name": explore_name,
                        "model_name": model_name,
                        "sql_table_name": ", ".join(sorted(sql_tables)) if sql_tables else None,
                        "derived_table_sources": ", ".join(sorted(set(ds.strip() for ds in derived_sources if ds.strip())))
                    })

# === WRITE TO CSV ===
with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "lkml_file", "view_or_model_name", "model_name", "sql_table_name", "derived_table_sources"
    ])
    writer.writeheader()
    writer.writerows(results)

# === SUMMARY ===
print(f"\n✅ LookML mapping saved to: {OUTPUT_CSV}")
print(f"📄 Total entries parsed (views + explores): {len(results)}")
