"""
Description:
    This script recursively scans a Looker project's `looker-master` folder to extract:
        - View definitions and their associated Redshift tables
        - Explore definitions (from model files), along with referenced views
        - Explicitly captures joined views (join: xyz), even when `from:` is omitted
        - Captures nested joins and references across `.view.lkml` files
        - Captures field references like ${other_view.field} in LookML definitions

    Enhancements:
        - Classifies entries as 'view', 'explore', 'join_view', or 'view_reference'
        - Ensures any reference to a view in any `.lkml` file is captured (even if already defined)
        - Deduplicates references per (view name, file)
        - Accurately parses derived_table SQL for source tables (e.g., postgres_agg.distributionpoints)

    Output columns:
        - view_or_model_type        ("view", "explore", "join_view", "view_reference")
        - view_or_model_name        (view name or explore name)
        - model_name                (for explores and joins)
        - base_view_name            (for explores, the defined base view)
        - lkml_file                 (relative path to the .lkml file)
        - sql_table_name            (if view has direct table)
        - derived_table_sources     (if view contains derived tables)
"""

import os
import re
import csv

# === CONFIGURATION ===
LOOKML_ROOT = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\looker-master"
OUTPUT_CSV = "script_01-extracting_looker_tables_from_views_and_models.csv"

# === REGEX PATTERNS ===
view_pattern = re.compile(r'view:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
explore_pattern = re.compile(r'explore:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
join_pattern = re.compile(r'join:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
from_pattern = re.compile(r'from:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
view_name_override = re.compile(r'view_name:\s*["\']?([\w\-]+)["\']?', re.IGNORECASE)
sql_table_pattern = re.compile(r'sql_table_name:\s*["\']?([\w\.\[\]"]+)["\']?', re.IGNORECASE)
derived_sql_pattern = re.compile(r'derived_table\s*:\s*{[^}]*?sql\s*:\s*(.*?)\s*;;', re.DOTALL | re.IGNORECASE)
field_ref_pattern = re.compile(r'\$\{\s*([\w\-]+)\.', re.IGNORECASE)

# === SQL Helpers ===
def extract_table_names_from_sql(sql):
    tables = set()
    pattern = re.compile(r'(?:from|join)\s+((?:"[^"]+"|\w+)\.(?:"[^"]+"|\w+))', re.IGNORECASE)
    for match in pattern.findall(sql):
        if "." in match and not match.lower().endswith((".", "id")):
            tables.add(match.strip())
    return sorted(tables)

# === Storage ===
results = []
view_metadata = {}
seen_view_references = set()  # Track (view_name, file) to prevent duplication

# === Pass 1: Collect all views ===
for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".lkml"):
            path = os.path.join(root, file)
            rel_path = os.path.relpath(path, os.path.dirname(LOOKML_ROOT))

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

                # Is this a view file?
                if view_match := view_pattern.search(content):
                    view_name = view_match.group(1)
                    sql_table_match = sql_table_pattern.search(content)
                    derived_sql_match = derived_sql_pattern.search(content)

                    sql_table = sql_table_match.group(1) if sql_table_match else None
                    derived_sources = extract_table_names_from_sql(derived_sql_match.group(1)) if derived_sql_match else []

                    results.append({
                        "view_or_model_type": "view",
                        "view_or_model_name": view_name,
                        "model_name": None,
                        "base_view_name": None,
                        "lkml_file": rel_path,
                        "sql_table_name": sql_table,
                        "derived_table_sources": ", ".join(derived_sources)
                    })

                    view_metadata[view_name] = {
                        "sql_table_name": sql_table,
                        "derived_table_sources": ", ".join(derived_sources),
                        "lkml_file": rel_path
                    }

                # Collect references to other views from ${view.field} patterns
                referenced_views = set(field_ref_pattern.findall(content))
                for ref_view in referenced_views:
                    key = (ref_view, rel_path)
                    if key not in seen_view_references:
                        seen_view_references.add(key)
                        results.append({
                            "view_or_model_type": "view_reference",
                            "view_or_model_name": ref_view,
                            "model_name": None,
                            "base_view_name": None,
                            "lkml_file": rel_path,
                            "sql_table_name": None,
                            "derived_table_sources": None
                        })

# === Pass 2: Extract explores + join views ===
for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".model.lkml"):
            path = os.path.join(root, file)
            rel_path = os.path.relpath(path, os.path.dirname(LOOKML_ROOT))
            model_name = file.split(".")[0]

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

                for explore_match in explore_pattern.finditer(content):
                    explore_name = explore_match.group(1)
                    block_start = explore_match.start()
                    block_end = content.find("explore:", block_start + 1)
                    explore_block = content[block_start:block_end] if block_end != -1 else content[block_start:]

                    base_view = view_name_override.search(explore_block)
                    base_view_name = base_view.group(1) if base_view else None

                    results.append({
                        "view_or_model_type": "explore",
                        "view_or_model_name": explore_name,
                        "model_name": model_name,
                        "base_view_name": base_view_name,
                        "lkml_file": rel_path,
                        "sql_table_name": None,
                        "derived_table_sources": None
                    })

                    for join_match in join_pattern.finditer(explore_block):
                        join_name = join_match.group(1)
                        join_block_start = join_match.start()
                        join_block_end = content.find("join:", join_block_start + 1)
                        join_block = content[join_block_start:join_block_end] if join_block_end != -1 else content[join_block_start:]
                        from_match = from_pattern.search(join_block)
                        resolved_view = from_match.group(1) if from_match else join_name

                        results.append({
                            "view_or_model_type": "join_view",
                            "view_or_model_name": resolved_view,
                            "model_name": model_name,
                            "base_view_name": explore_name,
                            "lkml_file": rel_path,
                            "sql_table_name": None,
                            "derived_table_sources": None
                        })

# === Write to CSV ===
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "view_or_model_type", "view_or_model_name", "model_name", "base_view_name",
        "lkml_file", "sql_table_name", "derived_table_sources"
    ])
    writer.writeheader()
    writer.writerows(results)

print(f"✅ LookML mapping saved to: {OUTPUT_CSV}")
print(f"📄 Total rows (views, explores, joins, refs): {len(results)}")
