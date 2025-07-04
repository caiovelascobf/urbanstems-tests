"""
Description:
    This script recursively scans a Looker project's `looker-master` folder to extract:
        - View definitions and their associated tables (`sql_table_name`)
        - Derived tables' SQL sources (`derived_table.sql`)
        - Explore definitions from `.model.lkml` files
        - Join aliases used in explores (`join: name { from: some_view }`)
        - `${other_view.field}` references from any LookML file
        - Explore-level `from:` references (if `view_name:` is not explicitly defined)

    Types:
        - "view": actual LookML view
        - "explore": explore block, referencing a base view (via `view_name:` or `from:`)
        - "join_view": join alias in an explore (even if from: another view)
        - "view_reference": cross-view reference from `${other_view.field}`

Output Columns:
    - view_or_model_type        ("view", "explore", "join_view", "view_reference")
    - view_or_model_name        (view or explore name)
    - model_name                (for explores and joins)
    - base_view_name            (explore's base view if known)
    - lkml_file                 (relative path to the file)
    - sql_table_name            (only for view rows)
    - derived_table_sources     (only for view rows)
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

def extract_table_names_from_sql(sql):
    tables = set()
    pattern = re.compile(r'(?:from|join)\s+((?:"[^"]+"|\w+)\.(?:"[^"]+"|\w+))', re.IGNORECASE)
    for match in pattern.findall(sql):
        if "." in match and not match.lower().endswith((".", "id")):
            tables.add(match.strip())
    return sorted(tables)

# === Storage ===
results = []
seen_view_references = set()
seen_join_aliases = set()

# === Pass 1: View definitions and ${view.field} references ===
for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".lkml"):
            path = os.path.join(root, file)
            rel_path = os.path.relpath(path, os.path.dirname(LOOKML_ROOT))

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

                # --- View definition ---
                if view_match := view_pattern.search(content):
                    view_name = view_match.group(1)
                    sql_table = sql_table_pattern.search(content)
                    derived_sql = derived_sql_pattern.search(content)

                    results.append({
                        "view_or_model_type": "view",
                        "view_or_model_name": view_name,
                        "model_name": None,
                        "base_view_name": None,
                        "lkml_file": rel_path,
                        "sql_table_name": sql_table.group(1) if sql_table else None,
                        "derived_table_sources": ", ".join(extract_table_names_from_sql(derived_sql.group(1))) if derived_sql else None
                    })

                # --- ${view.field} references ---
                for ref_view in set(field_ref_pattern.findall(content)):
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

# === Pass 2: Explore + Join blocks ===
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
                    explore_start = explore_match.start()
                    explore_end = content.find("explore:", explore_start + 1)
                    explore_block = content[explore_start:explore_end] if explore_end != -1 else content[explore_start:]

                    # Get base view from view_name: or fallback to from:
                    base_view_match = view_name_override.search(explore_block)
                    base_view_name = base_view_match.group(1) if base_view_match else None

                    if not base_view_name:
                        from_match = from_pattern.search(explore_block)
                        base_view_name = from_match.group(1) if from_match else None

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
                        join_alias = join_match.group(1)
                        join_start = join_match.start()
                        join_end = explore_block.find("join:", join_start + 1)
                        join_block = explore_block[join_start:join_end] if join_end != -1 else explore_block[join_start:]

                        from_match = from_pattern.search(join_block)
                        from_view = from_match.group(1) if from_match else join_alias

                        dedup_key = (join_alias, model_name, explore_name, rel_path)
                        if dedup_key not in seen_join_aliases:
                            seen_join_aliases.add(dedup_key)
                            results.append({
                                "view_or_model_type": "join_view",
                                "view_or_model_name": from_view,
                                "model_name": model_name,
                                "base_view_name": explore_name,
                                "lkml_file": rel_path,
                                "sql_table_name": None,
                                "derived_table_sources": None
                            })

# === Deduplicate rows based on all column values ===
unique_results = [dict(t) for t in {tuple(sorted(d.items())) for d in results}]

# === Write Output ===
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "view_or_model_type", "view_or_model_name", "model_name", "base_view_name",
        "lkml_file", "sql_table_name", "derived_table_sources"
    ])
    writer.writeheader()
    writer.writerows(unique_results)

print(f"✅ LookML mapping saved to: {OUTPUT_CSV}")
print(f"📄 Total unique rows (views, explores, joins, refs): {len(unique_results)}")
