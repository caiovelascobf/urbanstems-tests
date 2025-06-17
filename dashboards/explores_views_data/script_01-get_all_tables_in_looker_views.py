"""
Script: lookml_table_mapping_extractor.py

Description:
    This script recursively scans a Looker project's `looker-master` folder to extract
    the mapping between LookML views and their associated Redshift tables. It identifies:
        - Direct SQL table references via `sql_table_name`
        - Derived table sources via `derived_table { sql: ... }` by parsing FROM and JOIN clauses
        - View name and source .lkml file path

    The output is written to a CSV with the following columns:
        - lkml_file          (relative file path)
        - view_name
        - sql_table_name
        - derived_table_sources

Dependencies:
    pip install sqlparse

Usage:
    - Set the LOOKML_ROOT path to the root of your Looker repo (where .lkml files live).
    - Run the script to generate `looker_views_and_its_tables_mapping.csv` in the current directory.
"""

import os
import re
import csv
import sqlparse

# === CONFIGURATION ===
LOOKML_ROOT = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_data\looker-master"
OUTPUT_CSV = "script_01-looker_views_and_its_tables_mapping.csv"

# === REGEX FOR LOOKML BLOCKS ===
view_pattern = re.compile(r'view:\s*["\']?([a-zA-Z0-9_]+)["\']?', re.IGNORECASE)
sql_table_pattern = re.compile(r'sql_table_name:\s*["\']?([a-zA-Z0-9_."]+)["\']?', re.IGNORECASE)
derived_sql_pattern = re.compile(r'derived_table\s*:\s*{[^}]*?sql\s*:\s*(.*?)\s*;;', re.DOTALL | re.IGNORECASE)

# === SQL PARSING UTILITIES ===

def extract_table_names_from_sql(sql):
    """
    Robust parser: extracts fully qualified table names from FROM and JOIN clauses,
    including quoted identifiers and multi-word table names. Preserves quotes for accuracy.
    """
    tables = set()

    # Match FROM or JOIN followed by quoted or unquoted schema + table (ignore aliases)
    pattern = re.compile(
        r'(?:from|join)\s+((?:"[^"]+"|\w+)(?:\.(?:"[^"]+"|\w+)))',
        re.IGNORECASE
    )

    for match in pattern.findall(sql):
        cleaned = match.strip()
        if is_valid_table(cleaned):
            tables.add(cleaned)

    return sorted(tables)


def normalize_table(text):
    text = text.strip().split()[0]
    return text.replace('"', '').replace('`', '').replace('[', '').replace(']', '')

def is_valid_table(text):
    # Must contain a dot, and not end with a dot
    if "." not in text or text.endswith("."):
        return False

    # Filter out likely column names (e.g. tasks.completed_time, foo.created_at)
    column_like_suffixes = [
        "created_at", "updated_at", "completed_time", "order_number", "success",
        "short_id", "address", "amount", "rate", "price", "city", "name", "id"
    ]

    # If the right side (after last dot) matches column-like names, discard
    if text.split(".")[-1].lower() in column_like_suffixes:
        return False

    # Also filter common function patterns
    if any(fn in text.lower() for fn in [
        "(", ")", "case", "when", "select", "datediff", "coalesce", "greatest", "extract"
    ]):
        return False

    return True


# === COLLECT RESULTS ===
results = []
for root, _, files in os.walk(LOOKML_ROOT):
    for file in files:
        if file.endswith(".lkml"):
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()

                # Match view and possible table references
                view_match = view_pattern.search(content)
                if not view_match:
                    continue

                view_name = view_match.group(1)
                sql_table_match = sql_table_pattern.search(content)
                derived_sql_match = derived_sql_pattern.search(content)

                sql_table_name = sql_table_match.group(1) if sql_table_match else None
                derived_tables = []

                # Process derived SQL for FROM/JOIN table references
                if derived_sql_match:
                    derived_sql = derived_sql_match.group(1)
                    derived_tables = extract_table_names_from_sql(derived_sql)

                results.append({
                    "lkml_file": os.path.relpath(full_path, os.path.dirname(LOOKML_ROOT)),  # includes "looker-master/"
                    "view_name": view_name,
                    "sql_table_name": sql_table_name,
                    "derived_table_sources": ", ".join(derived_tables)
                })

# === WRITE TO CSV ===
with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["lkml_file", "view_name", "sql_table_name", "derived_table_sources"])
    writer.writeheader()
    writer.writerows(results)

# === SUMMARY ===
print(f"\n✅ LookML view mapping saved to: {OUTPUT_CSV}")
print(f"📄 Total view files parsed: {len(results)}")
