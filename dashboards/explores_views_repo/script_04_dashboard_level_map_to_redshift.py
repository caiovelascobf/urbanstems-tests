"""
Description:
    This script maps a list of dashboard titles to the Redshift tables used in them,
    by joining against a pre-processed mapping of dashboards to redshift_table values.

Inputs:
    - script_01-dashboard_list.csv
        A one-column CSV (no header), each row is a dashboard title

    - script_02-dashboards_to_views_to_redshift.csv
        Must contain columns:
            - dashboard_title
            - redshift_tables

Output:
    - script_04-dashboards_with_redshift_tables.csv
        Columns:
            - dashboard_title
            - redshift_tables
"""

import pandas as pd

# === INPUT FILES ===
DASHBOARD_LIST_CSV = r"raw\script_01-dashboard_list.csv"
DASHBOARD_MAPPING_CSV = "script_02-dashboards_to_views_to_redshift.csv"
OUTPUT_CSV = "script_04-dashboards_to_redshift_tables.csv"

# === LOAD DATA ===
dashboard_list = pd.read_csv(DASHBOARD_LIST_CSV, header=None, names=["dashboard_title"])
dashboard_mapping = pd.read_csv(DASHBOARD_MAPPING_CSV)

# === CLEAN COLUMN NAMES ===
dashboard_mapping.columns = dashboard_mapping.columns.str.strip().str.lower()
dashboard_list["dashboard_title"] = dashboard_list["dashboard_title"].str.strip()

# === GROUP redshift_tables BY DASHBOARD TITLE ===
table_lookup = dashboard_mapping.groupby("dashboard_title")["redshift_tables"] \
    .apply(lambda x: ", ".join(sorted(set(filter(pd.notnull, x))))).to_dict()

# === MAP TO INPUT LIST ===
dashboard_list["redshift_tables"] = dashboard_list["dashboard_title"].map(table_lookup)

# === EXPORT ===
dashboard_list.to_csv(OUTPUT_CSV, index=False)

# === SUMMARY ===
print(f"\n✅ Dashboard to Redshift table mapping saved to: {OUTPUT_CSV}")
print(f"📊 Dashboards in input list: {len(dashboard_list)}")
print(f"🔗 Dashboards matched with Redshift tables: {dashboard_list['redshift_tables'].notnull().sum()}")
