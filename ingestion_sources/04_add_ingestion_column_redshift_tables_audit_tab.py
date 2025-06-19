"""

This script enriches a Redshift query log by adding an 'Ingestion Tool' column.
It performs a schema-level join between Redshift usage data and ingestion metadata.

Steps performed:
1. Loads Redshift usage data and ingestion table metadata from CSV files.
2. Normalizes schema name columns for consistent, case-insensitive matching.
3. Maps each Redshift schema to its associated ingestion tool.
4. Adds a new 'Ingestion Tool' column to the Redshift dataset.
5. Saves the enriched dataset to a new output CSV file.

Inputs:
    - raw\\brainforge_redshift_audit_tab_google_sheet.csv  (Redshift usage data)
    - raw\\brainforge_ingestion_tables_audit_tab_google_sheet.csv  (Table-level Ingestion metadata)

Output:
    - 04_added_ingestion_column_redshift_tables_audit_tab.csv (enriched dataset)

"""

import pandas as pd

# === Input files
QUERY_CSV = r"raw\brainforge_redshift_audit_tab_google_sheet.csv"
INGESTION_CSV = r"raw\brainforge_ingestion_tables_audit_tab_google_sheet.csv"
OUTPUT_CSV = "04_added_ingestion_column_redshift_tables_audit_tab.csv"

# === Load both datasets
df_query = pd.read_csv(QUERY_CSV)
df_ingestion = pd.read_csv(INGESTION_CSV)

# === Normalize schema name columns for consistent matching
df_query["Schema Name"] = df_query["Schema Name"].astype(str).str.strip().str.lower()
df_ingestion["DB Schema Name"] = df_ingestion["DB Schema Name"].astype(str).str.strip().str.lower()

# === Build a mapping from DB Schema Name to Ingestion Tool
schema_to_tool = df_ingestion.set_index("DB Schema Name")["Ingestion Tool"].to_dict()

# === Add Ingestion Tool column to Redshift query data
df_query["Ingestion Tool"] = df_query["Schema Name"].map(schema_to_tool)

# === Save the enriched output
df_query.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Enriched Redshift usage data saved to {OUTPUT_CSV}")