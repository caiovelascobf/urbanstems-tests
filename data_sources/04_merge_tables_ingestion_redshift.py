import pandas as pd

# === Input files
QUERY_CSV = "redshift_brainforge_google_sheet.csv"
INGESTION_CSV = "final_brainforge_google_sheet_ingestion_table_level.csv"
OUTPUT_CSV = "04_csv_redshift_and_ingestion_schemas.csv"

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