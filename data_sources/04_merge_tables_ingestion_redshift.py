import pandas as pd

# === Input files
QUERY_CSV = "redshift_brainforge_google_sheet.csv"
INGESTION_CSV = "final_brainforge_google_sheet_ingestion_table_level.csv"
OUTPUT_CSV = "04_csv_redshift_and_ingestion_schemas.csv"

# === Load both datasets
df_query = pd.read_csv(QUERY_CSV)
df_ingestion = pd.read_csv(INGESTION_CSV)

# === Normalize schema name fields for reliable matching
df_query["Schema Name"] = df_query["Schema Name"].astype(str).str.strip().str.lower()
df_ingestion["DB Schema Name"] = df_ingestion["DB Schema Name"].astype(str).str.strip().str.lower()

# === Merge on schema name
df_merged = df_query.merge(
    df_ingestion[["DB Schema Name", "Ingestion Tool"]],
    left_on="Schema Name",
    right_on="DB Schema Name",
    how="left"
)

# === Drop merge key duplicate
df_merged.drop(columns=["DB Schema Name"], inplace=True)

# === Save result
df_merged.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Merged output saved to {OUTPUT_CSV}")