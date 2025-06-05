import pandas as pd

# === Input files
QUERY_CSV = "redshift_Final_Combined_Query_for_Google_Sheets_Output_Tracking_Used_Sca_202506051344.csv"
INGESTION_CSV = "final_brainforge_google_sheet_ingestion_table_level.csv"
OUTPUT_CSV = "04_csv_redshift_and_ingestion_schemas.csv"

# === Load both datasets
df_query = pd.read_csv(QUERY_CSV)
df_ingestion = pd.read_csv(INGESTION_CSV)

# === Merge based on schema name
df_merged = df_query.merge(
    df_ingestion[["DB Schema Name", "Ingestion Tool"]],
    left_on="schema_name",
    right_on="DB Schema Name",
    how="left"
)

# === Drop duplicate schema key column if desired
df_merged.drop(columns=["DB Schema Name"], inplace=True)

# === Save merged output
df_merged.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Merged output saved to {OUTPUT_CSV}")
