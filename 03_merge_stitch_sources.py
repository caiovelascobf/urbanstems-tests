import pandas as pd

# === Input CSVs
SOURCE_CSV = "01_csv_stitch_sources_freq_dest.csv"
TABLE_CSV = "02_csv_stitch_tables.csv"
OUTPUT_CSV = "03_csv_stitch_merged_source_tables.csv"

# === Load both datasets
df_sources = pd.read_csv(SOURCE_CSV)
df_tables = pd.read_csv(TABLE_CSV)

# === Merge on "Source Name"
df_merged = df_tables.merge(
    df_sources[["Source Name", "Frequency", "Destination"]],
    on="Source Name",
    how="left"  # keep all table-level rows even if some don't match
)

# === Save final enriched table-level CSV
df_merged.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Merged table saved to {OUTPUT_CSV}")
