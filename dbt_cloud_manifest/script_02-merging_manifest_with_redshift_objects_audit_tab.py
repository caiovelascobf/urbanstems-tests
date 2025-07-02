import pandas as pd

# Load Redshift audit file
audit_file = r"raw\script_03-new_redshift_objects_audit_google_sheet_180.csv"
audit_df = pd.read_csv(audit_file)

# Load DBT-managed models and sources extracted from manifest
dbt_file = 'script_01-dbt_models_and_sources_from_manifest.csv'
dbt_df = pd.read_csv(dbt_file)

# Normalize audit columns to lowercase for matching
audit_df["schema_name"] = audit_df["schema_name"].str.lower()
audit_df["object_name"] = audit_df["object_name"].str.lower()

# Merge on schema_name and object_name
merged_df = audit_df.merge(
    dbt_df,
    on=["schema_name", "object_name"],
    how="left",
    indicator=True
)

# Add a Y/N flag for DBT-managed status
merged_df["Is in manifest.json"] = merged_df["_merge"].apply(lambda x: "Y" if x == "both" else "N")

# Drop helper columns
merged_df.drop(columns=["_merge", "database"], inplace=True, errors='ignore')

# Save to a new CSV
output_path = 'script_02-redshift_audit_with_dbt_flag_180.csv'
merged_df.to_csv(output_path, index=False)

# Summary output
print(f"✅ Merged audit saved to: {output_path}")
print(f"🔢 Total objects: {len(merged_df)}")
print(f"🟢 In manifest.json (Y): {(merged_df['Is in manifest.json'] == 'Y').sum()}")
print(f"🔴 Not in manifest.json (N): {(merged_df['Is in manifest.json'] == 'N').sum()}")
