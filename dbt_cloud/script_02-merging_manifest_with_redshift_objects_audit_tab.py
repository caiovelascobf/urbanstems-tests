import pandas as pd

# Load Redshift audit file
audit_file = r"raw\script_03-new_redshit_objects_audit_google_sheet.csv"
audit_df = pd.read_csv(audit_file)

# Load dbt-managed models extracted from manifest
dbt_file = 'script_01-dbt_models_from_manifest.csv'
dbt_df = pd.read_csv(dbt_file)

# Merge on schema_name and object_name
merged_df = audit_df.merge(
    dbt_df,
    on=["schema_name", "object_name"],
    how="left",
    indicator=True
)

# Add a new flag: whether object is managed by dbt
merged_df["is_managed_by_dbt"] = merged_df["_merge"] == "both"

# Drop the merge indicator and database (if you want to keep it clean)
merged_df.drop(columns=["_merge", "database"], inplace=True, errors='ignore')

# Save to a new CSV
output_path = 'script_02-redshift_audit_with_dbt_flag.csv'
merged_df.to_csv(output_path, index=False)

print(f"✅ Merged audit saved to: {output_path}")
print(f"🔢 Total objects: {len(merged_df)}")
print(f"🟢 Managed by dbt: {merged_df['is_managed_by_dbt'].sum()}")
