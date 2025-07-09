import pandas as pd
import os

# -----------------------------
# 🧾 File Paths & Configuration
# -----------------------------

INPUT_CSV = r"C:\jobs_repo\brainforge\urbanstems-tests\redshift_tables_audit\redshift_archive_schema\raw\archive_candidates.csv"
OUTPUT_DIR = r"C:\jobs_repo\brainforge\urbanstems-tests\redshift_tables_audit\redshift_archive_schema\output_csvs"
SCHEMA_LIMIT = 10  # Limit number of schemas to process (for testing)

def export_schema_csvs(input_csv: str, output_dir: str, schema_limit: int = None) -> None:
    """
    Splits an input CSV containing Redshift object candidates into multiple CSV files,
    one per schema, named as 'archive_<schema>.csv'.

    Parameters:
    -----------
    input_csv : str
        Full path to input CSV. Must contain 'schema_name', 'table_name', and 'object_type' columns.

    output_dir : str
        Directory where the individual schema CSVs will be saved.

    schema_limit : int, optional
        If specified, limits the number of unique schemas processed (for testing).

    Output:
    -------
    Creates one CSV file per unique schema in the format: archive_<schema>.csv
    Each file contains: schema_name, table_name, object_type
    """
    # Load the input CSV
    df = pd.read_csv(input_csv)

    # Check required columns
    required_cols = {"schema_name", "table_name", "object_type"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Input CSV must contain columns: {required_cols}")

    # Get unique schemas (limited if requested)
    unique_schemas = df['schema_name'].dropna().unique()
    if schema_limit:
        unique_schemas = unique_schemas[:schema_limit]

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Filter to only rows in selected schemas
    filtered_df = df[df['schema_name'].isin(unique_schemas)]

    # Export one CSV per schema
    for schema, group in filtered_df.groupby('schema_name'):
        sanitized_schema = schema.replace(' ', '_')  # Optional: sanitize
        output_path = os.path.join(output_dir, f"archive_{sanitized_schema}.csv")
        group.to_csv(output_path, index=False, columns=["schema_name", "table_name", "object_type"])

    print(f"✅ Created {len(unique_schemas)} CSV files in: {output_dir}")

# Run it
export_schema_csvs(INPUT_CSV, OUTPUT_DIR, SCHEMA_LIMIT)
