import pandas as pd

# ----------------------------
# 🧾 Input & Output File Paths
# ----------------------------

INPUT_CSV_PATH = r"C:\jobs_repo\brainforge\urbanstems-tests\redshift_tables_audit\raw\schemas_in_redshift_table_audit_2025_07_08.csv"
OUTPUT_CSV_PATH = "script_01_unique_schemas.csv"

def extract_unique_schemas(input_csv_path: str, output_csv_path: str) -> None:
    """
    Reads a CSV file containing Redshift object metadata (including schema names),
    extracts unique schema names, and writes them to a new CSV file.

    Parameters:
    ----------
    input_csv_path : str
        The full path to the input CSV file. The file must contain a column named 'schema_name'.
    
    output_csv_path : str
        The full path to the output CSV file that will contain unique schema names.
    
    Output:
    ------
    A CSV file at `output_csv_path` containing one column: 'schema_name',
    with each unique schema name from the input. Also prints how many duplicates were removed.
    """
    # Read the input CSV
    df = pd.read_csv(input_csv_path)

    # Ensure the required column exists
    if 'schema_name' not in df.columns:
        raise ValueError("Input CSV must contain a 'schema_name' column.")

    # Count before deduplication
    total_count = len(df)
    unique_count = df['schema_name'].dropna().nunique()
    duplicate_count = total_count - unique_count

    # Extract and sort unique schema names
    unique_schemas = df['schema_name'].dropna().unique()
    unique_df = pd.DataFrame({'schema_name': sorted(unique_schemas)})

    # Save to output CSV
    unique_df.to_csv(output_csv_path, index=False)

    # Print summary
    print("✅ Schema Deduplication Summary")
    print("-------------------------------")
    print(f"Total rows read        : {total_count}")
    print(f"Unique schema names    : {unique_count}")
    print(f"Duplicate entries found: {duplicate_count}")
    print(f"Output written to      : {output_csv_path}")

# Run the script
extract_unique_schemas(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
