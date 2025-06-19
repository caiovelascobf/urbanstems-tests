import json
import csv

# Load manifest.json from dbt Cloud (or local run)
with open(r"raw\manifest.json") as f:
    manifest = json.load(f)

# Prepare a list to hold model metadata
models = []

# Loop through all nodes and extract only dbt models
for key, node in manifest.get('nodes', {}).items():
    if node.get('resource_type') == 'model':
        models.append({
            'database': node.get('database'),
            'schema_name': node.get('schema'),      # Renamed to match audit schema
            'object_name': node.get('alias')        # alias = actual name in warehouse
        })

# Define output CSV file
output_path = 'script_01-dbt_models_from_manifest.csv'

# Write the extracted data to CSV
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['database', 'schema_name', 'object_name'])
    writer.writeheader()
    writer.writerows(models)

print(f"✅ Extracted {len(models)} dbt-managed models.")
print(f"📄 Output saved to: {output_path}")
