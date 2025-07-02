"""
This script parses the dbt `manifest.json` file to extract all objects that are 
either managed by dbt (models) or referenced as external sources.

It produces a normalized, deduplicated list of DBT objects including:
- database
- schema_name (lowercased)
- object_name (lowercased)
- resource_type ('model' or 'source')

Key Constraints:
- Only one entry is allowed per unique (database, schema_name, object_name) triplet.
  If duplicates exist (e.g., the same table used both as a model and a source),
  the first occurrence is kept (priority not enforced).

Output:
- A CSV file: `script_01-dbt_models_and_sources_from_manifest.csv`
"""

import json
import csv

# Load manifest.json from dbt Cloud or local run
with open(r"raw\manifest.json") as f:
    manifest = json.load(f)

# Prepare lists for models and sources
models = []
sources = []

# Extract models
for key, node in manifest.get('nodes', {}).items():
    if node.get('resource_type') == 'model':
        models.append({
            'database': node.get('database'),
            'schema_name': node.get('schema', '').lower(),
            'object_name': node.get('alias', '').lower(),
            'resource_type': 'model'
        })

# Extract sources
for key, source in manifest.get('sources', {}).items():
    sources.append({
        'database': source.get('database'),
        'schema_name': source.get('schema', '').lower(),
        'object_name': source.get('identifier', '').lower(),
        'resource_type': 'source'
    })

# Combine and deduplicate by (database, schema_name, object_name)
combined = models + sources
seen_keys = set()
unique_objects = []

for obj in combined:
    key = (obj['database'], obj['schema_name'], obj['object_name'])
    if key not in seen_keys:
        unique_objects.append(obj)
        seen_keys.add(key)

# Define output CSV file
output_path = 'script_01-dbt_models_and_sources_from_manifest.csv'

# Write to CSV
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['database', 'schema_name', 'object_name', 'resource_type'])
    writer.writeheader()
    writer.writerows(unique_objects)

print(f"✅ Extracted {len(models)} models and {len(sources)} sources from manifest.")
print(f"🧹 Deduplicated to {len(unique_objects)} unique objects.")
print(f"📄 Output saved to: {output_path}")
