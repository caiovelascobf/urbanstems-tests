"""
🔗 Join Dashboard Usage with User Metadata

This script merges:
- The main processed usage data (`dashboard_usage_summary.csv`)
- Additional user and dashboard metadata (`dashboard_user_metadata.csv`)

The join is performed on:
  - `Content ID` (usage)
  - `Dashboard ID (User-defined only)` (metadata)

Output:
- final/final_dashboard_user_summary.csv

"""

import pandas as pd
import os

# File paths
usage_file = "final/dashboard_usage_summary.csv"
meta_file = "raw/user_info_system__activity dashboard 2025-06-09T1912.csv"
output_file = "final/final_dashboard_user_summary.csv"

# Load data
usage_df = pd.read_csv(usage_file)
meta_df = pd.read_csv(meta_file)

# Rename metadata fields to match or standardize
meta_df = meta_df.rename(columns={
    'Dashboard ID (User-defined only)': 'Dashboard ID',
    'Updated By Name': 'Updated By Name',
    'User Name': 'User Name',
    'User Email': 'User Email',
    'User ID': 'User ID',
    'Dashboard Description': 'Description',
    'Dashboard Title': 'Meta Title'
})

# Convert join keys to string
usage_df['Content ID'] = usage_df['Content ID'].astype(str)
meta_df['Dashboard ID'] = meta_df['Dashboard ID'].astype(str)

# Join usage with metadata on Content ID ↔ Dashboard ID
joined_df = usage_df.merge(
    meta_df,
    left_on='Content ID',
    right_on='Dashboard ID',
    how='left'
)

# Save to final output
os.makedirs(os.path.dirname(output_file), exist_ok=True)
joined_df.to_csv(output_file, index=False)

print(f"✅ Final dashboard-user summary saved to: {output_file}")
