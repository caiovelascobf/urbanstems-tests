"""
📊 Dashboard Usage Summary Script

This script processes a Looker System Activity export (Content Usage CSV) to:
1. Load and clean the raw data.
2. Sort dashboards by last access date.
3. Identify dashboards with duplicate names.
4. Detect similar dashboard titles using fuzzy text matching.
5. Group dashboards by similar names (e.g., variants or copies).
6. Add flags and metrics to help identify important dashboards and reduce redundancy.
7. Save a cleaned, structured CSV for review or further analysis.
8. Additionally, generate a summary of unique dashboards based on title, showing latest activity.

Outputs:
- final/dashboard_usage_summary.csv → Full cleaned dashboard list with grouping logic
- final/unique_dashboard_summary.csv → Unique dashboards by title, showing one most recent entry

"""

import pandas as pd
import os
from difflib import SequenceMatcher

# File paths
input_file = "raw/system__activity content_usage 2025-06-04T1248.csv"
output_summary = "final/dashboard_usage_summary.csv"
output_unique = "final/unique_dashboard_summary.csv"

# Load CSV
df = pd.read_csv(input_file)
total_rows_raw = len(df)
unique_titles_raw = df['Content Usage Content Title'].nunique()
print(f"📥 Raw dashboards loaded: {total_rows_raw} rows, {unique_titles_raw} unique dashboard titles")

# Rename for ease
df = df.rename(columns={
    'Content Usage Content ID': 'Content ID',
    'Content Usage Content Title': 'Content Title',
    'Content Usage Last Accessed Date': 'Last Accessed Date',
    'Content Usage View Count': 'View Count',
    'Content Usage Content Type': 'Content Type',
    'Content Usage Schedule Total': 'Schedule Total',
    'Content Usage Favorites Total': 'Favorites Total'
})

# Parse dates
df['Last Accessed Date'] = pd.to_datetime(df['Last Accessed Date'], errors='coerce')

# Keep only relevant columns
columns_to_keep = [
    'Content ID',
    'Content Title',
    'Content Type',
    'Last Accessed Date',
    'View Count',
    'Schedule Total',
    'Favorites Total'
]
df = df[columns_to_keep]

# Flag exact duplicate titles
df['Is Duplicate Title'] = df['Content Title'].duplicated(keep=False).map({True: "Yes", False: "No"})

# Group similar titles using fuzzy matching (difflib)
titles = df['Content Title'].unique()
similar_groups = {}
threshold = 0.85  # similarity ratio

for title in titles:
    matched_group = None
    for group_title in similar_groups:
        if SequenceMatcher(None, title.lower(), group_title.lower()).ratio() >= threshold:
            matched_group = group_title
            break
    if matched_group:
        similar_groups[matched_group].append(title)
    else:
        similar_groups[title] = [title]

# Map each title to its group representative and count members
title_to_group = {}
group_sizes = {}
for group_title, group_members in similar_groups.items():
    for title in group_members:
        title_to_group[title] = group_title
    group_sizes[group_title] = len(group_members)

df['Similar Title Group'] = df['Content Title'].map(title_to_group)
df['Title Group Count'] = df['Similar Title Group'].map(group_sizes)

# Flag titles with similar matches (group size > 1)
df['Is Similar Title'] = df['Title Group Count'].apply(lambda x: "Yes" if x > 1 else "No")

# Reorder columns for clarity
ordered_columns = [
    'Content ID',
    'Content Title',
    'Content Type',
    'Last Accessed Date',
    'View Count',
    'Schedule Total',
    'Favorites Total',
    'Is Duplicate Title',
    'Is Similar Title',
    'Similar Title Group',
    'Title Group Count'
]
df = df[ordered_columns]

# Sort to group similar dashboards and order by recency within group
df = df.sort_values(by=['Similar Title Group', 'Last Accessed Date'], ascending=[True, False])

# Output summary
print(f"📤 Final dashboards: {len(df)} rows")
print(f"🔁 Exact duplicates: {df['Is Duplicate Title'].value_counts().get('Yes', 0)}")
print(f"🔎 Similar title groups: {df['Is Similar Title'].value_counts().get('Yes', 0)} dashboards across {len([g for g in group_sizes.values() if g > 1])} groups")

# Save dashboard usage summary
os.makedirs(os.path.dirname(output_summary), exist_ok=True)
df.to_csv(output_summary, index=False)
print(f"✅ Dashboard usage summary saved to: {output_summary}")

# ✨ Create unique dashboard summary based on latest accessed per title
df_unique = df[df['Content Type'] == 'dashboard'].copy()
df_unique = df_unique.sort_values(by='Last Accessed Date', ascending=False)
df_unique = df_unique.drop_duplicates(subset='Content Title', keep='first')
df_unique = df_unique[['Content Title', 'Content ID', 'Last Accessed Date', 'View Count']]
df_unique = df_unique.sort_values(by='Last Accessed Date', ascending=False)

# Save unique dashboard list
df_unique.to_csv(output_unique, index=False)
print(f"📌 Unique dashboard summary saved to: {output_unique}")
