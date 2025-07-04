# 🧱 Looker View Deprecation Analysis — Step-by-Step Guide

This guide explains how to identify **unused Looker views** in your LookML project, so they can be safely removed or deprecated. Views define the dimensions and measures that power dashboards, explores, and ad hoc queries — but unused views add complexity and technical debt.

This analysis combines:
- **Explore-based usage** from your LookML models
- **Real-world query usage** from Looker’s System Activity logs

---

## 🎯 Why This Matters in the Deprecation Workflow

In Looker:
- **Views** are data blueprints
- **Explores** are user entry points
- **Dashboards/Looks** rely on explores, which in turn reference views

That means:
- If a view is **not referenced by any explore**, and
- **Not used in any actual query** (SQL Runner, dev explores, dashboard filters)...

👉 It is **safe to deprecate**.

> ✅ Deprecating unused views will:
> - Remove technical clutter
> - Prevent confusion and misused fields
> - Improve model performance and validation times
> - Reduce maintenance cost

---

## ✅ Step 1: Extract View and Explore Metadata

**📄 Source File:** `script_01-extracting_looker_tables_from_views_and_models.csv`

This file is generated from `.view.lkml` and `.model.lkml` files, and includes:
- View names and file paths
- Explore definitions, including `from:` references and base view overrides
- Join view aliases and their resolved `from:` views
- `${other_view.field}` field-level cross references
- Derived table upstream sources parsed from `derived_table.sql`

> ⚙️ You already created this using `script_01`, which parses your LookML project recursively and generates a fully deduplicated map of dependencies.

---

## ✅ Step 2: Gather System Activity (Real Query Usage)

**📄 Source File:** `system__activity_history_YYYY-MM-DD.csv`  
Exported from **System Activity → History** in Looker.

This file includes:
- `Query Fields Used`: actual fields used in any Looker query
- `Query Created Date`: when the query was run
- Used to detect views even if they're **not modeled in explores**

> This step helps catch edge cases like:
> - SQL Runner queries
> - Dev-mode explores
> - Scheduled content based on legacy views

---

## ✅ Step 3: Flag Unused Views

**📜 Script:** `script_03-flag_unused_views.py`  
**📄 Output:** `script_03-flag_unused_views.csv`

This script combines both input files and produces a comprehensive view usage audit.

### 🧠 Key Fields in the Output:

| Column | Description |
|--------|-------------|
| `view_name` | Name of the LookML view |
| `lkml_file` | Path to the `.view.lkml` file |
| `sql_table_names` | Table name if the view uses `sql_table_name` |
| `derived_table_sources` | Source tables used in `derived_table.sql`, if present |
| `used_in_explore` | `True` if referenced in a model explore (via `from:`, `view_name:`, `join:`, `${view.field}`, or derived table SQL) |
| `used_in_system_activity` | `True` if queried in actual Looker usage (System Activity) |
| `last_used_in_system_activity` | Most recent date the view was queried (empty if never) |
| `safe_to_deprecate_view` | `True` if the view is unused in both LookML *and* live queries |

---

## ⚙️ Logic Summary

```text
1. Load all views defined in .view.lkml files

2. Identify view references in:
    a. base_view_name of explores (via view_name: or from:)
    b. join aliases resolved to view names (join: xyz { from: abc })
    c. ${other_view.field} references across LookML files
    d. derived_table.sql blocks that use FROM or JOIN view.table patterns

3. Load query history from System Activity CSV:
    a. Parse view names from "Query Fields Used"
    b. Track most recent usage date per view

4. For each view:
    - Flag used_in_explore based on step 2 references
    - Flag used_in_system_activity based on step 3 query history
    - If both are False → safe_to_deprecate_view = True

5. Output the final audit to script_03-flag_unused_views.csv
