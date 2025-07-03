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
- Explore names and their referenced views
- SQL table names and derived table sources (used for indirect references)

> ⚙️ You already created this using `script_01`, which parses your LookML project recursively.

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
| `used_in_explore` | `True` if referenced in a model explore (`base_view_name`, joins, or derived sources) |
| `used_in_system_activity` | `True` if queried in actual Looker queries (SQL Runner, dev explore, dashboard filters) |
| `last_used_in_system_activity` | Most recent date the view was queried (empty if never) |
| `safe_to_deprecate_view` | `True` if the view is unused in both explores and system activity |

---

## ⚙️ Logic Summary

```text
1. Load all views defined in .view.lkml files
2. Identify views referenced in:
    a. base_view_name of explores
    b. join/from clauses
    c. derived_table_sources
3. Load system activity query history
    a. Parse view names from Query Fields Used
    b. Track last-used date per view
4. For each view:
    - Mark used_in_explore
    - Mark used_in_system_activity
    - Derive last_used_in_system_activity
    - If both are False → safe_to_deprecate_view = True
5. Output all data to script_03-flag_unused_views.csv
