## 🧠 DBT-to-Looker Mapping Strategy (Updated)

### 🎯 Objective

Build a mapping between **DBT models** and **Looker dashboards**, allowing end-to-end data lineage — from DBT model code to Redshift tables, through LookML views and explores, all the way to dashboard tiles. This enables change impact analysis, auditing, and better collaboration between data engineering and BI teams.

---

### 📥 Inputs

1. **Looker LookML repo (`looker-master`)**
   - Includes all `.lkml` files:
     - `*.view.lkml` (view definitions, sql_table_name, derived_table)
     - `*.model.lkml` (explore declarations, join logic)
   - Source of:
     - View → Redshift table mappings (`sql_table_name`)
     - Derived tables and join references
     - `${view.field}` cross-view usage

2. **Looker System Activity export (CSV)**
   - Dashboard metadata and usage context
   - Includes:
     - `dashboard_id_(user-defined_only)`
     - `dashboard_title`
     - `query_model`
     - `query_explore`

3. **DBT model list (CSV)**
   - List of `.sql` files such as `orders.sql`, `user_metrics.sql`
   - Used to generate expected materializations (e.g., `analytics.orders`)

4. *(Optional)* **Redshift information**
   - Used to validate materialization (e.g., `analytics.orders`, `s3_csv.*`, etc.)

---

### ⚙️ Processing Steps

#### 🔹 Step 1: Extract View + Explore Table Usage

**📜 Script:** `script_01-extracting_looker_tables_from_views_and_models.py`  
**📄 Output:** `script_01-extracting_looker_tables_from_views_and_models.csv`

This script recursively scans all LookML files to build a full dependency map:

| Column | Description |
|--------|-------------|
| `view_or_model_type` | One of: `view`, `explore`, `join_view`, `view_reference` |
| `view_or_model_name` | Name of the view, join alias, or explore |
| `model_name` | Extracted from the `.model.lkml` file name |
| `base_view_name` | For explores, indicates `view_name:` override |
| `lkml_file` | Relative path to the LookML file |
| `sql_table_name` | If view has a `sql_table_name` declaration |
| `derived_table_sources` | Views referenced in the `derived_table { sql: ... }` block (parsed from JOINs / FROMs) |

It captures:
- `view:` blocks with sql table or derived table SQL
- `explore:` blocks with optional `view_name:` override
- `join:` aliases (including resolved `from:` view names)
- `${other_view.field}` references anywhere in `.lkml` files
- References in derived tables like `JOIN analytics.orders`, parsed from SQL

The script **deduplicates** all rows based on full content to avoid redundant entries.

---

#### 🔹 Step 2: Map Dashboards → Explores → LookML → Redshift Tables

**📜 Script:** `script_02-dashboards_to_views_to_redshift.py`

- Joins:
  - `query_explore` from System Activity
  - `view_or_model_name` and metadata from `script_01`
- Resolves:
  - Explores → views
  - Views → Redshift tables (from `sql_table_name`, `derived_table_sources`)
- Produces:
  - Clean dashboard lineage (via `query_model` and `query_explore`)
  - Flattened file: one row per dashboard-view-table combination

**📄 Outputs:**
- `script_02-dashboards_to_views_to_redshift.csv`
- `script_02-dashboards_to_views_to_redshift_exploded.csv`

---

#### 🔹 Step 3: Match DBT Models to Redshift Tables

**📜 Script:** `script_03-dbt_models_to_dashboards.py`

- Converts DBT model files (`orders.sql`) into candidate table names:
  - e.g., `analytics.orders`, `analytics.analytics.orders`
- Performs string matching against Redshift table references from Step 2
- Produces:
  - DBT model → dashboard dependencies

---

#### 🔹 Step 4: Reverse Map Dashboards → DBT Models

- Inverts the result of Step 3
- Outputs a list of all DBT models supporting each dashboard
- Enables reverse traceability (dashboard → underlying models)

---

### 📤 Outputs

#### ✅ Tab: **DBT Models → Dashboards**

- For each DBT model:
  - Lists dependent dashboards
- Use case: *“If I refactor this model, what dashboards might break?”*

#### ✅ Tab: **Dashboards → DBT Models**

- For each dashboard:
  - Lists all DBT models referenced (directly or indirectly)
- Use case: *“Where does this dashboard’s data come from?”*

---

### ⚠️ Gaps & Limitations

- DBT models must be materialized in Redshift to match
- Non-modeled SQL (raw queries) may bypass DBT and not link
- `${view.field}` references do not imply semantic usage — may be unused
- String-based matching is sensitive to naming mismatches or prefix ambiguity
- Transitive lineage (e.g. model A feeds model B which feeds view C) is not resolved

---

### ✅ Summary

This strategy provides a complete DBT → Looker lineage view using:

- **Static LookML analysis** from `script_01`
- **Real dashboard usage** from Looker System Activity
- **DBT naming heuristics** for model → table resolution
- **Redshift materialization assumptions**
- **Exploded many-to-many mappings** for downstream impact and auditability

Useful for platform migrations, trust audits, and lifecycle management across the BI pipeline.
