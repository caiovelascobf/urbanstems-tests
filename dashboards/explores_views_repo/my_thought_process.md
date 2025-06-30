## 🧠 DBT-to-Looker Mapping Strategy (Updated)

### 🎯 Objective

Build a mapping between **DBT models** and **Looker dashboards**, allowing end-to-end data lineage — from DBT model code to Redshift tables, through LookML views and explores, all the way to dashboard tiles. This enables change impact analysis, auditing, and better collaboration between data engineering and BI teams.

---

### 📥 Inputs

1. **Looker LookML repo (`looker-master`)**
   - Includes all `.lkml` files (`.view.lkml`, `.model.lkml`)
   - Source of:
     - `sql_table_name` (direct table reference)
     - `derived_table` SQL blocks
     - `explore:` definitions (via model files)

2. **Looker System Activity export (CSV)**
   - Dashboard metadata
   - Includes:
     - `dashboard_id_(user-defined_only)`
     - `dashboard_title`
     - `query_model`
     - `query_explore`

3. **DBT model list (CSV)**
   - List of `.sql` files such as `orders.sql`, `user_metrics.sql`
   - Derivable names: `orders`, `user_metrics`

4. *(Optional)* **Redshift information**
   - Used to validate materialization (e.g., tables like `analytics.orders` or `analytics.analytics.orders`)

---

### ⚙️ Processing Steps

#### 🔹 Step 1: Extract View + Explore Table Usage

- **Script:** `script_01-extracting_looker_tables_from_views_and_models.py`
- Recursively scans all `.lkml` files to extract:
  - `view_or_model_name` → name of the view or explore
  - `model_name` → from model file names
  - `sql_table_name` → direct table mapping
  - `derived_table_sources` → extracted from `derived_table { sql: ... }` blocks
  - `lkml_file` → source file path
- Output: `script_01-extracting_looker_tables_from_views_and_models.csv`

#### 🔹 Step 2: Map Dashboards → Explores → LookML → Redshift Tables

- **Script:** `script_02-dashboards_to_views_to_redshift.py`
- Joins:
  - `query_explore` from Looker System Activity
  - with `view_or_model_name` from script_01
- Resolves and combines:
  - `sql_table_name`
  - `derived_table_sources`
- Produces:
  - Full mapping: `dashboard_id`, `dashboard_title`, `query_model`, `query_explore`, `lkml_file`, `view_or_model_name`, `redshift_tables`
  - Exploded mapping: one row per dashboard-table pair
- Outputs:
  - `script_02-dashboards_to_views_to_redshift.csv`
  - `script_02-dashboards_to_views_to_redshift_exploded.csv`

#### 🔹 Step 3: Match DBT Models to Redshift Tables

- **Script:** `script_03-dbt_models_to_dashboards.py` (custom logic)
- Converts DBT models (`orders.sql`) into candidate Redshift tables:
  - `analytics.orders`
  - `analytics.analytics.orders`
- Performs string-based matching against `redshift_table` column from exploded output (Step 2)
- Creates DBT model → dashboard associations

#### 🔹 Step 4: Reverse Map Dashboards → DBT Models

- Inverts the mapping from Step 3
- Aggregates DBT models by dashboard title
- Useful for traceability and documentation

---

### 📤 Outputs

#### ✅ Tab: **DBT Models → Dashboards**

- For each DBT model:
  - Which dashboards rely on its output table
- Use case: *“If I change this model, what dashboards might break?”*

#### ✅ Tab: **Dashboards → DBT Models**

- For each dashboard:
  - Which DBT models its tiles depend on (via Redshift table lineage)
- Use case: *“Where does this metric come from?”*

---

### ⚠️ Gaps & Limitations

- DBT models not materialized in Redshift will not match
- Some dashboards may use legacy tables or raw SQL that bypass DBT
- String-matching introduces false positives/negatives
- Does not account for transitive dependencies between DBT models

---

### ✅ Summary

This strategy provides a practical, explainable DBT → Looker lineage map by combining:

- LookML static analysis
- Dashboard metadata
- DBT model naming conventions
- Redshift table resolution
- Exploded lineage for full many-to-many joins

Ideal for auditability, observability, and collaborative debugging across data teams.
