## 🧠 DBT-to-Looker Mapping Strategy: Organized Thinking

### 🎯 Objective

Build a mapping between **DBT models** and **Looker Dashboards**, to understand which dashboards rely on which DBT models and vice versa. This allows traceability from data transformation (DBT) to visualization (Looker).

---

### 📥 Inputs

1. **Looker LookML repo (`looker-master`)**
   - `.view.lkml` and `.model.lkml` files
   - Source of `sql_table_name` and `derived_table` queries (includes actual Redshift tables used)

2. **Looker System Activity export (CSV)**
   - Dashboard metadata
   - Includes: `Dashboard ID`, `Dashboard Title`, `Query Explore`, `Query Model`

3. **DBT model list (CSV)**
   - Raw list of `.sql` files like `orders.sql`, `user_metrics.sql`

4. *(Optional)* **Redshift information**
   - Used to validate whether a dbt model has been materialized in Redshift (e.g., under `analytics.analytics.dbt_model_name`)

---

### ⚙️ Processing Steps

#### 🔹 Step 1: Extract Table Usage from Looker Views

- **Script:** `script_01-looker_views_and_its_tables_mapping.csv`
- Extract:
  - `view_name`
  - `sql_table_name` (direct)
  - `derived_table_sources` (via parsing SQL: all `FROM`/`JOIN` targets)
- Output: Redshift tables used per LookML view

#### 🔹 Step 2: Map Looker Dashboards → Views → Redshift Tables

- **Script:** `script_02-dashboards_to_views_to_redshift.csv`
- Join the System Activity dashboard data to LookML views by `query_explore` = `view_name`
- Resolve Redshift tables used by each dashboard
- Also generate an **exploded** version: one row per dashboard-table pair

#### 🔹 Step 3: Match DBT Models to Redshift Tables

- **Script:** `script_03-dbt_models_to_dashboards.csv`
- Convert each DBT model (e.g., `orders.sql`) to:
  - `dbt_model_name` = `orders`
  - `potential_full_path` = `analytics.orders` or `analytics.analytics.orders`
- Match these names against the Redshift tables extracted in Step 2 (exploded)
- Result: list of dashboards using tables that correspond to DBT models

#### 🔹 Step 4: Reverse Mapping: Dashboards → DBT Models

- (Same logic as above, reversed)
- Group all matched DBT models by dashboard title
- Output: which DBT models feed each dashboard

---

### 📤 Outputs

You now have two useful views:

#### ✅ Tab: **DBT Models → Dashboards**

- For each DBT model, which dashboards are *using* it (via Redshift tables)
- This supports impact analysis: *“If I change this DBT model, who gets affected?”*

#### ✅ Tab: **Dashboards → DBT Models**

- For each dashboard, what DBT models it depends on
- Helps explain data lineage: *“Where does this chart's data come from?”*

---

### ⚠️ Gaps & Limitations

- Some rows are blank:
  - Many DBT models may not be materialized in Redshift
  - Some Looker dashboards may use non-DBT tables or legacy views
- Views with *heavily derived SQL logic* or non-standard joins may be partially parsed
- Matching is done by **string logic**, not by full SQL lineage graphs

---

### ✅ Summary

You effectively created a **DBT-to-Looker lineage map**, using:
- Static code parsing (LookML, DBT)
- Metadata from Looker
- Heuristic matching (`analytics.model_name`)
- Exploded table mapping for many-to-many relationships

This is a strong foundation for automated lineage, auditability, and collaboration between data engineering and BI teams.
