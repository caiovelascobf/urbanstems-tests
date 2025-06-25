# 📊 Dashboard Deprecation Workflow

## Step 1: Build a Comprehensive Dashboard List

- **First**, we needed to compile a full list of dashboards.
  - To do this, we **extracted dashboard usage data** and enriched it with **additional user metadata**.
    - This involved several **Python scripts** to pull and merge data from **Looker → System Activity**.
      - ✅ The result was the creation of the **`Dashboards (Audit)`** sheet.

---

## Step 2: Map Dashboards to Redshift Tables via Explores, Views & DBT

- **Second**, we extracted all **Looker Explores and Views** from the `looker-master` repo folder, in order to map them to **Redshift tables**.
  - Then, we joined those Redshift tables with their corresponding **dbt models** from the **`DBT (Audit)`** sheet.
  - After that, we merged these Redshift–DBT mappings back to dashboards, giving us a **dashboard-to-redshift-table map** focused on their **underlying dbt models**.
    - ✅ Key columns added to `DBT (Audit)`: **Match** and **Dashboards**.

- **Third**, we also created a separate sheet that maps **dashboards directly to Redshift tables**, on a **dashboard-level granularity**, to make analysis more flexible.
  - ✅ Output: **`Dash <> DBT (Audit)`** sheet.

- **Fourth**, we enriched the `Dashboards (Audit)` sheet with columns to **flag usage of the associated Explores and Views**.
  - ✅ Columns added: **Unused Explores**, **Unused Views**.

---

## Step 3: Initial Deprecation Signals from the Client

- Now that we had a foundational list of dashboards, the client provided an initial set of dashboards they considered **candidates for deprecation**.
  - ✅ This was captured in the **`Deprecate`** column in `Dashboards (Audit)`.

- However, this client-provided list wasn't enough — we needed to make it more **data-driven and robust**.

---

## Step 4: Add a Model-Backed Deprecation Layer

To strengthen our deprecation logic, we introduced a **second layer** that ties **dashboard deprecation to their associated dbt models** via Redshift tables.

### How:

1. **Query Redshift Table Usage**
   - We pulled usage metrics from Redshift to see whether users or systems were querying these tables.
     - ✅ In `Redshift Tables (Audit)`: columns like **Queried By**, **Last Query Time**, and **Scan Count**.
   - We flagged tables that had **no scans** (i.e., not used).
     - ✅ Column: **Is Used in Redshift**.

2. **Check DBT Usage of Redshift Tables**
   - We flagged Redshift tables that were **not used by any dbt model**.
     - ✅ Column: **Is Used by DBT**.
   - We also checked whether each table was **referenced in an active dbt model** (via `manifest.json`).
     - ✅ Column: **Is in manifest.json**.

3. **Rate DBT Model Accuracy**
   - We evaluated dbt models based on criteria like **Reliability**, **Importance**, and **Is Accurate**.
     - ✅ Found in `DBT (Audit)`.

4. **Propagate DBT Accuracy to Redshift Tables**
   - We needed to **bring accuracy flags** from dbt models back to the Redshift tables that support them.
     - ✅ New column in `Redshift Tables (Audit)`: **Is Accurate**.
     - 📌 **New ticket**: Write a script to map dbt models → redshift tables and backfill `Is Accurate`.

5. **Define Redshift Table Deprecation Rule**
   - With flags for:
     - Not used in Redshift
     - Not used by dbt
     - Not in manifest
     - Low model accuracy
   - We built a **Deprecation rule** to filter out Redshift tables for removal.
     - ✅ Final column: **Deprecation** in `Redshift Tables (Audit)`.

---

## Step 5: Finalize Dashboard Deprecation

- At this point, we had:
  - Dashboard usage data
  - Explore & View usage data
  - Redshift table flags (usage, model references, accuracy)

- We used this to finalize dashboard deprecation decisions.
  - ✅ Final column: **Final Deprecate** in `Dashboards (Audit)`.

---

## Step 6: Additional Cleanup Work (Outside Dashboard Scope)

- **DBT Models Cleanup**
  - We can shut off dbt models that were flagged above by **deleting their dbt jobs**.
  - ⚠️ This step is **standalone** and **not part of dashboards deprecation**.

- **Redshift Table Cleanup**
  - Similarly, we can plan to **remove Redshift tables** that meet our deprecation criteria.
  - ⚠️ This too is **independent** of dashboards.

- **Reversible Actions First**
  - Uttam recommended starting with **non-destructive steps**, like:
    - Unsharing dashboards
    - Archiving dashboards
  - This ensures a **safe rollback** path and avoids disruptions.

- **Timing Matters**
  - Deprecation actions should ideally be **scheduled before Wednesdays**, to **prevent weekend issues** if anything breaks.
