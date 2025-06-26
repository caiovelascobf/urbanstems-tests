# 🧩 Step 5: Merging Dashboards with Explore + View Usage Flags

This step connects all the pieces from the previous scripts into a single, actionable dataset that shows **which dashboards depend on which explores and views**, and whether those dependencies are **active or safe to deprecate**.

---

## 🎯 Purpose of This Step

So far, we've:

1. Extracted all defined **explores** from LookML (`script_01`)
2. Flagged unused **explores** based on dashboard and Look usage (`script_02`)
3. Flagged unused **views** based on LookML references (`script_03`)
4. Merged **explores ↔ views** and their usage flags (`script_04`)

> 🔁 Now in **Step 5**, we bring it all together — mapping **dashboards → explores → views → redshift tables**, enriched with usage flags.

---

## 📁 Input Files

| File | Description |
|------|-------------|
| `script_02-dashboards_to_views_to_redshift.csv` | Dashboard metadata joined with LookML + redshift table lineage |
| `script_04-flag_unused_explores_and_views.csv` | Explore and view usage flags for deprecation |

---

## 🛠️ What This Script Does

- Merges both inputs on `model_name`, `explore_name`, and `view_name`
- Adds usage context for each dashboard's data pipeline
- Outputs a file where **each row = one dashboard → one explore → one view → one or more redshift tables**

---

## 📄 Sample Output: `script_05-dashboards_explores_views_usage.csv`

| dashboard_title           | model_name   | explore_name | view_name       | redshift_tables               | explore_used_in_dashboard | explore_used_in_look | is_used_in_either | view_used_in_explore | safe_to_deprecate_explore | safe_to_deprecate_view |
|---------------------------|--------------|---------------|------------------|--------------------------------|----------------------------|----------------------|-------------------|------------------------|----------------------------|-------------------------|
| Weekly Sales Overview     | ToplineSales | sales_data     | sales_data       | analytics.sales_transactions   | True                       | False                | True              | True                   | False                      | False                   |
| Legacy Reporting          | LegacyModel  | old_metrics    | legacy_users     | analytics.legacy_users_table   | False                      | False                | False             | False                  | True                       | True                    |

---

## ✅ Column Definitions

| Column                       | Description |
|------------------------------|-------------|
| `dashboard_title`            | Dashboard name from Looker |
| `model_name`                 | LookML model used |
| `explore_name`               | Explore powering one or more tiles |
| `view_name`                  | View backing the explore |
| `redshift_tables`            | Resolved SQL tables (from `sql_table_name` and derived queries) |
| `explore_used_in_dashboard` | `True` if explore appears in any dashboard tile |
| `explore_used_in_look`      | `True` if explore appears in any saved Look |
| `is_used_in_either`         | Combines dashboard + Look usage |
| `view_used_in_explore`      | `True` if the view is referenced by any explore in LookML |
| `safe_to_deprecate_explore`| `True` if the explore is unused everywhere |
| `safe_to_deprecate_view`   | `True` if view is unused across all explores |

---

## 🧠 Why This Is Valuable

This is your **cross-functional lineage map**:

- Connects dashboards to actual LookML usage
- Highlights dashboards at risk (because they rely on deprecated or unused LookML)
- Enables a cleanup strategy where:
  - ⚠️ You flag dashboards for review or removal
  - ✅ You protect dashboards that rely on critical models

---

## 💡 Use Cases Enabled by This File

| Use Case | How This File Helps |
|----------|---------------------|
| 🧹 Dashboard cleanup | Deprecate dashboards where *all* explores/views are unused |
| 🔍 Root cause analysis | When a view is deprecated, identify which dashboards rely on it |
| 📈 LookML refactoring | Ensure you’re not breaking high-usage dashboards |
| 📦 Data warehouse impact | See exactly which Redshift tables each dashboard pulls from |

---

## 🧭 Where This Fits in the Workflow

This is **Step 5** in your Looker audit:

1. Extract all explores
2. Flag unused explores from System Activity
3. Flag unused views from LookML
4. Merge explores + views with usage flags
5. ✅ Merge with dashboards for full dependency map
6. *(Next)* Integrate with **Content Validator** results for final deprecation safety

---

## ✅ Summary

This step closes the loop between:
- What dashboards are built
- What explores and views power them
- And which LookML components can be safely removed

> You now have a complete, auditable map of your dashboard-to-LookML-to-warehouse lineage — with usage flags at every level.
