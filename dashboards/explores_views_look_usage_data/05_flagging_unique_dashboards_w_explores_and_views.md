# 🧾 Step 6: Dashboard-Level Deprecation Summary (Aggregated)

This step merges the full list of Looker dashboards with explore and view usage flags to create a clean, deduplicated output that identifies dashboards safe for deprecation.

---

## 📁 Input Files

1. **`unique_looker_dashboards.csv`**  
   - Master list of all dashboards from Looker Admin  
   - Columns: `dashboard_name`, `dashboard_id`

2. **`script_05-dashboards_explores_views_usage.csv`**  
   - Exploded view of dashboards and their usage of explores and views  
   - Contains row-level flags: `safe_to_deprecate_explore`, `safe_to_deprecate_view`

---

## 🧠 What This Step Does

- Aggregates all explore/view flags **per dashboard**
- Flags each dashboard with:
  - Whether **any** explore or view is unused
  - Whether **all** explores/views are unused
  - A final recommendation: `safe_to_deprecate_dashboard`

- Ensures dashboards with **no usage match at all** (e.g., embedded content or broken references) are still accounted for.

---

## ⚙️ Aggregation Logic

The following logic is applied per unique `dashboard_id`:

| Column                    | Logic                                                              |
|---------------------------|---------------------------------------------------------------------|
| `has_unused_explores`     | At least one explore is unused in this dashboard                   |
| `has_unused_views`        | At least one view is unused in this dashboard                      |
| `all_explores_unused`     | All explores in the dashboard are flagged as unused                |
| `all_views_unused`        | All views in the dashboard are flagged as unused                   |
| `safe_to_deprecate_dashboard` | True if **all** explores **and** all views are unused           |
| `has_usage_match`         | True if the dashboard matched usage data in Step 5                 |

---

## 🧾 Output File

**`script_06_unused_explores_views_with_unique_dashboards.csv`**

| dashboard_name        | dashboard_id | has_unused_explores | has_unused_views | all_explores_unused | all_views_unused | safe_to_deprecate_dashboard | has_usage_match |
|-----------------------|--------------|----------------------|------------------|---------------------|------------------|------------------------------|------------------|
| Weekly Summary        | 1001         | False               | True             | False               | False            | False                        | True             |
| Abandoned Experiments | 1042         | True                | True             | True                | True             | ✅ True                       | True             |
| Legacy Placeholder    | 2009         | NaN                 | NaN              | NaN                 | NaN              | NaN                          | ❌ False         |

---

## 📊 Run Summary

| Metric                     | Value |
|----------------------------|-------|
| Total dashboards evaluated | 836   |
| Dashboards matched to usage data | 566 |
| Dashboards with no usage match   | 270 |
| Dashboards fully safe to deprecate | 0 *(example)* |

---

## 🧠 Why This Step Matters

This gives you a **dashboard-centric audit**, which is key for:

- Stakeholder reviews
- Content cleanup planning
- Visual reporting
- Cross-checking against the Content Validator

Instead of looking at explore/view flags row by row, you now have a **one-line-per-dashboard view** for simple filtering and action.

---

## ✅ Next Steps

- Filter `safe_to_deprecate_dashboard = True` — this gives dashboards that rely **only** on stale LookML.
- Also inspect `has_usage_match = False` — these may be:
  - Using embedded Looks
  - Orphaned dashboards
  - Legacy, broken, or hidden

You now have a complete dashboard audit framework powered by real usage data and model introspection 🚀

---
