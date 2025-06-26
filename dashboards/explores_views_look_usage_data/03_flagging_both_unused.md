# 🔗 Explore–View Dependency Matrix — Deprecation Step 4

This step brings together all the information collected from earlier scripts and merges the usage flags of **explores** and **views** to create a unified view of how your LookML project is structured — and what parts can be safely deprecated.

---

## 🎯 Goal of This Step

While previous scripts independently identified unused explores (Step 2) and unused views (Step 3), this step combines the two to answer critical questions like:

- Does an active explore rely on deprecated views?
- Can we safely remove both the explore and all its views?
- Are there views only used by explores that are themselves unused?

This enables safe and confident LookML cleanup without breaking dashboards or reports.

---

## 📁 File: `script_04-flag_unused_explores_and_views.csv`

This file contains one row for each **explore–view** relationship found in your Looker project. It merges metadata from:

- `script_01`: Explore → View mapping
- `script_02`: Explore usage in dashboards and Looks
- `script_03`: View usage in explores

---

## 🧱 What Each Row Represents

Each row tells you:
> "Explore X (in model Y) depends on view Z, and here’s whether each is used or safe to remove."

---

## 📊 Sample Output

| model_name | explore_name | view_name      | explore_used_in_dashboard | explore_used_in_look | is_used_in_either | view_used_in_explore | safe_to_deprecate_explore | safe_to_deprecate_view |
|------------|----------------|------------------|----------------------------|----------------------|-------------------|------------------------|----------------------------|-------------------------|
| `sales`    | `orders`       | `orders`         | True                       | False                | True              | True                   | False                      | False                   |
| `debug`    | `sandbox`      | `test_metrics`   | False                      | False                | False             | False                  | True                       | True                    |
| `marketing`| `campaign_old` | `campaign_stats` | False                      | False                | False             | True                   | True                       | False                   |

---

## ✅ Column Breakdown

| Column                      | Meaning                                                                 |
|-----------------------------|-------------------------------------------------------------------------|
| `model_name`                | The LookML model where the explore is defined                          |
| `explore_name`              | The explore being analyzed                                              |
| `view_name`                 | A view referenced in the explore                                        |
| `explore_used_in_dashboard`| `True` if explore appears in dashboard tiles                            |
| `explore_used_in_look`     | `True` if used in saved Looks                                           |
| `is_used_in_either`        | Combines dashboard + Look usage                                         |
| `view_used_in_explore`     | `True` if this view is used in any explore across the repo              |
| `safe_to_deprecate_explore`| `True` if the explore is unused anywhere                                |
| `safe_to_deprecate_view`   | `True` if the view is unused across all explores                        |

---

## 🧠 Why This Is Useful

This dataset helps you:

- 🧹 Identify **explores that can be deleted**, along with their views  
- ⚠️ Flag **risky explores** that depend on views no longer in use elsewhere  
- 🔍 Spot **views used only in deprecated explores** — safe to remove in batches  
- 🧬 Map **Explore → View** dependencies for governance and onboarding  

---

## 🧩 How This Fits Into the Workflow

This is **Step 4** in the full deprecation pipeline:

1. **Extract all explores** (`script_01`)
2. **Flag unused explores** based on System Activity (`script_02`)
3. **Flag unused views** based on LookML analysis (`script_03`)
4. ✅ **Merge and analyze explore–view relationships** (`script_04`)
5. *(Next)* Use Content Validator to find broken dashboards and finalize dashboard deprecation

---

## 📌 Summary

You now have:
- A powerful matrix linking explores and views
- Comprehensive flags for usage and deprecation
- The ability to safely remove unused LookML — or warn when removal might break dashboards

> This step creates a bridge between LookML code structure and content usage, forming the foundation for confident cleanup.
