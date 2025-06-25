# 🧱 Looker View Deprecation Analysis — Step-by-Step Guide

This guide explains how we identify **unused views** in Looker LookML files so they can be safely removed or flagged for cleanup. Views are foundational to Looker — they define the fields available in explores, tiles, and Looks.

This step builds upon your previous explore usage audit by analyzing which views are still referenced in active explores.

---

## 🎓 Why This Step Makes Sense in the Deprecation Workflow

In Looker, **views are building blocks**. They define the SQL fields (dimensions, measures, filters) that power queries. But views **can’t be used directly** by dashboards or Looks — they must first be included in an **explore**.

That means:
- If a view **is not referenced in any explore**, it is effectively **disconnected from the user interface**
- Users **cannot access its fields**, unless:
  - It’s indirectly used in Liquid includes or LookML dashboards (edge cases)
- So such views are **dead weight** in your model, increasing maintenance cost, confusion, and validator errors

> ✅ By removing unused views, you:
> - Clean up technical debt
> - Make your model more readable
> - Reduce risk of errors due to legacy logic
> - Speed up model rebuilds and parser performance

This step makes the deprecation audit complete: you’ve audited **entry points (explores)**, and now you’re auditing **field providers (views)**.

---

## ✅ Step 1: Extract All Views from LookML

**📁 Source File:** `script_01-extracting_looker_tables_from_views_and_models.csv`  
This file includes rows for both:
- `.view.lkml` files → defined views  
- `.model.lkml` files → defined explores with references to views  

> You already created this in `script_01`, so no additional System Activity or Looker exports are needed.

---

## ✅ Step 2: Analyze View Usage from Explores

**📁 Script:** `script_03-flag_unused_views.py`  
**📁 Output:** `script_03-flag_unused_views.csv`

This script:
- Separates **all defined views** from the input file
- Extracts **all views used in explores** (via `view_name:` or `join:` clauses)
- Flags which views are **not used in any explore**

### ⚙️ Logic Overview:
```text
1. From the CSV, extract all view names defined in .view.lkml files
2. From explores, extract all views that are:
    - Base views (defined via `view_name:` or defaulted to the explore name)
    - Joined views (via `join:` and optional `from:`)
3. Compare both sets to identify unused views
4. Output a CSV with:
    - All defined views
    - Views used in explores
    - Flags for unused views
```
