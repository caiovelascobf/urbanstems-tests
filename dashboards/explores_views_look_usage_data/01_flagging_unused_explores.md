# 📊 Looker Explore Deprecation Analysis — Step-by-Step Guide

This guide explains how we identify unused **explores** in a Looker project so we can safely clean up and deprecate stale dashboards and LookML components. The process is divided into 3 simple steps, one for each dataset.

---

## ✅ Step 1: Extracting All Defined Explores from LookML

**📁 File:** `script_01-extracting_looker_explores_from_models.csv`  
**Total explores found (example):** 87

We wrote a script to scan the Looker codebase (`looker-master`) for all explores defined in `.model.lkml` files.

Each explore in Looker is a curated entry point into your data. It combines one or more **views** (representing SQL tables or logic) and exposes them for querying.

### Why this matters:
- This gives us a **master list** of all explores that exist in our code.
- These are the only explores we control and could consider removing from the repo.

### Key columns:
| Column             | Meaning                                      |
|--------------------|----------------------------------------------|
| `explore_name`     | The name of the explore (from LookML)        |
| `model_name`       | The Looker model it belongs to               |
| `lkml_file`        | The file where the explore is defined        |
| `sql_table_names`  | Backend tables referenced by the explore     |
| `derived_table_sources` | If views in the explore use SQL subqueries |

---

## ✅ Step 2: Gathering Explore Usage from Dashboards

**📁 File:** `dashboard_explore_look_01_system__activity_dashboard_YYYY-MM-DD.csv`  
**Source:** Looker Admin Panel → System Activity → Dashboard Explore

Each row in this dataset represents a **tile** on a dashboard.

### 🧱 What is a tile?

- A **tile** is a visual block on a dashboard — a chart, table, or number.
- A tile can:
  - Run a **native query** directly from an explore, or
  - Display a **saved Look** that was built on an explore.

### Why this matters:
- If a tile queries an explore (either directly or through a Look), that explore is **actively in use**.
- This dataset helps us identify which explores are used in **dashboards**.

### Key columns:
| Column                    | Meaning                                         |
|---------------------------|-------------------------------------------------|
| `Dashboard ID`            | The dashboard that owns the tile                |
| `Dashboard Title`         | Name of the dashboard                           |
| `Look ID`                 | If present, this tile is based on a saved Look |
| `Query Explore`           | The explore powering this tile                  |
| `Query Model`             | The model the explore belongs to                |

---

## ✅ Step 3: Gathering Explore Usage from Saved Looks

**📁 File:** `dashboard_explore_look_02_system__activity look YYYY-MM-DD.csv`  
**Source:** Looker Admin Panel → System Activity → Look Explore

Looks are **saved queries** that users can share, reuse, or embed. They are not tied to a dashboard unless added as a tile.

### Why this matters:
- Explores used in **Looks** may still be critical to analysts or internal reports.
- If an explore is not used in dashboards or Looks, it becomes a candidate for deprecation.

### Key columns:
| Column            | Meaning                                |
|-------------------|----------------------------------------|
| `Look ID`         | Unique ID of the saved Look            |
| `Look Title`      | Display name of the Look               |
| `Query Explore`   | The explore powering the Look          |
| `Query Model`     | The model the explore belongs to       |

---

## 🔄 How We Merge and Why It Makes Sense

Once we have:
- **Dataset 1:** All explores defined in LookML  
  → from `script_01-extracting_looker_explores_from_models.csv`
- **Dataset 2A and 2B:** Explores used in dashboards and saved Looks  
  → from the `System Activity > Dashboard` and `System Activity > Look` explores

We first **combine 2A and 2B** into a single usage dataset. Then we do a **left join**:

### 🧬 Merge Logic:

```
Dataset 1 (defined explores)
    LEFT JOIN
Combined usage data (from dashboards and Looks)
    ON explore_name + model_name
```

We **merge them using `explore_name` and `model_name` as keys**. This lets us:
- Track which defined explores are actually used
- Identify explores that are never referenced in dashboards or Looks

This merge is what powers our **usage flags** — and ultimately, our ability to confidently deprecate unused explores.

---

## 🧠 Final Output: Explore Usage Flags

After merging and analyzing the datasets, we produce a file like:

**📁 File:** `script_02-flag_unused_explores.csv`  
**Total analyzed (example):** 132  
**Unused explores identified:** 10

### 🔍 Sample Output Table

| lkml_file                      | model_name   | explore_name        | sql_table_names         | used_in_dashboard | used_in_look | is_used_in_either | safe_to_deprecate_explore |
|-------------------------------|--------------|----------------------|--------------------------|-------------------|--------------|-------------------|----------------------------|
| sales.model.lkml              | sales        | orders               | analytics.orders         | True              | False        | True              | False                      |
| marketing.model.lkml          | marketing    | campaign_stats       | analytics.campaigns      | False             | True         | True              | False                      |
| internal_debug.model.lkml     | debug        | test_explore         | dev.test_table           | False             | False        | False             | True                       |
| shipping.model.lkml           | shipping     | deliveries           | analytics.shipments      | True              | True         | True              | False                      |

### ✅ What the flags mean:

| Column                      | Meaning                                                                 |
|-----------------------------|-------------------------------------------------------------------------|
| `used_in_dashboard`         | `True` if the explore appears in **any dashboard tile**                 |
| `used_in_look`              | `True` if the explore is used in a **saved Look**                       |
| `is_used_in_either`         | `True` if the explore is used **anywhere**                              |
| `safe_to_deprecate_explore` | `True` if the explore has **no usage** in dashboards or Looks — a cleanup candidate |

---

## 🔎 Why Script 02 Might Analyze More Explores Than Script 01

You may notice the final audit includes **more explores** than you found in your code. Example:

- Explores found in code: **87**
- Explores found in dashboards/Looks: **132**

This is expected. Here’s why:

| Reason                   | Explanation                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| 🕰️ Legacy references     | Dashboards or Looks may reference explores that were **removed from code** |
| 📦 Other LookML projects | System Activity reports usage across all projects                          |
| 🔄 Timing mismatch       | Some explores may appear in usage but not yet merged into your local code   |

These “extra” explores should be validated using the **Content Validator**, as they may break dashboards or require cleanup.

---

## ✅ Summary

By combining these datasets:
- We identify explores that are:
  - In use and safe
  - Partially used (dashboard-only or Look-only)
  - Fully unused and safe to deprecate
- This feeds into a cleanup strategy that improves Looker governance and model performance.

> This is Step 1 in a broader Looker deprecation and validation workflow.
