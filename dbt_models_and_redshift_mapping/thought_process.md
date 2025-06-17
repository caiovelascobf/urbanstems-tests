# 🧭 Thought Process: Mapping DBT Models to Looker Dashboards

This guide explains how Looker components relate to DBT models and why we start the mapping at the **view** level — even if our goal is to map **dashboards**.

---

## 🔄 Looker Component Chain

```
Dashboard → Tile → Query → Explore → View → Table (DBT Model)
```

Each dashboard visual (tile) runs a query based on:
- an **Explore** (defined in a model file)
- which uses one or more **Views**
- each view maps to a table — often created by a **DBT model**

---

## 🧱 Looker Concepts

| Component | Description | Relevance to DBT |
|-----------|-------------|------------------|
| **View** | Defines dimensions/measures; maps to a DB table or derived SQL | ✅ Usually uses DBT model output via `sql_table_name` |
| **Explore** | Combines views for querying | ⚠️ Doesn't directly reference DBT |
| **Dashboard** | UI container for visual tiles | ❌ Doesn’t reference DBT directly |
| **Query (Tile)** | Runs using an explore + filters/fields | ❌ Doesn’t reference DBT directly |

---

## ✅ Summary

Even though dashboards are the end product, the **only place where DBT models are explicitly referenced** is inside **views** via `sql_table_name:` or `derived_table:`.

So, to map a DBT model to a dashboard, trace:
1. **DBT model** → `table_name`
2. **LookML View** using that table
3. **Explore** referencing that view
4. **Dashboard tile** built on that explore

