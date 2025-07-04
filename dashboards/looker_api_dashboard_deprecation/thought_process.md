# Looker Dashboard Deprecation Utility

This script moves specified Looker dashboards into a central **"Deprecated"** folder for archival, cleanup, or audit purposes. It also logs move results, original folder names, and timestamps to a CSV.

---

## 🧾 Overview

- **Script**: `script_02_first_layer_deprecation_api.py`
- **Input CSV**: `raw/dashboards_first_layer_deprecation.csv`
  - **Columns**:
    - `dashboard_name`
    - `dashboard_id`
- **Output Log**: `raw/deprecation_log.csv`
  - **Columns**:
    - `dashboard_name`
    - `dashboard_id`
    - `original_folder`
    - `status`
    - `timestamp`

---

## ⚙️ Behavior

- Connects to Looker using the API credentials in `.env` and `looker.ini`.
- Ensures a **"Deprecated"** folder exists (creates it if missing).
- Moves each dashboard listed in the CSV to the "Deprecated" folder.
- Logs success/failure of each operation and original folder info.
- Automatically skips and logs inaccessible or deleted dashboards.
- Supports a **dry run mode** to preview changes.

---

## 🚦 Dry Run Mode

Enable preview mode without applying changes:

```python
dry_run = True  # inside script_02_first_layer_deprecation_api.py
```

---

## ✅ Running the Script

Run the script from the project folder:

```bash
py script_02_first_layer_deprecation_api.py
```

The script will:
- Print each dashboard’s current folder
- Log results to `raw/deprecation_log.csv`
- Safely skip dashboards that cannot be fetched or moved
- Reuse the existing "Deprecated" folder if already present

---

## 🧪 Tips

- Test a small batch with dry run before running on large dashboard sets.
- You can rerun the script multiple times – it’s safe and idempotent.
- Simply append more dashboards to the CSV file to process them in future runs.
- Check the log file to verify results, troubleshoot failures, or audit the operation.

---
