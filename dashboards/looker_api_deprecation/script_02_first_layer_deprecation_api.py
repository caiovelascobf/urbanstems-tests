import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from looker_sdk import init40
from looker_sdk.sdk.api40.models import CreateFolder, WriteDashboard

# Load env vars
load_dotenv()
sdk = init40()

# ➕ DRY RUN toggle
dry_run = False  # Set to True to preview without making changes

# CSV paths
csv_path = r"raw\dashboards_first_layer_deprecation.csv"
log_path = r"raw\deprecation_log.csv"

# ✅ Ensure 'Deprecated' folder exists or create it
def get_or_create_deprecated_folder():
    try:
        folders = sdk.all_folders(fields="id,name")
        for folder in folders:
            if folder.name == "Deprecated":
                print(f"✅ Using existing 'Deprecated' folder: {folder.id}")
                return folder.id
        new_folder = sdk.create_folder(CreateFolder(name="Deprecated", parent_id="1"))
        print(f"🆕 Created new 'Deprecated' folder: {new_folder.id}")
        return new_folder.id
    except Exception as e:
        print(f"❌ Error while getting/creating folder: {e}")
        exit(1)

deprecated_folder_id = get_or_create_deprecated_folder()

# Prepare log
log_entries = []
log_fields = ["dashboard_name", "dashboard_id", "original_folder", "status", "timestamp"]

# 📋 Process dashboards from CSV
with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        dashboard_id = row["dashboard_id"]
        dashboard_name = row["dashboard_name"]
        timestamp = datetime.utcnow().isoformat()
        folder_name = "Unknown"
        status = ""

        try:
            dashboard = sdk.dashboard(dashboard_id)
        except Exception as e:
            status = f"Fetch failed: {e}"
            print(f"❌ Cannot fetch dashboard {dashboard_name} (ID: {dashboard_id}): {e}")
            log_entries.append({
                "dashboard_name": dashboard_name,
                "dashboard_id": dashboard_id,
                "original_folder": folder_name,
                "status": status,
                "timestamp": timestamp
            })
            continue

        if dashboard.folder_id:
            try:
                folder = sdk.folder(dashboard.folder_id)
                folder_name = folder.name
            except:
                folder_name = "Unavailable"

        print(f"📋 {dashboard_name} (ID: {dashboard_id}) — in folder: {folder_name}")

        if dry_run:
            status = f"DRY RUN — would move to 'Deprecated' (ID: {deprecated_folder_id})"
            print(f"🔎 {status}\n")
        else:
            try:
                updated = sdk.update_dashboard(
                    dashboard_id,
                    WriteDashboard(folder_id=deprecated_folder_id)
                )
                status = "Moved to Deprecated"
                print(f"✅ {status}: {updated.title} (ID: {dashboard_id})\n")
            except Exception as e:
                status = f"Move failed: {e}"
                print(f"❌ {status} for {dashboard_name} (ID: {dashboard_id})\n")

        log_entries.append({
            "dashboard_name": dashboard_name,
            "dashboard_id": dashboard_id,
            "original_folder": folder_name,
            "status": status,
            "timestamp": timestamp
        })

# Save log
with open(log_path, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=log_fields)
    writer.writeheader()
    writer.writerows(log_entries)
