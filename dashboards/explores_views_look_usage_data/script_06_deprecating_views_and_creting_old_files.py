import os
import shutil

# === CONFIGURATION ===
LOOKML_ROOT = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_repo\looker-master"
ARCHIVE_FOLDER = r"C:\jobs_repo\brainforge\urbanstems-tests\dashboards\explores_views_look_usage_data\deprecated_views"

# === List of Views to Archive (relative to LOOKML_ROOT) ===
view_paths = [
    r"ForecastViews\tentpole_piece_sku_forecast.view.lkml",
    r"DerivedTables\vday_2021_refunded_orders.view.lkml",
    r"mode_email_migrations\care_buffer_report.view.lkml",
    r"views\snapshot_comparison.view.lkml",
    r"ForecastViews\tentpole_parent_sku_forecast.view.lkml",
    r"KPIViews\kpi_quality.view.lkml",
    r"DerivedTables\364_day_repeat_customers.view.lkml",
    r"interim_map_forecast.view.lkml",
    r"DerivedTables\potential_new_local_fc_derived_table.view.lkml",
    r"views\churn_prediction.view.lkml",
    r"views\local_capacity_adjustments.view.lkml",
    r"ToplineSalesViews\holiday_window_capacities_custom_query.view.lkml",
    r"views\allocation_import.view.lkml",
    r"ToplineSalesViews\usercredits.view.lkml",
    r"DerivedTables\clean_msa_derived_table.view.lkml",
    r"KPIViews\kpi_credits_issued.view.lkml",
    r"KPIViews\fc_daily.view.lkml",
    r"InventoryViews\inventory_used_derived.view.lkml",
    r"views\formatted_data.view.lkml",
    r"InventoryViews\inventory_allocation_derived.view.lkml",
    r"DerivedTables\oms_transactions.view.lkml",
    r"FinanceViews\order_to_lineitem_discrepancy_derived_table.view.lkml",
    r"InventoryViews\inventory_on_hand_derived.view.lkml",
    r"ForecastViews\weekly_forecast.view.lkml",
    r"DerivedTables\TEMP_orders_per_customer.view.lkml",
]

# === Create archive folder if it doesn't exist ===
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

# === Process Files ===
archived = []
missing = []

for rel_path in view_paths:
    source_path = os.path.join(LOOKML_ROOT, rel_path)
    if os.path.isfile(source_path):
        filename = os.path.basename(source_path)
        target_path = os.path.join(ARCHIVE_FOLDER, f"old_{filename}")
        shutil.copy2(source_path, target_path)
        archived.append(rel_path)
    else:
        missing.append(rel_path)

# === Summary ===
print("✅ Archived Views:")
for path in archived:
    print(f"  - {path}")

if missing:
    print("\n⚠️ Missing Views (not found at expected path):")
    for path in missing:
        print(f"  - {path}")
else:
    print("\n🚀 All views successfully archived.")
