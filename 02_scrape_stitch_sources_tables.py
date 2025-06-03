from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import pandas as pd
import time

# === Config
EDGE_DRIVER_PATH = "C:/edgedriver/msedgedriver.exe"
STITCH_SOURCES_URL = "https://app.stitchdata.com/client/100557/pipeline/v2/sources"
TABLE_OUTPUT_CSV = "02_csv_stitch_tables.csv"
MAX_SOURCES = float('inf')  # Process all sources: float('inf') or set to a number like 10 for testing

# === Setup Edge
driver = webdriver.Edge(service=EdgeService(executable_path=EDGE_DRIVER_PATH))

# === Step 1: Login
driver.get(STITCH_SOURCES_URL)
print("🔐 Please log in manually in the opened Edge window...")
input("✅ Press Enter once you're on the sources page with visible rows: ")
time.sleep(2)
start_time = time.time()

# === Step 2: Gather source metadata
raw_rows = driver.find_elements(By.CSS_SELECTOR, "div.rt-tr")
sources_metadata = []

for idx, row in enumerate(raw_rows):
    if len(sources_metadata) >= MAX_SOURCES:
        break

    try:
        name_el = row.find_element(By.CSS_SELECTOR, 'a[id^="st-t-name-cell"]')
        name = name_el.text.strip()
        source_url = name_el.get_attribute("href")

        try:
            button = row.find_element(By.CSS_SELECTOR, 'button[role="switch"]')
            is_on = button.get_attribute("aria-checked") == "true"
            button_state = "On" if is_on else "Off"
        except:
            button_state = ""

        status = row.find_element(By.CSS_SELECTOR, 'span[id^="st-t-status-cell"]').text.strip()

        sources_metadata.append({
            "Source Name": name,
            "Source URL": source_url,
            "Button": button_state,
            "Status": status
        })

        print(f"✅ [{len(sources_metadata)}] Found source: {name}")

    except Exception as e:
        print(f"⚠️ Skipped row {idx}: {e}")

if not sources_metadata:
    print("❌ No valid sources found.")
    driver.quit()
    exit()

# === Step 3: Visit each source’s Tables tab and collect table data
table_data = []

def extract_table_rows():
    rows = []
    try:
        table_rows = driver.find_elements(By.CSS_SELECTOR, "tr.st-table__row--body")
        for tr in table_rows:
            try:
                try:
                    table_name_el = tr.find_element(By.CSS_SELECTOR, 'button[id^="st-t-button-"]')
                except NoSuchElementException:
                    table_name_el = tr.find_element(By.CSS_SELECTOR, 'button[ng-click^="view.switchTableFilter"]')

                table_name = table_name_el.text.strip()
                status_el = tr.find_element(By.CSS_SELECTOR, 'div#st-t-method-cell')
                table_status = status_el.text.strip()

                try:
                    checkbox = tr.find_element(By.CSS_SELECTOR, 'button[id^="st-t-table-checkbox-"]')
                    is_selected = "st-checkbox-button--checked" in checkbox.get_attribute("class")
                    table_selected = "Yes" if is_selected else "No"
                except NoSuchElementException:
                    table_selected = ""

                rows.append({
                    "Table Name": table_name,
                    "Table Status": table_status,
                    "Table Selected": table_selected
                })

            except Exception as e:
                print("⚠️ Skipped one table row:", e)
    except Exception as e:
        print("⚠️ Could not get table rows:", e)
    return rows

def has_next_page():
    try:
        next_btn = driver.find_element(By.ID, "st-t-pagination-button-next")
        return next_btn.is_enabled() and not next_btn.get_attribute("disabled")
    except NoSuchElementException:
        return False

def go_to_next_page():
    try:
        next_btn = driver.find_element(By.ID, "st-t-pagination-button-next")
        if next_btn.is_enabled() and not next_btn.get_attribute("disabled"):
            next_btn.click()
            time.sleep(1.5)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tr.st-table__row--body"))
            )
            return True
        return False
    except Exception:
        return False

for src in sources_metadata:
    source_name = src["Source Name"]
    source_url = src["Source URL"]

    print(f"\n➡️ Opening source page for: {source_name}")
    driver.get(source_url)
    time.sleep(1)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "st-t-nav-tables-to-replicate"))
        )
        time.sleep(1)
        driver.find_element(By.ID, "st-t-nav-tables-to-replicate").click()
        print("🧭 Clicked 'Tables to Replicate' tab")

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.st-table__row--body"))
        )
        time.sleep(1)

        full_table_rows = []

        while True:
            current_rows = extract_table_rows()
            full_table_rows.extend(current_rows)
            if not has_next_page():
                break
            go_to_next_page()

        if not full_table_rows:
            print(f"⚠️ No tables found for {source_name}. Saving blank row.")
            table_data.append({
                "Source Name": source_name,
                "Source URL": source_url,
                "Table Name": "",
                "Table Status": "",
                "Table Selected": ""
            })
            continue

        print(f"📄 Found {len(full_table_rows)} tables for {source_name}")

        for row in full_table_rows:
            table_data.append({
                "Source Name": source_name,
                "Source URL": source_url,
                **row
            })

    except TimeoutException as e:
        print(f"⚠️ Timeout or navigation error for {source_name}. Could not load tables.")
        table_data.append({
            "Source Name": source_name,
            "Source URL": source_url,
            "Table Name": "",
            "Table Status": "Page not loaded",
            "Table Selected": ""
        })
        continue

# === Step 4: Save to CSV
df_tables = pd.DataFrame(table_data)
df_tables.to_csv(TABLE_OUTPUT_CSV, index=False)
print(f"\n✅ Done! Saved {len(df_tables)} entries to {TABLE_OUTPUT_CSV}")

# === Timer Output
end_time = time.time()
elapsed = end_time - start_time
minutes = int(elapsed // 60)
seconds = int(elapsed % 60)
print(f"⏱️ Total time elapsed: {minutes}m {seconds}s")

# === Cleanup
driver.quit()
