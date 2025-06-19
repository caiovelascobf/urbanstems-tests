from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# === Config
EDGE_DRIVER_PATH = "C:/edgedriver/msedgedriver.exe"
STITCH_SOURCES_URL = "https://app.stitchdata.com/client/100557/pipeline/v2/sources"
SOURCE_OUTPUT_CSV = "01_csv_stitch_sources.csv"
MAX_SOURCES = 1  # Process all sources: float('inf'), Set to a number like 10 for testing

# === Setup Edge
driver = webdriver.Edge(service=EdgeService(executable_path=EDGE_DRIVER_PATH))

# === Step 1: Login
driver.get(STITCH_SOURCES_URL)
print("🔐 Please log in manually in the opened Edge window...")
input("✅ Press Enter once you're on the sources page with visible rows: ")
time.sleep(3)

# === Step 2: Scrape source rows
source_data = []

# Wait explicitly for at least one row to load
try:
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.rt-tr"))
    )
except Exception as e:
    print("❌ Failed to detect source rows:", e)
    driver.quit()
    exit()

rows = driver.find_elements(By.CSS_SELECTOR, "div.rt-tr")

for idx, row in enumerate(rows):
    if len(source_data) >= MAX_SOURCES:
        break

    try:
        name_el = row.find_element(By.CSS_SELECTOR, 'a[id^="st-t-name-cell"]')
        name = name_el.text.strip()
        url = name_el.get_attribute("href")

        # Try to get the on/off toggle button, if present
        try:
            button = row.find_element(By.CSS_SELECTOR, 'button[role="switch"]')
            is_on = button.get_attribute("aria-checked") == "true"
            button_state = "On" if is_on else "Off"
        except:
            button_state = ""  # No toggle available

        status = row.find_element(By.CSS_SELECTOR, 'span[id^="st-t-status-cell"]').text.strip()

        source_data.append({
            "Button": button_state,
            "Source Name": name,
            "Status": status,
            "Source URL": url
        })

        print(f"✅ [{len(source_data)}] Found source: {name}")

    except Exception as e:
        print(f"⚠️ Skipped row {idx}, not a valid source:", e)

# === Step 3: Save source-level CSV
df_sources = pd.DataFrame(source_data)
df_sources.to_csv(SOURCE_OUTPUT_CSV, index=False)
print(f"\n✅ Done! Saved {len(df_sources)} sources to {SOURCE_OUTPUT_CSV}")

# === Cleanup
driver.quit()
