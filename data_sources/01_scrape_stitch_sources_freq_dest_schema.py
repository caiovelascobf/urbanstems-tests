from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import pandas as pd
import time

# === Config
EDGE_DRIVER_PATH = "C:/edgedriver/msedgedriver.exe"
STITCH_SOURCES_URL = "https://app.stitchdata.com/client/100557/pipeline/v2/sources"
SOURCE_OUTPUT_CSV = "01_csv_stitch_sources_freq_dest_schema.csv"
MAX_SOURCES = float('inf')  # Process all sources: float('inf') or set to a number like 10 for testing

# === Setup Edge
driver = webdriver.Edge(service=EdgeService(executable_path=EDGE_DRIVER_PATH))

# === Step 1: Login
driver.get(STITCH_SOURCES_URL)
print("🔐 Please log in manually in the opened Edge window...")
input("✅ Press Enter once you're on the sources page with visible rows: ")
time.sleep(3)

# === Step 2: Scrape source rows and settings
source_data = []

try:
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.rt-tr"))
    )
except Exception as e:
    print("❌ Failed to detect source rows:", e)
    driver.quit()
    exit()

idx = 0
while len(source_data) < MAX_SOURCES:
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "div.rt-tr")
        if idx >= len(rows):
            break

        row = rows[idx]

        name_el = row.find_element(By.CSS_SELECTOR, 'a[id^="st-t-name-cell"]')
        name = name_el.text.strip()
        source_url = name_el.get_attribute("href")
        settings_url = source_url + "/edit"

        try:
            button = row.find_element(By.CSS_SELECTOR, 'button[role="switch"]')
            is_on = button.get_attribute("aria-checked") == "true"
            button_state = "On" if is_on else "Off"
        except:
            button_state = ""

        status = row.find_element(By.CSS_SELECTOR, 'span[id^="st-t-status-cell"]').text.strip()

        # === Step 2.1: Get Frequency, Destination, Schema
        freq = "Not found"
        dest = "Not found"
        schema = "Not found"
        try:
            driver.get(settings_url)

            # Wait for frequency slider
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.rc-slider-mark-text-active"))
            )
            time.sleep(1)

            freq_el = driver.find_element(By.CSS_SELECTOR, "span.rc-slider-mark-text-active")
            freq_text = freq_el.text.strip().upper()

            if "1 MIN" in freq_text:
                freq = "every 1 minute"
            elif "30" in freq_text:
                freq = "every 30 minutes"
            elif "1 HR" in freq_text:
                freq = "every 1 hour"
            elif "6" in freq_text:
                freq = "every 6 hours"
            elif "12" in freq_text:
                freq = "every 12 hours"
            elif "24" in freq_text:
                freq = "every 24 hours"
            else:
                freq = f"every {freq_text.lower()}"

            # Destination
            try:
                dest_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.ID, "st-t-target-destination-name"))
                )
                dest = dest_el.text.strip()
            except Exception as e:
                print(f"⚠️ Couldn't get destination for {name}: {e}")

            # DB Schema Name
            try:
                schema_el = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#st-t-integration-name-alias-text-edit-page strong"))
                )
                schema = schema_el.text.strip()
            except Exception as e:
                print(f"⚠️ Couldn't get DB schema name for {name}: {e}")

        except Exception as e:
            print(f"⚠️ Couldn't get settings for {name}: {e}")

        source_data.append({
            "Button": button_state,
            "Source Name": name,
            "Status": status,
            "Source URL": source_url,
            "Frequency": freq,
            "Destination": dest,
            "DB Schema Name": schema
        })

        print(f"✅ [{len(source_data)}] Found source: {name}")

        # Return to source list
        driver.get(STITCH_SOURCES_URL)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.rt-tr"))
        )
        time.sleep(1)

        idx += 1

    except StaleElementReferenceException:
        print(f"♻️ Row {idx} became stale. Retrying...")
        time.sleep(1)
        continue
    except Exception as e:
        print(f"⚠️ Skipped row {idx} due to error: {e}")
        idx += 1
        continue

# === Step 3: Save CSV
df_sources = pd.DataFrame(source_data)
df_sources.to_csv(SOURCE_OUTPUT_CSV, index=False)
print(f"\n✅ Done! Saved {len(df_sources)} sources to {SOURCE_OUTPUT_CSV}")

# === Cleanup
driver.quit()
