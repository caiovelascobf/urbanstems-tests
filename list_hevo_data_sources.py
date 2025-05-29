import os
import csv
import base64
import requests
import json
from dotenv import load_dotenv

# Load API credentials from .env
load_dotenv()
API_KEY = os.getenv("HEVO_API_ACCESS_KEY")
API_SECRET = os.getenv("HEVO_API_SECRET_KEY")
BASE_URL = "https://us2.hevodata.com"
CSV_FILENAME = "hevo_pipelines.csv"

# Encode credentials for Basic Auth
credentials = f"{API_KEY}:{API_SECRET}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()
HEADERS = {
    "Authorization": f"Basic {encoded_credentials}"
}

def get_pipelines():
    url = f"{BASE_URL}/api/public/v2.0/pipelines"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    try:
        return response.json().get("data", [])
    except ValueError:
        print("Received non-JSON response:")
        print(response.text)
        return []

def summarize_pipeline(pipeline):
    return {
        "source_name": pipeline.get("source", {}).get("name"),
        "source_categories": ", ".join(pipeline.get("source", {}).get("type", {}).get("categories", [])),
        "destination_name": pipeline.get("destination", {}).get("name"),
        "destination_status": pipeline.get("destination", {}).get("status"),
        "destination_db_name": pipeline.get("destination", {}).get("config", {}).get("db_name"),
        "destination_db_user": pipeline.get("destination", {}).get("config", {}).get("db_user"),
        "destination_db_port": pipeline.get("destination", {}).get("config", {}).get("db_port"),
        "destination_schema_name": pipeline.get("destination", {}).get("config", {}).get("schema_name"),
        "destination_db_host": pipeline.get("destination", {}).get("config", {}).get("db_host"),
        "destination_schedule_type": pipeline.get("destination", {}).get("schedule", {}).get("type"),
        "destination_schedule": pipeline.get("destination", {}).get("schedule", {}).get("schedule"),
        "pipeline_schedule_type": pipeline.get("schedule", {}).get("type"),
        "pipeline_schedule": pipeline.get("schedule", {}).get("schedule")
    }

def write_to_csv(pipelines, filename):
    if not pipelines:
        print("⚠️ No pipelines found.")
        return

    print(f"📄 Writing {len(pipelines)} pipelines to {filename}...")

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=pipelines[0].keys())
        writer.writeheader()
        writer.writerows(pipelines)

    print("✅ Done.")

def main():
    print("🔍 Fetching Hevo pipelines...")
    raw_pipelines = get_pipelines()
    
    if not raw_pipelines:
        print("No pipelines found.")
        return

    # Print the first pipeline's JSON structure
    print("🔎 Sample pipeline JSON:")
    print(json.dumps(raw_pipelines[0], indent=2))

    summarized = [summarize_pipeline(p) for p in raw_pipelines]
    write_to_csv(summarized, CSV_FILENAME)

if __name__ == "__main__":
    main()
