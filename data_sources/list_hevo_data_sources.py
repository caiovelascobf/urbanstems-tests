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
PIPELINE_CSV = "hevo_pipelines.csv"
TABLE_CSV = "hevo_pipelines_table_level.csv"

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

def get_pipeline_objects(pipeline_id):
    url = f"{BASE_URL}/api/public/v2.0/pipelines/{pipeline_id}/objects"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    try:
        return response.json().get("data", [])
    except ValueError:
        print(f"Received non-JSON response for pipeline {pipeline_id}:")
        print(response.text)
        return []

def summarize_pipeline(pipeline):
    return {
        "Status": pipeline.get("status"),
        "Source": pipeline.get("source", {}).get("name"),
        "Schedule Type": pipeline.get("schedule", {}).get("type"),
        "Frequency (Pipeline)": pipeline.get("schedule", {}).get("schedule"),
        "Destination": pipeline.get("destination", {}).get("name"),
        "Frequency (Destination)": pipeline.get("destination", {}).get("schedule", {}).get("schedule"),
        "DB User": pipeline.get("destination", {}).get("config", {}).get("db_user"),
        "DB Name": pipeline.get("destination", {}).get("config", {}).get("db_name"),
        "DB Host": pipeline.get("destination", {}).get("config", {}).get("db_host"),
        "DB Port": pipeline.get("destination", {}).get("config", {}).get("db_port"),
        "Schema Name": pipeline.get("destination", {}).get("config", {}).get("schema_name")
    }

def write_to_csv(data, filename, fieldnames):
    if not data:
        print(f"⚠️ No data to write to {filename}.")
        return

    print(f"📄 Writing {len(data)} records to {filename}...")

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print("✅ Done.")

def main():
    print("🔍 Fetching Hevo pipelines...")
    raw_pipelines = get_pipelines()

    if not raw_pipelines:
        print("No pipelines found.")
        return

    # Print full JSON of raw_pipelines[2] with its objects
    # if len(raw_pipelines) > 2:
    #     selected_pipeline = raw_pipelines[2]
    #     pipeline_id = selected_pipeline.get("id")
    #     try:
    #         objects = get_pipeline_objects(pipeline_id)
    #         selected_pipeline["objects"] = objects
    #         print("\n🔎 Full JSON for raw_pipelines[2] WITH objects:\n")
    #         print(json.dumps(selected_pipeline, indent=2))
    #     except Exception as e:
    #         print(f"Failed to get objects for pipeline {pipeline_id}: {e}")

    # Generate pipeline summary CSV
    summarized = [summarize_pipeline(p) for p in raw_pipelines]
    pipeline_fieldnames = [
        "Status", "Source", "Schedule Type", "Frequency (Pipeline)",
        "Destination", "Frequency (Destination)", "DB User", "DB Name", "DB Host",
        "DB Port", "Schema Name"
    ]
    write_to_csv(summarized, PIPELINE_CSV, pipeline_fieldnames)

    # Generate table-level CSV with object details
    table_data = []
    for pipeline in raw_pipelines:
        pipeline_id = pipeline.get("id")
        pipeline_name = pipeline.get("source", {}).get("name")
        pipeline_status = pipeline.get("status")
        try:
            objects = get_pipeline_objects(pipeline_id)
            for obj in objects:
                table_data.append({
                    "Pipeline Status": pipeline_status,
                    "Pipeline ID": pipeline_id,
                    "Pipeline Name": pipeline_name,
                    "Table Name": obj.get("name"),
                    "Table Status": obj.get("status")
                })
        except requests.HTTPError as e:
            print(f"❌ Failed to fetch objects for pipeline {pipeline_id}: {e}")
            table_data.append({
                "Pipeline Status": pipeline_status,
                "Pipeline ID": pipeline_id,
                "Pipeline Name": pipeline_name,
                "Table Name": "Error fetching objects",
                "Table Status": "N/A"
            })

    table_fieldnames = [
        "Pipeline Status",
        "Pipeline ID",
        "Pipeline Name",
        "Table Name",
        "Table Status"
    ]
    
        # Generate merged final CSV
    final_data = []
    for pipeline in raw_pipelines:
        pipeline_id = pipeline.get("id")
        pipeline_name = pipeline.get("source", {}).get("name")
        pipeline_status = pipeline.get("status")
        try:
            objects = get_pipeline_objects(pipeline_id)
            for obj in objects:
                final_data.append({
                    "Status": pipeline_status,
                    "Source": pipeline_name,
                    "Table Name": obj.get("name"),
                    "Table Status": obj.get("status"),
                    "Schedule Type": pipeline.get("schedule", {}).get("type"),
                    "Frequency (Pipeline)": pipeline.get("schedule", {}).get("schedule"),
                    "Frequency (Destination)": pipeline.get("destination", {}).get("schedule", {}).get("schedule"),
                    "Destination": pipeline.get("destination", {}).get("name"),
                    "DB Name": pipeline.get("destination", {}).get("config", {}).get("db_name"),
                    "DB Schema Name": pipeline.get("destination", {}).get("config", {}).get("schema_name"),
                    "DB User": pipeline.get("destination", {}).get("config", {}).get("db_user"),
                    "DB Port": pipeline.get("destination", {}).get("config", {}).get("db_port"),
                    "DB Host": pipeline.get("destination", {}).get("config", {}).get("db_host")
                })
        except requests.HTTPError as e:
            print(f"❌ Failed to fetch objects for pipeline {pipeline_id}: {e}")
            final_data.append({
                "Status": pipeline_status,
                "Source": pipeline_name,
                "Table Name": "Error fetching objects",
                "Table Status": "N/A",
                "Schedule Type": pipeline.get("schedule", {}).get("type"),
                "Frequency (Pipeline)": pipeline.get("schedule", {}).get("schedule"),
                "Frequency (Destination)": pipeline.get("destination", {}).get("schedule", {}).get("schedule"),
                "Destination": pipeline.get("destination", {}).get("name"),
                "DB Name": pipeline.get("destination", {}).get("config", {}).get("db_name"),
                "DB Schema Name": pipeline.get("destination", {}).get("config", {}).get("schema_name"),
                "DB User": pipeline.get("destination", {}).get("config", {}).get("db_user"),
                "DB Port": pipeline.get("destination", {}).get("config", {}).get("db_port"),
                "DB Host": pipeline.get("destination", {}).get("config", {}).get("db_host")
            })

    final_fieldnames = [
        "Status", "Source", "Table Name", "Table Status", "Schedule Type",
        "Frequency (Pipeline)", "Frequency (Destination)", "Destination",
        "DB Name", "DB Schema Name", "DB User", "DB Port", "DB Host"
    ]
    write_to_csv(final_data, "hevo_pipelines_final.csv", final_fieldnames)

if __name__ == "__main__":
    main()


