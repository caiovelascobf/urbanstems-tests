import os
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Redshift connection details from environment
REDSHIFT_CONFIG = {
    "host": os.getenv("REDSHIFT_HOST"),
    "port": os.getenv("REDSHIFT_PORT", "5439"),
    "dbname": os.getenv("REDSHIFT_DBNAME"),
    "user": os.getenv("REDSHIFT_USER_NAME"),
    "password": os.getenv("REDSHIFT_PASSWORD"),
}

def test_connection(config):
    print("🔌 Attempting connection to Redshift...")
    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname=config["dbname"]
        )
        print("✅ Successfully connected to Redshift.")
        conn.close()
    except OperationalError as e:
        print("❌ Connection failed:")
        print(e)

if __name__ == "__main__":
    test_connection(REDSHIFT_CONFIG)
