import os
from dotenv import load_dotenv
from looker_sdk import init40

# Load environment variables from .env file
load_dotenv()

# Initialize the SDK
sdk = init40()

# Test the connection
me = sdk.me()
print(f"✅ Connected as: {me.display_name} ({me.email})")
