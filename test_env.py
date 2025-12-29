import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Load the .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

def test_connection():
    if not url or not key:
        print("❌ FAILED: Could not find variables in .env file.")
        return

    try:
        supabase = create_client(url, key)
        # Try a simple request to the users table
        response = supabase.table("users").select("count", count="exact").limit(1).execute()
        print(f"✅ SUCCESS: Connected to Supabase!")
        print(f"Key used starts with: {key[:8]}...")
    except Exception as e:
        print(f"❌ FAILED: Connection error. Check if your key is correct.")
        print(f"Error details: {e}")

if __name__ == "__main__":
    test_connection()