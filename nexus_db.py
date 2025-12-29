import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. Force Python to find the .env file in the same folder as this script
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# 2. Load variables
# We use the NAMES of the variables as defined in your .env file
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# 3. Debugging Print (This will show up in your terminal so you know it worked)
# We print only the first 5 characters to verify it loaded without revealing the secret
if url and key:
    print(f"✅ SUCCESS: Loaded Supabase URL: {url[:8]}...")
    print(f"✅ SUCCESS: Loaded Key starting with: {key[:5]}...")
else:
    print("❌ ERROR: Still could not find SUPABASE_URL or SUPABASE_KEY in .env")
    print(f"Looking for .env at: {env_path}")

# 4. Initialize Client
if not url or not key:
    raise ValueError("Supabase credentials missing. Check .env file.")

supabase: Client = create_client(url, key)

# ... (keep your existing functions below this line) ...
def get_user_by_key(key_hash):
    """Search for a user using the hashed version of their API key."""
    result = supabase.table("users").select("user_id, balance").eq("api_key_hash", key_hash).execute()
    if result.data:
        user = result.data[0]
        return user["user_id"], user["balance"]
    return None

def update_balance(user_id, new_balance):
    """Update the user's credit balance after a purchase."""
    supabase.table("users").update({"balance": new_balance}).eq("user_id", user_id).execute()


def save_token(token, user_id):
    # This sends the token to the database
    result = supabase.table("tokens").insert({"token": token, "user_id": user_id}).execute()
    # DEBUG: Add this line to your code to see if the DB actually accepted it
    print(f"DB DEBUG: Token saved status: {result.data}")

    # DEBUG: Add this line to your code to see if the DB actually accepted it
    print(f"DB DEBUG: Token saved status: {result.data}")
def verify_and_burn_token(token):
    """Check if a token is valid, then delete it so it can't be reused."""
    result = supabase.table("tokens").select("user_id").eq("token", token).execute()
    if result.data:
        user_id = result.data[0]["user_id"]
        # Delete token after use (One-time use protocol)
        supabase.table("tokens").delete().eq("token", token).execute()
        return user_id
    return None