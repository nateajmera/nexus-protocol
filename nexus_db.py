import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Load local .env only if it exists (dev machine). On Render, env vars come from Render UI.
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Supabase credentials missing. Set SUPABASE_URL and SUPABASE_KEY.")

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