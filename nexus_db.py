import os
from supabase import create_client

# These are pulled from your Render Environment Variables
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# Initialize the Supabase client
supabase = create_client(url, key)

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
    """Store the temporary session token for the seller to verify."""
    supabase.table("tokens").insert({"token": token, "user_id": user_id}).execute()

def verify_and_burn_token(token):
    """Check if a token is valid, then delete it so it can't be reused."""
    result = supabase.table("tokens").select("user_id").eq("token", token).execute()
    if result.data:
        user_id = result.data[0]["user_id"]
        # Delete token after use (One-time use protocol)
        supabase.table("tokens").delete().eq("token", token).execute()
        return user_id
    return None