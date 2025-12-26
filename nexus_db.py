import os
from supabase import create_client

# We will set these in Render later for security
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(URL, KEY)

def get_user_by_key(api_key):
    import hashlib
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    # Query Supabase
    response = supabase.table("users").select("*").eq("api_key_hash", key_hash).execute()
    return (response.data[0]['user_id'], response.data[0]['balance']) if response.data else None

def update_balance(user_id, new_amount):
    supabase.table("users").update({"balance": new_amount}).eq("user_id", user_id).execute()

def save_token(token, user_id):
    supabase.table("tokens").insert({"token": token, "user_id": user_id}).execute()

def verify_and_burn_token(token):
    response = supabase.table("tokens").select("user_id").eq("token", token).execute()
    if response.data:
        user_id = response.data[0]['user_id']
        supabase.table("tokens").delete().eq("token", token).execute()
        return user_id
    return None