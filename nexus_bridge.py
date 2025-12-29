import hashlib
import uuid
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")


class BuyRequest(BaseModel):
    seller_id: str


COST = 10


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active"}


@app.post("/request_access")
def request_access(request: BuyRequest, x_api_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()

    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    user = user_resp.data[0]
    u_id = user['user_id']

    current_balance = user['balance']
    if current_balance < COST:
        raise HTTPException(status_code=402, detail="Insufficient Balance")

    new_balance = current_balance - COST
    new_escrow = (user.get('escrow_balance') or 0) + COST

    supabase.table("users").update({
        "balance": new_balance,
        "escrow_balance": new_escrow
    }).eq("user_id", u_id).execute()

    new_token = str(uuid.uuid4())
    supabase.table("tokens").insert({
        "token": new_token,
        "user_id": u_id,
        "seller_id": request.seller_id
    }).execute()

    print(f"BRIDGE: Locked {COST} from {u_id} for {request.seller_id}", flush=True)
    return {"auth_token": new_token}


@app.get("/verify/{token}")
def verify_token(token: str):
    token_resp = supabase.table("tokens").select("*").eq("token", token).execute()
    if not token_resp.data:
        return {"valid": False}

    token_data = token_resp.data[0]
    buyer_id = token_data['user_id']
    seller_id = token_data['seller_id']

    # 1. PAYOUT LOGIC
    # A. Deduct from Buyer's Escrow
    buyer_resp = supabase.table("users").select("escrow_balance").eq("user_id", buyer_id).execute()
    if buyer_resp.data:
        current_escrow = buyer_resp.data[0].get('escrow_balance') or 0
        supabase.table("users").update({
            "escrow_balance": max(0, current_escrow - COST)
        }).eq("user_id", buyer_id).execute()

    # B. Add to Seller's Total Earned
    seller_resp = supabase.table("users").select("total_earned").eq("user_id", seller_id).execute()
    if seller_resp.data:
        current_earned = seller_resp.data[0].get('total_earned') or 0
        supabase.table("users").update({
            "total_earned": current_earned + COST
        }).eq("user_id", seller_id).execute()

    # 2. NEW: RECORD THE TRANSACTION IN THE LEDGER
    supabase.table("transactions").insert({
        "buyer_id": buyer_id,
        "seller_id": seller_id,
        "amount": COST,
        "token": token
    }).execute()

    # 3. Burn the token
    supabase.table("tokens").delete().eq("token", token).execute()

    print(f"BRIDGE: Payment complete & Ledger updated for {seller_id}", flush=True)
    return {"valid": True, "buyer_id": buyer_id}