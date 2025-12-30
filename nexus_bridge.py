# nexus_bridge.py (FULL REPLACEMENT - VERSION STAMP)

import hashlib
import uuid
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
BRIDGE_VERSION = "rep_v1_debug_001"  # <-- MUST show up in logs


class BuyRequest(BaseModel):
    seller_id: str


def _require(value: str | None, header_name: str) -> str:
    if not value:
        raise HTTPException(status_code=400, detail=f"Missing {header_name} header")
    return value


def _auth_user_id(api_key: str) -> str:
    hashed = hashlib.sha256(api_key.encode()).hexdigest()
    resp = supabase.table("users").select("user_id").eq("api_key_hash", hashed).execute()
    if not resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return resp.data[0]["user_id"]


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active", "version": BRIDGE_VERSION}


@app.post("/request_access")
def request_access(request: BuyRequest, x_api_key: str = Header(None)):
    buyer_id = _auth_user_id(_require(x_api_key, "x-api-key"))

    buyer_resp = supabase.table("users").select("balance, escrow_balance").eq("user_id", buyer_id).execute()
    if not buyer_resp.data:
        raise HTTPException(status_code=401, detail="Invalid Buyer")

    buyer = buyer_resp.data[0]
    balance = buyer.get("balance") or 0
    escrow = buyer.get("escrow_balance") or 0

    if balance < COST:
        raise HTTPException(status_code=402, detail="Insufficient Balance")

    supabase.table("users").update({
        "balance": balance - COST,
        "escrow_balance": escrow + COST
    }).eq("user_id", buyer_id).execute()

    token = str(uuid.uuid4())
    supabase.table("tokens").insert({
        "token": token,
        "user_id": buyer_id,
        "seller_id": request.seller_id
    }).execute()

    print(f"[{BRIDGE_VERSION}] BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    seller_id = _auth_user_id(_require(x_seller_api_key, "x-seller-api-key"))

    token_resp = supabase.table("tokens").select("token, user_id, seller_id").eq("token", token).execute()
    if not token_resp.data:
        return {"valid": False}

    row = token_resp.data[0]
    buyer_id = row["user_id"]
    intended_seller_id = row["seller_id"]

    if seller_id != intended_seller_id:
        raise HTTPException(status_code=403, detail="Seller not authorized for this token")

    # Burn token first
    delete_resp = supabase.table("tokens").delete().eq("token", token).execute()
    if delete_resp.data is not None and len(delete_resp.data) == 0:
        return {"valid": False}

    # Deduct buyer escrow
    buyer_escrow_resp = supabase.table("users").select("escrow_balance").eq("user_id", buyer_id).execute()
    buyer_escrow = (buyer_escrow_resp.data[0].get("escrow_balance") or 0) if buyer_escrow_resp.data else 0
    supabase.table("users").update({
        "escrow_balance": max(0, buyer_escrow - COST)
    }).eq("user_id", buyer_id).execute()

    # Read seller earned + rep
    seller_resp = supabase.table("users").select("total_earned, reputation").eq("user_id", intended_seller_id).execute()
    if not seller_resp.data:
        raise HTTPException(status_code=500, detail="Seller not found in users table")

    seller = seller_resp.data[0]
    earned = seller.get("total_earned") or 0
    rep = seller.get("reputation")
    if rep is None:
        print(f"[{BRIDGE_VERSION}] WARNING: seller reputation is NULL; treating as 0", flush=True)
        rep = 0

    upd_resp = supabase.table("users").update({
        "total_earned": earned + COST,
        "reputation": rep + 1
    }).eq("user_id", intended_seller_id).execute()

    print(f"[{BRIDGE_VERSION}] DEBUG: users.update = {upd_resp.data}", flush=True)

    seller_check = supabase.table("users").select("reputation, total_earned").eq("user_id", intended_seller_id).execute()
    print(f"[{BRIDGE_VERSION}] DEBUG: seller after update = {seller_check.data}", flush=True)

    supabase.table("transactions").insert({
        "buyer_id": buyer_id,
        "seller_id": intended_seller_id,
        "amount": COST,
        "token": token
    }).execute()

    print(f"[{BRIDGE_VERSION}] BRIDGE: Verified & settled token for seller {intended_seller_id} (rep +1 expected)", flush=True)
    return {"valid": True, "buyer_id": buyer_id}
