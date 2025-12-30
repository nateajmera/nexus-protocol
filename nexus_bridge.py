# nexus_bridge.py (FULL REPLACEMENT)

import hashlib
import uuid
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")


class BuyRequest(BaseModel):
    seller_id: str


COST = 10


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
    return {"status": "online", "message": "Nexus Bridge is active"}


@app.post("/request_access")
def request_access(request: BuyRequest, x_api_key: str = Header(None)):
    # 1) Authenticate buyer
    buyer_api_key = _require(x_api_key, "x-api-key")
    buyer_id = _auth_user_id(buyer_api_key)

    # 2) Load buyer balances
    buyer_resp = supabase.table("users").select("balance, escrow_balance").eq("user_id", buyer_id).execute()
    if not buyer_resp.data:
        raise HTTPException(status_code=401, detail="Invalid Buyer")

    buyer = buyer_resp.data[0]
    balance = buyer.get("balance") or 0
    escrow = buyer.get("escrow_balance") or 0

    if balance < COST:
        raise HTTPException(status_code=402, detail="Insufficient Balance")

    # 3) Lock funds into escrow
    supabase.table("users").update({
        "balance": balance - COST,
        "escrow_balance": escrow + COST
    }).eq("user_id", buyer_id).execute()

    # 4) Mint token tied to buyer + intended seller
    token = str(uuid.uuid4())
    supabase.table("tokens").insert({
        "token": token,
        "user_id": buyer_id,
        "seller_id": request.seller_id
    }).execute()

    print(f"BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    """
    Seller-authenticated redemption.
    Burn token FIRST (safer).
    Record token in transactions ledger.
    """
    # 1) Authenticate seller
    seller_api_key = _require(x_seller_api_key, "x-seller-api-key")
    seller_auth_id = _auth_user_id(seller_api_key)

    # 2) Load token row
    token_resp = supabase.table("tokens").select("token, user_id, seller_id").eq("token", token).execute()
    if not token_resp.data:
        return {"valid": False}

    row = token_resp.data[0]
    buyer_id = row["user_id"]
    intended_seller_id = row["seller_id"]

    # 3) Enforce intended seller
    if seller_auth_id != intended_seller_id:
        raise HTTPException(status_code=403, detail="Seller not authorized for this token")

    # 4) Burn token FIRST (prevents easy double redemption)
    delete_resp = supabase.table("tokens").delete().eq("token", token).execute()
    if delete_resp.data is not None and len(delete_resp.data) == 0:
        # Already deleted by another redemption
        return {"valid": False}

    # 5) Deduct from buyer escrow
    buyer_escrow_resp = supabase.table("users").select("escrow_balance").eq("user_id", buyer_id).execute()
    buyer_escrow = (buyer_escrow_resp.data[0].get("escrow_balance") or 0) if buyer_escrow_resp.data else 0

    supabase.table("users").update({
        "escrow_balance": max(0, buyer_escrow - COST)
    }).eq("user_id", buyer_id).execute()

    # 6) Add to seller total_earned
    seller_resp = supabase.table("users").select("total_earned").eq("user_id", intended_seller_id).execute()
    seller_earned = (seller_resp.data[0].get("total_earned") or 0) if seller_resp.data else 0

    supabase.table("users").update({
        "total_earned": seller_earned + COST
    }).eq("user_id", intended_seller_id).execute()

    # 7) Record transaction (includes token)
    supabase.table("transactions").insert({
        "buyer_id": buyer_id,
        "seller_id": intended_seller_id,
        "amount": COST,
        "token": token
    }).execute()

    print(f"BRIDGE: Verified & settled token for seller {intended_seller_id}", flush=True)
    return {"valid": True, "buyer_id": buyer_id}
