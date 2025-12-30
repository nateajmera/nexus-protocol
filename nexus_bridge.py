# nexus_bridge.py (FULL REPLACEMENT - A2 Challenge + Resolution)

import os
import hashlib
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
BRIDGE_VERSION = "rep_v1_challenge_v1"

# Challenge parameters (tune later)
CHALLENGE_STAKE = 1          # buyer pays $1 to file a challenge
SELLER_PENALTY_VALID = 5     # seller rep decreases by 5 on valid challenge


class BuyRequest(BaseModel):
    seller_id: str


class ChallengeOpenRequest(BaseModel):
    token: str
    reason: str | None = None


class ChallengeResolveRequest(BaseModel):
    token: str
    outcome: str  # "valid" or "invalid"


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


def _require_admin(x_admin_key: str | None) -> None:
    admin_key = _require(x_admin_key, "x-admin-key")
    expected = os.environ.get("ADMIN_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_KEY not set on server")
    if admin_key != expected:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/")
def health_check():
    return {
        "status": "online",
        "message": "Nexus Bridge is active",
        "version": BRIDGE_VERSION
    }


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

    # Lock funds
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

    print(f"[{BRIDGE_VERSION}] Locked {COST} from {buyer_id} for {request.seller_id}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    """
    Seller-authenticated redemption.
    Burn token first.
    Pay seller.
    Record transaction.
    Increment seller reputation by +1.
    """
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

    # Pay seller + rep +1
    seller_resp = supabase.table("users").select("total_earned, reputation").eq("user_id", intended_seller_id).execute()
    if not seller_resp.data:
        raise HTTPException(status_code=500, detail="Seller not found in users table")

    seller = seller_resp.data[0]
    earned = seller.get("total_earned") or 0
    rep = seller.get("reputation")
    if rep is None:
        rep = 0

    supabase.table("users").update({
        "total_earned": earned + COST,
        "reputation": rep + 1
    }).eq("user_id", intended_seller_id).execute()

    # Record transaction (challenge fields default)
    supabase.table("transactions").insert({
        "buyer_id": buyer_id,
        "seller_id": intended_seller_id,
        "amount": COST,
        "token": token,
        "challenged": False,
        "challenge_status": None,
        "challenge_stake": 0,
        "challenge_reason": None,
        "challenge_resolved_at": None
    }).execute()

    print(f"[{BRIDGE_VERSION}] Settled token for seller {intended_seller_id} (rep +1)", flush=True)
    return {"valid": True, "buyer_id": buyer_id}


@app.post("/challenge")
def open_challenge(req: ChallengeOpenRequest, x_api_key: str = Header(None)):
    """
    Buyer opens a challenge by staking CHALLENGE_STAKE.
    No buyer reputation exists.
    Invalid challenges do NOT increase seller reputation.
    """
    buyer_id = _auth_user_id(_require(x_api_key, "x-api-key"))

    tx_resp = supabase.table("transactions").select("*").eq("token", req.token).execute()
    if not tx_resp.data:
        raise HTTPException(status_code=404, detail="Transaction not found")

    tx = tx_resp.data[0]
    if tx["buyer_id"] != buyer_id:
        raise HTTPException(status_code=403, detail="Not your transaction")

    if tx.get("challenged"):
        raise HTTPException(status_code=400, detail="Already challenged")

    # Deduct stake from buyer balance
    buyer_resp = supabase.table("users").select("balance").eq("user_id", buyer_id).execute()
    if not buyer_resp.data:
        raise HTTPException(status_code=401, detail="Invalid Buyer")

    balance = buyer_resp.data[0].get("balance") or 0
    if balance < CHALLENGE_STAKE:
        raise HTTPException(status_code=402, detail="Insufficient balance to stake a challenge")

    supabase.table("users").update({
        "balance": balance - CHALLENGE_STAKE
    }).eq("user_id", buyer_id).execute()

    # Mark challenge pending
    supabase.table("transactions").update({
        "challenged": True,
        "challenge_status": "pending",
        "challenge_stake": CHALLENGE_STAKE,
        "challenge_reason": req.reason
    }).eq("token", req.token).execute()

    print(f"[{BRIDGE_VERSION}] Challenge opened for token {req.token[:8]} by {buyer_id}", flush=True)
    return {"status": "challenge_opened", "stake": CHALLENGE_STAKE}


@app.post("/resolve_challenge")
def resolve_challenge(req: ChallengeResolveRequest, x_admin_key: str = Header(None)):
    """
    Admin-only resolution.
    outcome:
      - "valid": seller rep -= SELLER_PENALTY_VALID, buyer stake refunded
      - "invalid": no seller rep increase, buyer stake NOT refunded
    """
    _require_admin(x_admin_key)

    outcome = (req.outcome or "").strip().lower()
    if outcome not in ("valid", "invalid"):
        raise HTTPException(status_code=400, detail="Outcome must be 'valid' or 'invalid'")

    tx_resp = supabase.table("transactions").select("*").eq("token", req.token).execute()
    if not tx_resp.data:
        raise HTTPException(status_code=404, detail="Transaction not found")

    tx = tx_resp.data[0]

    if not tx.get("challenged") or tx.get("challenge_status") != "pending":
        raise HTTPException(status_code=400, detail="Challenge is not pending")

    buyer_id = tx["buyer_id"]
    seller_id = tx["seller_id"]
    stake = tx.get("challenge_stake") or 0

    resolved_at = datetime.now(timezone.utc).isoformat()

    if outcome == "valid":
        # 1) Penalize seller rep (floor at 0)
        seller_resp = supabase.table("users").select("reputation").eq("user_id", seller_id).execute()
        rep = (seller_resp.data[0].get("reputation") or 0) if seller_resp.data else 0
        new_rep = max(0, rep - SELLER_PENALTY_VALID)
        supabase.table("users").update({"reputation": new_rep}).eq("user_id", seller_id).execute()

        # 2) Refund buyer stake
        if stake > 0:
            buyer_resp = supabase.table("users").select("balance").eq("user_id", buyer_id).execute()
            bal = (buyer_resp.data[0].get("balance") or 0) if buyer_resp.data else 0
            supabase.table("users").update({"balance": bal + stake}).eq("user_id", buyer_id).execute()

        # 3) Mark resolved
        supabase.table("transactions").update({
            "challenge_status": "valid",
            "challenge_resolved_at": resolved_at
        }).eq("token", req.token).execute()

        print(f"[{BRIDGE_VERSION}] Challenge VALID for {req.token[:8]}: seller rep -{SELLER_PENALTY_VALID}, stake refunded", flush=True)
        return {"status": "resolved", "outcome": "valid"}

    # outcome == "invalid"
    supabase.table("transactions").update({
        "challenge_status": "invalid",
        "challenge_resolved_at": resolved_at
    }).eq("token", req.token).execute()

    print(f"[{BRIDGE_VERSION}] Challenge INVALID for {req.token[:8]}: no seller rep change, no refund", flush=True)
    return {"status": "resolved", "outcome": "invalid"}
