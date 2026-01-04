import hashlib
import os
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10


class BuyRequest(BaseModel):
    seller_id: str
    ttl_seconds: int = Field(default=600, ge=5, le=3600)  # allow 5s–1h for testing


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def admin_key_value() -> str:
    return os.environ.get("ADMIN_KEY", "")


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active"}


@app.post("/request_access")
def request_access(
    request: BuyRequest,
    x_api_key: str = Header(None),
    x_idempotency_key: str = Header(None),
):
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing Idempotency Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()
    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    buyer_id = user_resp.data[0]["user_id"]

    # Atomic mint via DB RPC
    try:
        rpc_args = {
            "p_buyer_id": buyer_id,
            "p_seller_id": request.seller_id,
            "p_cost": COST,
            "p_idempotency_key": x_idempotency_key,
            "p_ttl_seconds": request.ttl_seconds,
        }
        rpc_resp = supabase.rpc("nexus_request_access", rpc_args).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RPC failure: {e}")

    if not rpc_resp.data:
        raise HTTPException(status_code=500, detail="RPC returned no data")

    token = None
    if isinstance(rpc_resp.data, list) and len(rpc_resp.data) > 0:
        token = rpc_resp.data[0].get("token") or rpc_resp.data[0].get("auth_token")
    elif isinstance(rpc_resp.data, dict):
        token = rpc_resp.data.get("token") or rpc_resp.data.get("auth_token")

    if not token:
        raise HTTPException(status_code=500, detail={"rpc_data": rpc_resp.data})

    print(
        f"BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id} ttl={request.ttl_seconds}",
        flush=True,
    )
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    if not x_seller_api_key:
        raise HTTPException(status_code=401, detail="Missing x-seller-api-key")

    # MVP mapping (we’ll remove this in step 3)
    if x_seller_api_key == "SELLER_KEY_1":
        caller_seller_id = "seller_01"
    else:
        raise HTTPException(status_code=403, detail="Invalid seller API key")

    try:
        rpc_args = {
            "p_token": token,
            "p_caller_seller_id": caller_seller_id,
            "p_cost": COST,
        }
        rpc_resp = supabase.rpc("nexus_verify_and_settle", rpc_args).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})

    if not rpc_resp.data or len(rpc_resp.data) == 0:
        raise HTTPException(status_code=500, detail="RPC returned no data")

    row = rpc_resp.data[0]
    return {
        "valid": bool(row.get("valid")),
        "buyer_id": row.get("buyer_id"),
        "error": row.get("error"),
    }


@app.post("/sweep_expired")
def sweep_expired(x_admin_key: str = Header(None), x_triggered_by: str = Header(None)):
    req_id = str(uuid.uuid4())[:8]
    expected = admin_key_value()

    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_KEY not configured on server")
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Missing x-admin-key")
    if x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Invalid admin key")

    triggered_by = x_triggered_by or "manual"

    try:
        payload = {"p_limit": 500, "p_cost": COST, "p_triggered_by": triggered_by}
        resp = supabase.rpc("nexus_sweep_expired_tokens", payload).execute()
        swept = int(resp.data or 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})

    print(f"[{now_utc_iso()}] req_id={req_id} SWEEP ok swept={swept} triggered_by={triggered_by}", flush=True)
    return {"status": "ok", "swept": swept}


@app.get("/invariants")
def invariants(
    x_admin_key: str = Header(None),
    buyer_id: str = "agent_buyer_01",
    seller_id: str = "seller_01",
):
    expected = admin_key_value()
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_KEY not configured on server")
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Missing x-admin-key")
    if x_admin_key != expected:
        raise HTTPException(status_code=403, detail="Invalid admin key")

    # Live tokens
    tokens_count_resp = supabase.table("tokens").select("token", count="exact").limit(1).execute()
    live_tokens = int(tokens_count_resp.count or 0)

    # Sum token amounts (fallback to COST if amount missing)
    tokens_sum = 0
    try:
        tokens_rows = supabase.table("tokens").select("amount").execute().data or []
        for r in tokens_rows:
            tokens_sum += int((r.get("amount") or COST))
    except Exception:
        # If schema doesn’t have amount, just estimate
        tokens_sum = live_tokens * COST

    # Buyer
    b = supabase.table("users").select("balance, escrow_balance").eq("user_id", buyer_id).limit(1).execute().data
    if not b:
        raise HTTPException(status_code=404, detail=f"Buyer not found: {buyer_id}")
    buyer_balance = int(b[0].get("balance") or 0)
    buyer_escrow = int(b[0].get("escrow_balance") or 0)

    # Seller
    s = supabase.table("users").select("total_earned, reputation").eq("user_id", seller_id).limit(1).execute().data
    if not s:
        raise HTTPException(status_code=404, detail=f"Seller not found: {seller_id}")
    seller_earned = int(s[0].get("total_earned") or 0)
    seller_rep = int(s[0].get("reputation") or 0)

    return {
        "live_tokens": live_tokens,
        "live_tokens_amount_sum": tokens_sum,
        "buyer": {"user_id": buyer_id, "balance": buyer_balance, "escrow_balance": buyer_escrow},
        "seller": {"user_id": seller_id, "total_earned": seller_earned, "reputation": seller_rep},
    }
