# nexus_bridge.py (FULL REPLACEMENT - I + T + A)

import os
import hashlib
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
TOKEN_TTL_SECONDS = 600  # 10 minutes
SWEEP_LIMIT = 200

BRIDGE_VERSION = "atomic_rpc_v2_ITA"


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


def _require_admin(x_admin_key: str | None) -> None:
    admin_key = _require(x_admin_key, "x-admin-key")
    expected = os.environ.get("ADMIN_KEY")
    if not expected:
        raise HTTPException(status_code=500, detail="ADMIN_KEY not set on server")
    if admin_key != expected:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active", "version": BRIDGE_VERSION}


@app.post("/request_access")
def request_access(
    request: BuyRequest,
    x_api_key: str = Header(None),
    x_idempotency_key: str = Header(None)
):
    buyer_id = _auth_user_id(_require(x_api_key, "x-api-key"))

    # Perfect mode: idempotency is required
    idem = _require(x_idempotency_key, "x-idempotency-key")

    try:
        rpc_resp = supabase.rpc(
            "nexus_request_access",
            {
                "p_buyer_id": buyer_id,
                "p_seller_id": request.seller_id,
                "p_cost": COST,
                "p_idempotency_key": idem,
                "p_ttl_seconds": TOKEN_TTL_SECONDS,
            },
        ).execute()
    except Exception as e:
        # If buyer has insufficient funds, function raises exception
        msg = str(e)
        if "INSUFFICIENT_BALANCE" in msg:
            raise HTTPException(status_code=402, detail="Insufficient Balance")
        raise HTTPException(status_code=500, detail=f"RPC failure: {e}")

    token = rpc_resp.data
    if not token or not isinstance(token, str):
        raise HTTPException(status_code=500, detail=f"Unexpected RPC response: {rpc_resp.data}")

    print(f"[{BRIDGE_VERSION}] Locked {COST} from {buyer_id} for {request.seller_id} (idem={idem[:8]})", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    seller_id = _auth_user_id(_require(x_seller_api_key, "x-seller-api-key"))

    try:
        rpc_resp = supabase.rpc(
            "nexus_settle_token",
            {
                "p_token": token,
                "p_seller_id": seller_id,
                "p_cost": COST,
            },
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RPC failure: {e}")

    result = rpc_resp.data
    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail=f"Unexpected RPC response: {result}")

    if not result.get("valid"):
        return {
            "valid": False,
            "error": result.get("error")
        }

    print(f"[{BRIDGE_VERSION}] Settled token for seller {seller_id} (atomic)", flush=True)
    return {"valid": True, "buyer_id": result.get("buyer_id"), "already_settled": result.get("already_settled", False)}


@app.post("/sweep_expired")
def sweep_expired(x_admin_key: str = Header(None)):
    _require_admin(x_admin_key)

    try:
        rpc_resp = supabase.rpc(
            "nexus_sweep_expired_tokens",
            {
                "p_limit": SWEEP_LIMIT,
                "p_cost": COST,
                "p_triggered_by": "admin"
            },
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RPC failure: {e}")

    swept = rpc_resp.data
    if not isinstance(swept, int):
        raise HTTPException(status_code=500, detail=f"Unexpected RPC response: {swept}")

    print(f"[{BRIDGE_VERSION}] Swept expired tokens: {swept}", flush=True)
    return {"status": "ok", "swept": swept}
