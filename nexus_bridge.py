# nexus_bridge.py (FULL REPLACEMENT - ATOMIC TX MODE)

import hashlib
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
BRIDGE_VERSION = "atomic_rpc_v1"


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

    # Atomic: balance check + escrow lock + token mint (in DB)
    try:
        rpc_resp = supabase.rpc(
            "nexus_request_access",
            {
                "p_buyer_id": buyer_id,
                "p_seller_id": request.seller_id,
                "p_cost": COST,
            },
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RPC failure: {e}")

    token = None
    # supabase-py returns RPC result in .data
    if isinstance(rpc_resp.data, str):
        token = rpc_resp.data
    elif rpc_resp.data is not None:
        token = rpc_resp.data  # sometimes still a string

    if not token:
        # If the function raised an exception, PostgREST typically returns 400/500, but just in case:
        raise HTTPException(status_code=402, detail="Insufficient Balance")

    print(f"[{BRIDGE_VERSION}] BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    seller_id = _auth_user_id(_require(x_seller_api_key, "x-seller-api-key"))

    # Atomic settlement in DB
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
        # Defensive: if PostgREST returns JSON as string in some cases
        raise HTTPException(status_code=500, detail=f"Unexpected RPC response: {result}")

    if not result.get("valid"):
        return {"valid": False}

    print(f"[{BRIDGE_VERSION}] BRIDGE: Settled token for seller {seller_id} (atomic)", flush=True)
    return {"valid": True, "buyer_id": result.get("buyer_id")}
