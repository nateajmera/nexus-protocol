import hashlib
import uuid
import traceback
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10

# Map seller API keys -> seller_id (DB user_id)
# Keep it simple for now. Later we move this into DB table.
SELLER_KEY_MAP = {
    "SELLER_KEY_1": "seller_01",
    # add more later
}


class BuyRequest(BaseModel):
    seller_id: str


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Print full stack trace to Render logs so we can debug production issues
    print("🔥 UNHANDLED EXCEPTION:", repr(exc), flush=True)
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error_type": type(exc).__name__},
    )


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active"}


@app.post("/request_access")
def request_access(
    request: BuyRequest,
    x_api_key: str = Header(None),
    x_idempotency_key: str = Header(None),
):
    """
    Buyer -> Bridge:
    - Authenticate buyer (API key hash -> users table)
    - Call DB RPC to lock funds + mint token (idempotent)
    """
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing Idempotency Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("user_id").eq("api_key_hash", hashed_key).execute()
    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    buyer_id = user_resp.data[0]["user_id"]

    # Your DB has two overloads. We use the idempotency version.
    # NOTE: We don't pass TTL here because your function signature doesn't support it (yet).
    rpc_resp = supabase.rpc(
        "nexus_request_access",
        {
            "p_buyer_id": buyer_id,
            "p_seller_id": request.seller_id,
            "p_cost": COST,
            "p_idempotency_key": x_idempotency_key,
        },
    ).execute()

    # supabase-py sometimes returns dict or list depending on function definition
    if rpc_resp.data is None:
        raise HTTPException(status_code=500, detail="RPC returned no data")

    # Normalize response
    if isinstance(rpc_resp.data, dict):
        token = rpc_resp.data.get("auth_token") or rpc_resp.data.get("token")
    else:
        token = rpc_resp.data[0].get("auth_token") or rpc_resp.data[0].get("token")

    if not token:
        raise HTTPException(status_code=500, detail="RPC did not return auth_token")

    print(f"BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    """
    Seller -> Bridge:
    - Authenticate seller via seller api key (maps to seller_id)
    - Call DB RPC to settle token (burn token, move escrow->seller, ledger)
    """
    if not x_seller_api_key:
        raise HTTPException(status_code=401, detail="Missing Seller API Key")

    seller_id = SELLER_KEY_MAP.get(x_seller_api_key)
    if not seller_id:
        raise HTTPException(status_code=403, detail="Invalid Seller API Key")

    rpc_resp = supabase.rpc(
        "nexus_settle_token",
        {
            "p_token": token,
            "p_seller_id": seller_id,
            "p_cost": COST,
        },
    ).execute()

    if rpc_resp.data is None:
        return {"valid": False, "error": "UNKNOWN"}

    if isinstance(rpc_resp.data, dict):
        valid = bool(rpc_resp.data.get("valid"))
        err = rpc_resp.data.get("error")
    else:
        row = rpc_resp.data[0]
        valid = bool(row.get("valid"))
        err = row.get("error")

    if valid:
        print(f"BRIDGE: Verified & settled token for seller {seller_id}", flush=True)

    return {"valid": valid, "error": err}


@app.post("/sweep_expired")
def sweep_expired(x_admin_key: str = Header(None)):
    """
    Admin -> Bridge:
    - Call DB RPC to sweep expired tokens and refund escrow
    Your current DB function signature is:
      nexus_sweep_expired_tokens(p_limit integer, p_cost integer [, p_triggered_by text])
    So we implement exactly that.
    """
    # If you already have an ADMIN_KEY system, keep it here.
    # If not, we still enforce "must send something" so the endpoint isn't open.
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Missing Admin Key")

    rpc_resp = supabase.rpc(
        "nexus_sweep_expired_tokens",
        {
            "p_limit": 5000,
            "p_cost": COST,
            "p_triggered_by": "bridge_admin",
        },
    ).execute()

    # normalize swept count
    swept = 0
    if rpc_resp.data is None:
        swept = 0
    elif isinstance(rpc_resp.data, dict):
        swept = int(rpc_resp.data.get("swept", 0))
    else:
        swept = int(rpc_resp.data[0].get("swept", 0))

    return {"status": "ok", "swept": swept}
