import hashlib
import uuid
import os
import traceback
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10


class BuyRequest(BaseModel):
    seller_id: str


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Print full stack trace to Render logs
    print("🔥 UNHANDLED EXCEPTION:", repr(exc), flush=True)
    traceback.print_exc()

    # Return a consistent JSON error
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "error_type": type(exc).__name__},
    )


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active"}


@app.post("/request_access")
def request_access(request: BuyRequest, x_api_key: str = Header(None), x_idempotency_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing Idempotency Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()
    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    buyer_id = user_resp.data[0]["user_id"]

    # IMPORTANT: This calls your DB-side RPC (race-safe).
    # If you don't have this RPC, tell me and I'll adapt.
    rpc_resp = supabase.rpc("nexus_request_access", {
        "p_buyer_id": buyer_id,
        "p_seller_id": request.seller_id,
        "p_cost": COST,
        "p_idempotency_key": x_idempotency_key,
        "p_ttl_seconds": int(os.environ.get("TOKEN_TTL_SECONDS", "600")),
    }).execute()

    if not rpc_resp.data:
        # If Supabase returns an error it will throw, but just in case:
        raise HTTPException(status_code=500, detail="RPC returned no data")

    return {"auth_token": rpc_resp.data["auth_token"] if isinstance(rpc_resp.data, dict) else rpc_resp.data[0]["auth_token"]}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    if not x_seller_api_key:
        raise HTTPException(status_code=401, detail="Missing Seller API Key")

    # You already have SELLER_KEY_1 logic somewhere; keep your mapping logic in DB/RPC.
    # We call DB settlement RPC.
    rpc_resp = supabase.rpc("nexus_settle_token", {
        "p_token": token,
        "p_seller_api_key": x_seller_api_key,
        "p_cost": COST,
    }).execute()

    if not rpc_resp.data:
        return {"valid": False, "error": "UNKNOWN"}

    if isinstance(rpc_resp.data, dict):
        return {"valid": bool(rpc_resp.data.get("valid")), "error": rpc_resp.data.get("error")}
    else:
        # list
        row = rpc_resp.data[0]
        return {"valid": bool(row.get("valid")), "error": row.get("error")}


@app.post("/sweep_expired")
def sweep_expired(x_admin_key: str = Header(None)):
    admin = os.environ.get("ADMIN_KEY")
    if not x_admin_key or not admin or x_admin_key != admin:
        raise HTTPException(status_code=401, detail="Unauthorized")

    rpc_resp = supabase.rpc("nexus_sweep_expired_tokens", {
        "p_ttl_seconds": int(os.environ.get("TOKEN_TTL_SECONDS", "600")),
        "p_cost": COST,
        "p_admin_key": x_admin_key,
        "p_max_rows": 5000,
    }).execute()

    if isinstance(rpc_resp.data, dict):
        return {"status": "ok", "swept": int(rpc_resp.data.get("swept", 0))}
    if rpc_resp.data and isinstance(rpc_resp.data, list):
        return {"status": "ok", "swept": int(rpc_resp.data[0].get("swept", 0))}
    return {"status": "ok", "swept": 0}
