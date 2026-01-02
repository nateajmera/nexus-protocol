import hashlib
import traceback
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from nexus_db import supabase

# This import is important so we can detect Supabase/PostgREST errors cleanly.
from postgrest.exceptions import APIError

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10

SELLER_KEY_MAP = {
    "SELLER_KEY_1": "seller_01",
}


class BuyRequest(BaseModel):
    seller_id: str


def _raise_clean_apierror(e: APIError):
    """
    Convert Supabase/PostgREST APIError into a readable HTTP response.
    This is safe: it doesn't expose your keys, just the DB/RPC error message.
    """
    # e.args[0] is often a dict like {"message": "...", "code": "...", ...}
    payload = None
    try:
        if e.args and isinstance(e.args[0], dict):
            payload = e.args[0]
    except Exception:
        payload = None

    if payload:
        raise HTTPException(
            status_code=500,
            detail={
                "supabase_error": True,
                "message": payload.get("message"),
                "code": payload.get("code"),
                "details": payload.get("details"),
                "hint": payload.get("hint"),
            },
        )
    else:
        raise HTTPException(
            status_code=500,
            detail={"supabase_error": True, "message": str(e)},
        )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Print full stack trace to Render logs
    print("🔥 UNHANDLED EXCEPTION:", repr(exc), flush=True)
    traceback.print_exc()

    # If it's Supabase/PostgREST, return a readable error to the client too
    if isinstance(exc, APIError):
        # We intentionally return the message so you can debug fast
        try:
            payload = exc.args[0] if exc.args else None
            return JSONResponse(
                status_code=500,
                content={
                    "detail": "Supabase RPC/APIError",
                    "payload": payload,
                },
            )
        except Exception:
            return JSONResponse(
                status_code=500,
                content={"detail": "Supabase RPC/APIError", "payload": str(exc)},
            )

    # Otherwise, generic
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
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing Idempotency Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("user_id").eq("api_key_hash", hashed_key).execute()
    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    buyer_id = user_resp.data[0]["user_id"]

    try:
        rpc_resp = supabase.rpc(
            "nexus_request_access",
            {
                "p_buyer_id": buyer_id,
                "p_seller_id": request.seller_id,
                "p_cost": COST,
                "p_idempotency_key": x_idempotency_key,
            },
        ).execute()
    except APIError as e:
        _raise_clean_apierror(e)

    if rpc_resp.data is None:
        raise HTTPException(status_code=500, detail="RPC returned no data")

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
    if not x_seller_api_key:
        raise HTTPException(status_code=401, detail="Missing Seller API Key")

    seller_id = SELLER_KEY_MAP.get(x_seller_api_key)
    if not seller_id:
        raise HTTPException(status_code=403, detail="Invalid Seller API Key")

    try:
        rpc_resp = supabase.rpc(
            "nexus_settle_token",
            {
                "p_token": token,
                "p_seller_id": seller_id,
                "p_cost": COST,
            },
        ).execute()
    except APIError as e:
        _raise_clean_apierror(e)

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
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Missing Admin Key")

    try:
        rpc_resp = supabase.rpc(
            "nexus_sweep_expired_tokens",
            {
                "p_limit": 5000,
                "p_cost": COST,
                "p_triggered_by": "bridge_admin",
            },
        ).execute()
    except APIError as e:
        _raise_clean_apierror(e)

    swept = 0
    if rpc_resp.data is None:
        swept = 0
    elif isinstance(rpc_resp.data, dict):
        swept = int(rpc_resp.data.get("swept", 0))
    else:
        swept = int(rpc_resp.data[0].get("swept", 0))

    return {"status": "ok", "swept": swept}
