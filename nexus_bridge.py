import hashlib
import traceback
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from nexus_db import supabase

from postgrest.exceptions import APIError

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
TOKEN_TTL_SECONDS = 600  # 10 minutes

SELLER_KEY_MAP = {
    "SELLER_KEY_1": "seller_01",
}


class BuyRequest(BaseModel):
    seller_id: str


def _apierror_payload(e: APIError):
    # e.args[0] is usually a dict: {"message": "...", "code": "...", ...}
    try:
        if e.args and isinstance(e.args[0], dict):
            return e.args[0]
    except Exception:
        pass
    return {"message": str(e)}


def _extract_token(data):
    """
    Supabase RPC return shapes vary by how the function is defined.
    We accept:
      - {"auth_token": "..."} or {"token": "..."}
      - [{"auth_token": "..."}] or [{"token": "..."}]
      - "....token-string...."
      - ["....token-string...."]
    """
    if data is None:
        return None

    # Dict
    if isinstance(data, dict):
        return data.get("auth_token") or data.get("token")

    # String
    if isinstance(data, str):
        return data

    # List/Tuple
    if isinstance(data, (list, tuple)) and len(data) > 0:
        first = data[0]
        if isinstance(first, dict):
            return first.get("auth_token") or first.get("token")
        if isinstance(first, str):
            return first

    return None


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    # Always print full traceback to Render logs
    print("🔥 UNHANDLED EXCEPTION:", repr(exc), flush=True)
    traceback.print_exc()

    # If it's Supabase/PostgREST, show payload
    if isinstance(exc, APIError):
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Supabase APIError",
                "payload": _apierror_payload(exc),
            },
        )

    # Otherwise show type + message so your curl is informative
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal Server Error",
            "error_type": type(exc).__name__,
            "message": str(exc),
        },
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
                "p_ttl_seconds": TOKEN_TTL_SECONDS,
            },
        ).execute()
    except APIError as e:
        payload = _apierror_payload(e)
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

    token = _extract_token(rpc_resp.data)

    # Debug print to logs so we know what shape came back
    print(
        f"BRIDGE DEBUG: nexus_request_access returned type={type(rpc_resp.data).__name__} data={rpc_resp.data}",
        flush=True,
    )

    if not token:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "RPC did not return an auth token in a supported shape",
                "returned_type": type(rpc_resp.data).__name__,
                "returned_data": rpc_resp.data,
            },
        )

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
            {"p_token": token, "p_seller_id": seller_id, "p_cost": COST},
        ).execute()
    except APIError as e:
        payload = _apierror_payload(e)
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

    # normalize response
    data = rpc_resp.data
    print(f"BRIDGE DEBUG: nexus_settle_token returned type={type(data).__name__} data={data}", flush=True)

    if data is None:
        return {"valid": False, "error": "UNKNOWN"}

    if isinstance(data, dict):
        valid = bool(data.get("valid"))
        err = data.get("error")
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        valid = bool(data[0].get("valid"))
        err = data[0].get("error")
    else:
        # If DB returns a bare boolean or something unexpected:
        return {"valid": False, "error": "BAD_RPC_SHAPE"}

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
            {"p_limit": 5000, "p_cost": COST, "p_triggered_by": "bridge_admin"},
        ).execute()
    except APIError as e:
        payload = _apierror_payload(e)
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

    data = rpc_resp.data
    print(f"BRIDGE DEBUG: nexus_sweep_expired_tokens returned type={type(data).__name__} data={data}", flush=True)

    swept = 0
    if isinstance(data, dict):
        swept = int(data.get("swept", 0))
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        swept = int(data[0].get("swept", 0))

    return {"status": "ok", "swept": swept}
