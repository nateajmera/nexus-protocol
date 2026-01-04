import hashlib
import os
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel
from nexus_db import supabase
from pydantic import BaseModel, Field

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
DEFAULT_TTL_SECONDS = 600

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
def request_access(request: BuyRequest, x_api_key: str = Header(None), x_idempotency_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=400, detail="Missing API Key header")

    if not x_idempotency_key:
        # For safety. Your stress tests rely on idempotency.
        raise HTTPException(status_code=400, detail="Missing Idempotency Key header")

    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()
    user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()

    if not user_resp.data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    user = user_resp.data[0]
    buyer_id = user["user_id"]

    # Call the DB RPC that mints tokens safely (this assumes you have it)
    # IMPORTANT: this passes ttl_seconds so expires_at is correct.
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

    # rpc_resp.data can be:
    # - a raw string token (your case)
    # - a list like [{"token": "..."}] or [{"auth_token": "..."}]
    # - a dict like {"token": "..."}
    d = rpc_resp.data

    token = None
    if isinstance(d, str):
        token = d
    elif isinstance(d, list) and len(d) > 0:
        first = d[0]
        if isinstance(first, str):
            token = first
        elif isinstance(first, dict):
            token = first.get("token") or first.get("auth_token") or first.get("nexus_request_access")
    elif isinstance(d, dict):
        token = d.get("token") or d.get("auth_token") or d.get("nexus_request_access")

    if not token:
        raise HTTPException(status_code=500, detail={"rpc_data": d})

    print(f"BRIDGE: Locked {COST} from {buyer_id} for {request.seller_id} ttl={request.ttl_seconds}", flush=True)
    return {"auth_token": token}


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    if not x_seller_api_key:
        raise HTTPException(status_code=401, detail="Missing x-seller-api-key")

    # MVP seller auth mapping
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
def sweep_expired(
    x_admin_key: str = Header(None),
    x_triggered_by: str = Header(None),
):
    req_id = str(uuid.uuid4())[:8]
    try:
        expected = admin_key_value()
        if not expected:
            raise HTTPException(status_code=500, detail="ADMIN_KEY not configured on server")
        if not x_admin_key:
            raise HTTPException(status_code=401, detail="Missing x-admin-key")
        if x_admin_key != expected:
            raise HTTPException(status_code=403, detail="Invalid admin key")

        triggered_by = x_triggered_by or "manual"

        def extract_int(d):
            # Supabase can return:
            # - int
            # - list like [{"nexus_sweep_expired_tokens": 12}]
            # - list like [{"swept": 12}]
            # - dict like {"swept": 12}
            if d is None:
                return 0
            if isinstance(d, int):
                return d
            if isinstance(d, dict):
                for k in ("swept", "nexus_sweep_expired_tokens"):
                    if k in d and d[k] is not None:
                        return int(d[k])
                return 0
            if isinstance(d, list) and len(d) > 0:
                return extract_int(d[0])
            return 0

        # Call canonical 3-arg signature (we just standardized this in DB)
        payload = {"p_limit": 500, "p_cost": COST, "p_triggered_by": triggered_by}
        resp = supabase.rpc("nexus_sweep_expired_tokens", payload).execute()
        swept = extract_int(resp.data)

        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP ok swept={swept} triggered_by={triggered_by}", flush=True)
        return {"status": "ok", "swept": swept}

    except HTTPException as e:
        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP http_error={e.status_code} detail={e.detail}", flush=True)
        raise
    except Exception as e:
        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})
