import hashlib
import os
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10
DEFAULT_TTL_SECONDS = 600


class BuyRequest(BaseModel):
    seller_id: str


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
    http_req: Request,
    x_api_key: str = Header(None),
    x_idempotency_key: str = Header(None),
):
    req_id = str(uuid.uuid4())[:8]
    try:
        if not x_api_key:
            raise HTTPException(status_code=400, detail="Missing API Key header")
        if not request.seller_id:
            raise HTTPException(status_code=400, detail="Missing seller_id")

        hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()

        user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()
        if not user_resp.data:
            raise HTTPException(status_code=401, detail="Invalid API Key")

        buyer = user_resp.data[0]
        buyer_id = buyer["user_id"]

        # Ensure seller exists
        seller_resp = supabase.table("users").select("user_id").eq("user_id", request.seller_id).execute()
        if not seller_resp.data:
            raise HTTPException(status_code=404, detail="Unknown seller_id")

        balance = int(buyer.get("balance") or 0)
        escrow = int(buyer.get("escrow_balance") or 0)
        if balance < COST:
            raise HTTPException(status_code=402, detail="Insufficient Balance")

        # Lock funds
        supabase.table("users").update({
            "balance": balance - COST,
            "escrow_balance": escrow + COST
        }).eq("user_id", buyer_id).execute()

        # Mint token row
        token = str(uuid.uuid4())
        ins = supabase.table("tokens").insert({
            "token": token,
            "user_id": buyer_id,
            "seller_id": request.seller_id,
        }).execute()

        if not ins.data:
            # Roll back funds lock (best effort)
            supabase.table("users").update({
                "balance": balance,
                "escrow_balance": escrow
            }).eq("user_id", buyer_id).execute()
            raise HTTPException(status_code=500, detail="Token insert failed. Funds rollback attempted.")

        print(
            f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS ok buyer={buyer_id} seller={request.seller_id} token={token[:8]}",
            flush=True
        )
        return {"auth_token": token}

    except HTTPException as e:
        print(f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS http_error={e.status_code} detail={e.detail}", flush=True)
        raise
    except Exception as e:
        print(f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    req_id = str(uuid.uuid4())[:8]
    try:
        if not x_seller_api_key:
            raise HTTPException(status_code=401, detail="Missing x-seller-api-key")

        # Find token
        token_resp = supabase.table("tokens").select("*").eq("token", token).limit(1).execute()
        if not token_resp.data:
            return {"valid": False, "error": "ALREADY_USED"}

        t = token_resp.data[0]
        buyer_id = t["user_id"]
        seller_id = t["seller_id"]

        # MVP seller auth mapping
        if x_seller_api_key == "SELLER_KEY_1":
            caller_seller_id = "seller_01"
        else:
            raise HTTPException(status_code=403, detail="Invalid seller API key")

        if caller_seller_id != seller_id:
            return {"valid": False, "error": "SELLER_MISMATCH"}

        # Settle escrow + seller earned + rep
        buyer_resp = supabase.table("users").select("escrow_balance").eq("user_id", buyer_id).execute()
        buyer_escrow = int((buyer_resp.data[0].get("escrow_balance") if buyer_resp.data else 0) or 0)
        supabase.table("users").update({
            "escrow_balance": max(0, buyer_escrow - COST)
        }).eq("user_id", buyer_id).execute()

        seller_resp = supabase.table("users").select("total_earned, reputation").eq("user_id", seller_id).execute()
        earned = int((seller_resp.data[0].get("total_earned") if seller_resp.data else 0) or 0)
        rep = int((seller_resp.data[0].get("reputation") if seller_resp.data else 0) or 0)
        supabase.table("users").update({
            "total_earned": earned + COST,
            "reputation": rep + 1
        }).eq("user_id", seller_id).execute()

        supabase.table("transactions").insert({
            "buyer_id": buyer_id,
            "seller_id": seller_id,
            "amount": COST,
            "token": token
        }).execute()

        supabase.table("tokens").delete().eq("token", token).execute()

        print(f"[{now_utc_iso()}] req_id={req_id} VERIFY ok token={token[:8]} buyer={buyer_id} seller={seller_id}", flush=True)
        return {"valid": True, "buyer_id": buyer_id}

    except HTTPException:
        raise
    except Exception as e:
        print(f"[{now_utc_iso()}] req_id={req_id} VERIFY crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})


@app.post("/sweep_expired")
def sweep_expired(
    x_admin_key: str = Header(None),
    x_triggered_by: str = Header(None),
):
    """
    Calls the DB-side RPC nexus_sweep_expired_tokens to:
    - find expired tokens
    - burn them
    - refund escrow
    """
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

        # Prefer the newer RPC signature if it exists
        # Some projects have: (p_limit, p_cost, p_triggered_by)
        # Others have:        (p_limit, p_cost)
        # We attempt 3-arg first, then fall back.

        payload_3 = {"p_limit": 500, "p_cost": COST, "p_triggered_by": triggered_by}
        try:
            resp = supabase.rpc("nexus_sweep_expired_tokens", payload_3).execute()
            swept = int(resp.data or 0)
        except Exception:
            payload_2 = {"p_limit": 500, "p_cost": COST}
            resp = supabase.rpc("nexus_sweep_expired_tokens", payload_2).execute()
            swept = int(resp.data or 0)

        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP ok swept={swept} triggered_by={triggered_by}", flush=True)
        return {"status": "ok", "swept": swept}

    except HTTPException as e:
        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP http_error={e.status_code} detail={e.detail}", flush=True)
        raise
    except Exception as e:
        print(f"[{now_utc_iso()}] req_id={req_id} SWEEP crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})
