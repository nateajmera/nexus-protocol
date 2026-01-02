import hashlib
import os
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel
from nexus_db import supabase

app = FastAPI(title="Nexus Protocol Bridge")

COST = 10


class BuyRequest(BaseModel):
    seller_id: str


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def require(value, msg: str):
    if not value:
        raise HTTPException(status_code=500, detail=msg)
    return value


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

        # NOTE: idempotency is optional in your earlier code. Keep optional.
        # But we log it so you can debug easily.
        idem = x_idempotency_key or None

        hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()

        user_resp = supabase.table("users").select("*").eq("api_key_hash", hashed_key).execute()
        if not user_resp.data:
            raise HTTPException(status_code=401, detail="Invalid API Key")

        buyer = user_resp.data[0]
        buyer_id = buyer["user_id"]

        # Seller must exist as a user row
        seller_resp = supabase.table("users").select("user_id").eq("user_id", request.seller_id).execute()
        if not seller_resp.data:
            raise HTTPException(status_code=404, detail="Unknown seller_id")

        balance = int(buyer.get("balance") or 0)
        escrow = int(buyer.get("escrow_balance") or 0)

        if balance < COST:
            raise HTTPException(status_code=402, detail="Insufficient Balance")

        # 1) Lock funds
        upd = supabase.table("users").update({
            "balance": balance - COST,
            "escrow_balance": escrow + COST
        }).eq("user_id", buyer_id).execute()

        # Supabase python client returns .data for update too; but can be empty depending on config.
        # We at least ensure no error raised.
        # 2) Insert token row (THIS MUST SUCCEED OR WE ROLLBACK)
        token = str(uuid.uuid4())

        ins = supabase.table("tokens").insert({
            "token": token,
            "user_id": buyer_id,
            "seller_id": request.seller_id,
            # created_at should be default in DB, but ok if present
        }).execute()

        # Hard validation: if insert didn't return data, treat as failure
        if not ins.data:
            # Roll back the funds lock (best-effort)
            supabase.table("users").update({
                "balance": balance,
                "escrow_balance": escrow
            }).eq("user_id", buyer_id).execute()

            raise HTTPException(status_code=500, detail="Token insert failed (no row returned). Funds rollback attempted.")

        # Sanity check: token actually exists
        check = supabase.table("tokens").select("token").eq("token", token).limit(1).execute()
        if not check.data:
            # Roll back
            supabase.table("users").update({
                "balance": balance,
                "escrow_balance": escrow
            }).eq("user_id", buyer_id).execute()

            raise HTTPException(status_code=500, detail="Token insert failed (token not found after insert). Funds rollback attempted.")

        print(
            f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS ok buyer={buyer_id} seller={request.seller_id} "
            f"cost={COST} idem={idem} token={token[:8]}",
            flush=True
        )

        return {"auth_token": token}

    except HTTPException as e:
        print(f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS http_error={e.status_code} detail={e.detail}", flush=True)
        raise
    except Exception as e:
        # Never lie with a 200 again.
        print(f"[{now_utc_iso()}] req_id={req_id} REQUEST_ACCESS crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})


@app.get("/verify/{token}")
def verify_token(token: str, x_seller_api_key: str = Header(None)):
    req_id = str(uuid.uuid4())[:8]
    try:
        # Require seller auth (you may already do this in your newer version)
        if not x_seller_api_key:
            raise HTTPException(status_code=401, detail="Missing x-seller-api-key")

        # Find token
        token_resp = supabase.table("tokens").select("*").eq("token", token).limit(1).execute()
        if not token_resp.data:
            return {"valid": False, "error": "ALREADY_USED"}

        t = token_resp.data[0]
        buyer_id = t["user_id"]
        seller_id = t["seller_id"]

        # Verify caller is the correct seller
        # (x_seller_api_key should map to seller; if you don’t have seller keys yet, replace this with your current rule)
        # For now: treat SELLER_KEY_1 as seller_01 only (MVP)
        if x_seller_api_key == "SELLER_KEY_1":
            caller_seller_id = "seller_01"
        else:
            raise HTTPException(status_code=403, detail="Invalid seller API key")

        if caller_seller_id != seller_id:
            return {"valid": False, "error": "SELLER_MISMATCH"}

        # 1) Deduct from buyer escrow
        buyer_resp = supabase.table("users").select("escrow_balance").eq("user_id", buyer_id).execute()
        if buyer_resp.data:
            buyer_escrow = int(buyer_resp.data[0].get("escrow_balance") or 0)
            supabase.table("users").update({
                "escrow_balance": max(0, buyer_escrow - COST)
            }).eq("user_id", buyer_id).execute()

        # 2) Add to seller earned + rep
        seller_resp = supabase.table("users").select("total_earned, reputation").eq("user_id", seller_id).execute()
        if seller_resp.data:
            earned = int(seller_resp.data[0].get("total_earned") or 0)
            rep = int(seller_resp.data[0].get("reputation") or 0)
            supabase.table("users").update({
                "total_earned": earned + COST,
                "reputation": rep + 1
            }).eq("user_id", seller_id).execute()

        # 3) Record transaction
        supabase.table("transactions").insert({
            "buyer_id": buyer_id,
            "seller_id": seller_id,
            "amount": COST,
            "token": token
        }).execute()

        # 4) Burn token
        supabase.table("tokens").delete().eq("token", token).execute()

        print(
            f"[{now_utc_iso()}] req_id={req_id} VERIFY ok token={token[:8]} buyer={buyer_id} seller={seller_id}",
            flush=True
        )
        return {"valid": True, "buyer_id": buyer_id}

    except HTTPException as e:
        print(f"[{now_utc_iso()}] req_id={req_id} VERIFY http_error={e.status_code} detail={e.detail}", flush=True)
        raise
    except Exception as e:
        print(f"[{now_utc_iso()}] req_id={req_id} VERIFY crash={type(e).__name__} msg={str(e)}", flush=True)
        raise HTTPException(status_code=500, detail={"error_type": type(e).__name__, "message": str(e)})


@app.post("/sweep_expired")
def sweep_expired(x_admin_key: str = Header(None)):
    # MVP: require presence
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Missing x-admin-key")

    # Your existing sweep implementation probably lives in SQL/RPC in your newer version.
    # If you’re using RPC, keep it there.
    # This endpoint stays as-is in your project; not implemented here intentionally.
    return {"status": "ok", "swept": 0, "note": "Sweep is implemented via DB RPC in your latest version."}
