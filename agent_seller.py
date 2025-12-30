# agent_seller.py (FULL REPLACEMENT)

from fastapi import FastAPI, Header, HTTPException
import requests

app = FastAPI(title="Nexus Seller Agent")

BRIDGE_VERIFY_BASE = "https://nexus-protocol.onrender.com/verify"

# This must be hashed into users.api_key_hash for seller_01 in Supabase
SELLER_API_KEY = "SELLER_KEY_1"


@app.get("/get_data")
def get_data(x_nexus_token: str = Header(None)):
    if not x_nexus_token:
        raise HTTPException(status_code=401, detail="Missing Nexus Token")

    print(f"SELLER: Verifying token {x_nexus_token[:8]}...", flush=True)

    try:
        verify_resp = requests.get(
            f"{BRIDGE_VERIFY_BASE}/{x_nexus_token}",
            headers={"x-seller-api-key": SELLER_API_KEY},
            timeout=10
        )

        if verify_resp.status_code != 200:
            print(f"SELLER ERROR: Bridge returned {verify_resp.status_code} - {verify_resp.text}", flush=True)
            raise HTTPException(status_code=403, detail="Bridge verification failed")

        verification = verify_resp.json()
        if verification.get("valid"):
            buyer_id = verification.get("buyer_id")
            print(f"✅ SELLER: Token valid! Providing data to {buyer_id}", flush=True)
            return {
                "status": "success",
                "data": "This is the secret protocol data from the Seller Agent.",
                "buyer_id": buyer_id
            }

        print("❌ SELLER: Token invalid or already used.", flush=True)
        raise HTTPException(status_code=403, detail="Invalid Token")

    except HTTPException:
        raise
    except Exception as e:
        print(f"SELLER CRASH: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Internal Seller Error")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
