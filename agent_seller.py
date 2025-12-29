from fastapi import FastAPI, Header, HTTPException
import requests

app = FastAPI(title="Nexus Seller Agent")

# FIXED: Point to your local Bridge instead of Render
BRIDGE_URL = "http://127.0.0.1:8000/verify"


@app.get("/get_data")
def get_data(x_nexus_token: str = Header(None)):
    if not x_nexus_token:
        raise HTTPException(status_code=401, detail="Missing Nexus Token")

    # 1. VERIFICATION: Ask the Bridge if this token is real
    print(f"SELLER: Verifying token {x_nexus_token[:8]}...")

    try:
        # The bridge endpoint is /verify/{token}
        verify_resp = requests.get(f"{BRIDGE_URL}/{x_nexus_token}")

        if verify_resp.status_code != 200:
            print(f"SELLER ERROR: Bridge returned {verify_resp.status_code}")
            raise HTTPException(status_code=403, detail="Bridge verification failed")

        verification = verify_resp.json()

        if verification.get("valid"):
            print(f"✅ SELLER: Token valid! Providing data to {verification.get('buyer_id')}")
            return {
                "status": "success",
                "data": "This is the secret protocol data from the Seller Agent.",
                "buyer_id": verification.get("buyer_id")
            }
        else:
            print("❌ SELLER: Token invalid or already used.")
            raise HTTPException(status_code=403, detail="Invalid Token")

    except Exception as e:
        print(f"SELLER CRASH: {e}")
        raise HTTPException(status_code=500, detail="Internal Seller Error")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001)