import requests
from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Seller Agent (The Store)")

# The location of the Bridge's verification line
BRIDGE_VERIFY_URL = "http://127.0.0.1:8000/verify/"

# The secret data the AI is selling
VALUABLE_DATA = "The secret ingredient is... 42."


@app.get("/get_data")
def get_data(x_nexus_token: str = Header(None)):
    """Seller holds the request and calls the Bridge for confirmation"""
    if not x_nexus_token:
        raise HTTPException(status_code=401, detail="Token Missing")

    print(f"STORE: Verifying token {x_nexus_token} with Nexus Bridge...")

    # CALL THE BRIDGE (The Check-Back)
    try:
        response = requests.get(f"{BRIDGE_VERIFY_URL}{x_nexus_token}")
        verification = response.json()
    except Exception:
        raise HTTPException(status_code=503, detail="Nexus Bridge is offline.")

    if verification.get("valid"):
        print(f"STORE: Token valid! Releasing data to {verification['buyer_id']}")
        return {
            "success": True,
            "data": VALUABLE_DATA,
            "verified_for": verification["buyer_id"]
        }
    else:
        print("STORE: Token REJECTED by Bridge.")
        raise HTTPException(status_code=403, detail="Invalid or Expired Nexus Token")