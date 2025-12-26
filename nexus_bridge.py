import hashlib
import secrets
from fastapi import FastAPI, HTTPException, Header
import nexus_db

app = FastAPI(title="Nexus Protocol Bridge")

@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is active"}

@app.post("/request_access")
def request_access(x_api_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API Key Missing")

    # 1. HASH THE KEY: Turn "NEXUS_CLIENT_SECRET_123" into the 64-char fingerprint
    hashed_key = hashlib.sha256(x_api_key.encode()).hexdigest()

    # 2. DATABASE LOOKUP: Pass the hash to the DB
    user_data = nexus_db.get_user_by_key(hashed_key)

    if not user_data:
        # This will show up in your Render logs for debugging
        print(f"SECURITY: No match found for hash: {hashed_key}")
        raise HTTPException(status_code=403, detail="Invalid API Key")

    user_id, balance = user_data
    cost = 10

    # 3. PAYMENT LOGIC
    if balance >= cost:
        new_balance = balance - cost
        nexus_db.update_balance(user_id, new_balance)

        # 4. TOKEN GENERATION
        new_token = secrets.token_hex(16)
        nexus_db.save_token(new_token, user_id)

        print(f"SUCCESS: {user_id} paid {cost}. Remaining: {new_balance}")
        return {
            "status": "APPROVED",
            "new_balance": new_balance,
            "auth_token": new_token
        }

    raise HTTPException(status_code=402, detail="Insufficient Nexus Credits")

@app.get("/verify/{token}")
def verify_token(token: str):
    buyer_id = nexus_db.verify_and_burn_token(token)
    if buyer_id:
        return {"valid": True, "buyer_id": buyer_id}
    return {"valid": False}