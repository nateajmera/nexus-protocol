from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import secrets
import nexus_db

app = FastAPI(title="Nexus Protocol MVP")


@app.get("/")
def health_check():
    return {"status": "online", "message": "Nexus Bridge is ready for agents"}


@app.post("/request_access")
def request_access(x_api_key: str = Header(None)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API Key Missing")

    # Look up the user by their key
    user_data = nexus_db.get_user_by_key(x_api_key)

    if not user_data:
        print(f"SECURITY: Blocked access attempt with key: {x_api_key}")
        raise HTTPException(status_code=403, detail="Invalid API Key")

    user_id, balance = user_data
    cost = 10

    if balance >= cost:
        new_balance = balance - cost
        nexus_db.update_balance(user_id, new_balance)

        # Issue the one-time-use token
        new_token = secrets.token_hex(16)
        nexus_db.save_token(new_token, user_id)

        print(f"SUCCESS: {user_id} paid {cost}. New balance: {new_balance}")
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
        print(f"VERIFIED: Token used by {buyer_id}")
        return {"valid": True, "buyer_id": buyer_id}
    return {"valid": False}