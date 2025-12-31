# agent_buyer.py (FULL REPLACEMENT)

import uuid
import requests

BRIDGE_URL = "https://nexus-protocol.onrender.com/request_access"
SELLER_URL = "http://127.0.0.1:8001/get_data"

API_KEY = "TEST_KEY_1"
TARGET_SELLER = "seller_01"


def run_transaction():
    # New idempotency key per purchase attempt (prevents double-lock on retries)
    idem = str(uuid.uuid4())

    print(f"--- 1. NEXUS: Requesting access to {TARGET_SELLER} ---")
    print(f"---    Idempotency Key: {idem} ---")

    headers = {
        "x-api-key": API_KEY,
        "x-idempotency-key": idem
    }
    payload = {"seller_id": TARGET_SELLER}

    try:
        resp = requests.post(BRIDGE_URL, headers=headers, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"--- FAILED: Bridge returned {resp.status_code} - {resp.text} ---")
            return

        token = resp.json()["auth_token"]
        print(f"--- 2. NEXUS: Success! Received Token: {token} ---")

        print("--- 3. SELLER: Requesting data ---")
        sell_resp = requests.get(SELLER_URL, headers={"x-nexus-token": token}, timeout=10)

        if sell_resp.status_code == 200:
            print(f"--- 4. SELLER: Received Data: {sell_resp.json().get('data')} ---")
        else:
            print(f"--- 4. SELLER FAILED: {sell_resp.status_code} - {sell_resp.text} ---")

    except Exception as e:
        print(f"--- ERROR: {e} ---")


if __name__ == "__main__":
    run_transaction()
