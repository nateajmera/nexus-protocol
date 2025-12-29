import requests

# SETTINGS
BRIDGE_URL = "http://127.0.0.1:8000/request_access"
SELLER_URL = "http://127.0.0.1:8001/get_data"
API_KEY = "TEST_KEY_1"

# NEW: Define who we want to buy from
TARGET_SELLER = "seller_01"


def run_transaction():
    print(f"--- 1. NEXUS: Requesting access to {TARGET_SELLER} ---")

    headers = {"x-api-key": API_KEY}

    # NEW: Send the seller_id in the body of the request
    payload = {"seller_id": TARGET_SELLER}

    try:
        # We now send 'json=payload' along with the headers
        resp = requests.post(BRIDGE_URL, headers=headers, json=payload)

        if resp.status_code == 200:
            token = resp.json()["auth_token"]
            print(f"--- 2. NEXUS: Success! Received Token: {token} ---")

            # Talk to the Seller
            print("--- 3. SELLER: Requesting data ---")
            sell_resp = requests.get(SELLER_URL, headers={"x-nexus-token": token})

            if sell_resp.status_code == 200:
                print(f"--- 4. SELLER: Received Data: {sell_resp.json().get('data')} ---")
            else:
                print(f"--- 4. SELLER FAILED: {sell_resp.status_code} ---")
        else:
            print(f"--- FAILED: Bridge returned {resp.status_code} - {resp.text} ---")

    except Exception as e:
        print(f"--- ERROR: {e} ---")


if __name__ == "__main__":
    run_transaction()