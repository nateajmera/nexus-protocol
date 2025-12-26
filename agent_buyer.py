import requests

BRIDGE_URL = "https://nexus-protocol.onrender.com/request_access"
SELLER_URL = "http://127.0.0.1:8001/get_data"

# The agent's secret master key
API_KEY = "NEXUS_CLIENT_SECRET_123"


def run_transaction():
    print("--- 1. NEXUS: Paying for access ---")
    headers = {"x-api-key": API_KEY}

    resp = requests.post(BRIDGE_URL, headers=headers)

    if resp.status_code == 200:
        token = resp.json()["auth_token"]
        print(f"--- 2. NEXUS: Success. Token: {token} ---")

        # Now talk to the seller
        sell_resp = requests.get(SELLER_URL, headers={"x-nexus-token": token})
        print(f"--- 3. SELLER: Received Data: {sell_resp.json()['data']} ---")
    else:
        print(f"--- FAILED: {resp.json()} ---")


if __name__ == "__main__":
    run_transaction()