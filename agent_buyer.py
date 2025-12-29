import requests

# 1. SETTINGS
# 'request_access' matches the @app.post in your bridge code
BRIDGE_URL = "http://127.0.0.1:8000/request_access"
# Seller usually runs on 8001
SELLER_URL = "http://127.0.0.1:8001/get_data"

# This must be the raw key.
# The Bridge will hash this and check it against 'api_key_hash' in Supabase.
API_KEY = "TEST_KEY_1"


def run_transaction():
    print("--- 1. NEXUS: Requesting access from Bridge ---")

    # Your bridge code requires 'x-api-key' in the headers
    headers = {"x-api-key": API_KEY}

    try:
        # Step A: Talk to the Bridge
        resp = requests.post(BRIDGE_URL, headers=headers)

        if resp.status_code == 200:
            token = resp.json()["auth_token"]
            print(f"--- 2. NEXUS: Success! Received Token: {token} ---")

            # Step B: Talk to the Seller using the token
            print("--- 3. SELLER: Requesting data with token ---")
            sell_resp = requests.get(SELLER_URL, headers={"x-nexus-token": token})

            if sell_resp.status_code == 200:
                print(f"--- 4. SELLER: Received Data: {sell_resp.json().get('data')} ---")
            else:
                print(f"--- 4. SELLER FAILED: {sell_resp.status_code} - {sell_resp.text} ---")

        elif resp.status_code == 401:
            print("--- FAILED: 401 Unauthorized. Your API_KEY doesn't match the hash in the DB. ---")
        elif resp.status_code == 402:
            print("--- FAILED: 402 Insufficient Balance. Add credits in Supabase! ---")
        else:
            print(f"--- FAILED: Bridge returned {resp.status_code} - {resp.text} ---")

    except requests.exceptions.ConnectionError:
        print("--- ERROR: Could not connect to the Bridge. Is Terminal 1 running? ---")
    except Exception as e:
        print(f"--- UNEXPECTED ERROR: {e} ---")


if __name__ == "__main__":
    run_transaction()