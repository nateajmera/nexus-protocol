import os
import time
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"

CONCURRENCY = 1
NUM_TOKENS = 20
TOKEN_TTL_SECONDS = 15
TIMEOUT = 60


def make_session(max_pool: int) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=max_pool, pool_maxsize=max_pool, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session(max_pool=CONCURRENCY * 2)


def request_access(idem: str, seller_id: str = "seller_01"):
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idem,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": seller_id, "ttl_seconds": TOKEN_TTL_SECONDS}
    r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
    return r.status_code, r.text


def sweep_once(admin_key: str):
    headers = {"x-admin-key": admin_key}
    r = SESSION.post(SWEEP_URL, headers=headers, timeout=TIMEOUT)
    return r.status_code, r.text


def main():
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key:
        print("❌ ADMIN_KEY is not set in this terminal session.")
        print("Run: export ADMIN_KEY='...'\n")
        return

    print("\n=== TEST 3: INVARIANT RECOVERY (mint_only -> sweep) ===")
    print(f"Minting tokens: {NUM_TOKENS}  Concurrency: {CONCURRENCY}")
    print(f"TTL: {TOKEN_TTL_SECONDS}s")
    print(f"ADMIN_KEY starts with: {admin_key[:6]}...\n")

    # 1) Mint tokens (do NOT verify)
    idems = [f"recovery_{uuid.uuid4()}" for _ in range(NUM_TOKENS)]
    minted = 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(request_access, idem) for idem in idems]
        for f in as_completed(futs):
            status, body = f.result()
            if status == 200:
                minted += 1
            else:
                print(f"[MINT FAIL] status={status} body={body}")

    print(f"[MINT] ok={minted}/{NUM_TOKENS}")
    print("Now go to Supabase and run:")
    print("  select count(*) from public.tokens;")
    print("  select escrow_balance from public.users where user_id='agent_buyer_01';\n")

    # 2) Wait then sweep
    wait_seconds = TOKEN_TTL_SECONDS + 10
    print(f"[WAIT] waiting {wait_seconds}s for expiry...")
    time.sleep(wait_seconds)

    s_status, s_body = sweep_once(admin_key)
    print(f"[SWEEP] status={s_status} body={s_body}")

    print("\nNow run invariant SQL again:")
    print("  select count(*) as live_tokens from public.tokens;")
    print("  select escrow_balance from public.users where user_id='agent_buyer_01';\n")


if __name__ == "__main__":
    main()
