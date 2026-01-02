import time
import json
import uuid
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

CONCURRENCY = 20
NUM_REQUESTS = 50

SETTLE_FRACTION = 0.6  # 60% settle, 40% leave to expire
TOKEN_TTL_SECONDS = 600  # must match bridge setting (currently 600)
SWEEP_EVERY_SECONDS = 3

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
    payload = {"seller_id": seller_id}
    r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
    return r.status_code, r.text


def verify_token(token: str):
    headers = {"x-seller-api-key": SELLER_API_KEY}
    r = SESSION.get(f"{VERIFY_URL}/{token}", headers=headers, timeout=TIMEOUT)
    return r.status_code, r.text


def sweep_once():
    # Your bridge currently accepts ANY x-admin-key (it only checks presence).
    headers = {"x-admin-key": "local_test"}
    r = SESSION.post(SWEEP_URL, headers=headers, timeout=TIMEOUT)
    return r.status_code, r.text


def main():
    print("\n=== TEST 2: FAILURE-MODE (leave_some) ===")
    print(f"Requests: {NUM_REQUESTS}  Concurrency: {CONCURRENCY}")
    print(f"Settle fraction: {SETTLE_FRACTION}")
    print(f"Sweep every: {SWEEP_EVERY_SECONDS}s")
    print(f"TTL: {TOKEN_TTL_SECONDS}s\n")

    # 1) Mint tokens
    idems = [f"idem_{uuid.uuid4()}" for _ in range(NUM_REQUESTS)]
    tokens = []

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(request_access, idem) for idem in idems]
        for f in as_completed(futs):
            status, body = f.result()
            if status == 200:
                token = json.loads(body)["auth_token"]
                tokens.append(token)

    print(f"[MINT] minted={len(tokens)}")
    if len(tokens) == 0:
        print("❌ No tokens minted. Stop.")
        return

    # 2) Choose some to settle, leave the rest unredeemed
    random.shuffle(tokens)
    settle_count = int(len(tokens) * SETTLE_FRACTION)
    to_settle = tokens[:settle_count]
    to_leave = tokens[settle_count:]

    print(f"[PLAN] settle={len(to_settle)} leave_unredeemed={len(to_leave)}")

    # 3) Start sweeping in background while settling (sweep should do ~0 now)
    start = time.time()
    next_sweep = start + SWEEP_EVERY_SECONDS

    # 4) Settle chosen tokens (simulate seller verification)
    ok_settles = 0
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(verify_token, t) for t in to_settle]
        for f in as_completed(futs):
            status, body = f.result()
            if status == 200 and '"valid":true' in body.replace(" ", "").lower():
                ok_settles += 1

            now = time.time()
            if now >= next_sweep:
                s_status, s_body = sweep_once()
                # We expect swept=0 during settle phase
                print(f"[SWEEP] status={s_status} body={s_body}")
                next_sweep = now + SWEEP_EVERY_SECONDS

    print(f"[SETTLE] ok={ok_settles}/{len(to_settle)}")

    # 5) Wait for TTL to expire, then final sweep
    wait_seconds = TOKEN_TTL_SECONDS + 10
    print(f"\n[WAIT] waiting {wait_seconds}s for expiry...")
    time.sleep(wait_seconds)

    s_status, s_body = sweep_once()
    print(f"[FINAL SWEEP] status={s_status} body={s_body}")
    print("\nNow run the invariant SQL in Supabase: live_tokens should be 0 and buyer_escrow should be 0.\n")


if __name__ == "__main__":
    main()
