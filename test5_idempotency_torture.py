import os
import uuid
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
INVARIANTS_URL = f"{BRIDGE_BASE}/invariants"

BUYER_API_KEY = "TEST_KEY_1"
BUYER_ID = "agent_buyer_01"
SELLER_ID = "seller_01"

CONCURRENCY = 30
TOTAL_CALLS = 50
TIMEOUT = 30

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

def get_invariants(admin_key: str):
    r = SESSION.get(
        f"{INVARIANTS_URL}?buyer_id={BUYER_ID}&seller_id={SELLER_ID}",
        headers={"x-admin-key": admin_key},
        timeout=TIMEOUT,
    )
    return r.status_code, r.text

def request_access_same_idem(idem_key: str):
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idem_key,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": SELLER_ID, "ttl_seconds": 600}
    r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
    return r.status_code, r.text

def main():
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key:
        print("❌ ADMIN_KEY not set. Run: export ADMIN_KEY='...'\n")
        return

    print("\n=== TEST 5: IDEMPOTENCY TORTURE (same idem key) ===")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"Total calls: {TOTAL_CALLS}")

    # Baseline invariants
    s0, b0 = get_invariants(admin_key)
    if s0 != 200:
        print(f"❌ invariants baseline failed: {s0} {b0}")
        return
    base = json.loads(b0)
    base_balance = base["buyer"]["balance"]
    base_escrow = base["buyer"]["escrow_balance"]
    print(f"[BASE] buyer_balance={base_balance} buyer_escrow={base_escrow} live_tokens={base['live_tokens']}")

    # Same idempotency key across ALL calls
    idem = f"idem_torture_{uuid.uuid4()}"
    print(f"Using SAME idempotency key for all calls: {idem[:20]}...")

    tokens = []
    status_counts = Counter()
    errors = Counter()

    t_start = time.time()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(request_access_same_idem, idem) for _ in range(TOTAL_CALLS)]
        for f in as_completed(futs):
            status, body = f.result()
            status_counts[status] += 1

            if status != 200:
                errors[f"{status}:{body[:120]}"] += 1
                continue

            try:
                j = json.loads(body)
                tokens.append(j.get("auth_token"))
            except Exception:
                errors[f"200:non_json:{body[:120]}"] += 1

    dt = time.time() - t_start

    unique_tokens = sorted(set([t for t in tokens if t]))
    print(f"\n[REQUEST_ACCESS] done in {dt:.2f}s")
    print("[STATUS COUNTS]")
    for k, v in status_counts.most_common():
        print(f"  {k}: {v}")

    if errors:
        print("\n[TOP ERRORS]")
        for k, v in errors.most_common(5):
            print(f"  x{v}  {k}")

    print(f"\nTokens returned: {len(tokens)}")
    print(f"Unique tokens: {len(unique_tokens)}")
    if unique_tokens:
        print(f"Example token: {unique_tokens[0]}")

    # Post invariants
    s1, b1 = get_invariants(admin_key)
    if s1 != 200:
        print(f"❌ invariants after failed: {s1} {b1}")
        return
    after = json.loads(b1)
    after_balance = after["buyer"]["balance"]
    after_escrow = after["buyer"]["escrow_balance"]

    print(f"\n[AFTER] buyer_balance={after_balance} buyer_escrow={after_escrow} live_tokens={after['live_tokens']}")

    # Expected deltas: EXACTLY ONCE
    delta_balance = base_balance - after_balance
    delta_escrow = after_escrow - base_escrow

    print(f"\n[DELTAS] balance_decrease={delta_balance} escrow_increase={delta_escrow}")

    print("\nPASS CONDITIONS:")
    print("- Unique tokens == 1")
    print("- balance_decrease == 10")
    print("- escrow_increase == 10")
    print("- All 200 responses returned that same token")

    # Strict checks
    pass_ok = True
    if len(unique_tokens) != 1:
        pass_ok = False
        print("❌ FAIL: Did not return exactly 1 unique token.")
    if delta_balance != 10:
        pass_ok = False
        print("❌ FAIL: Buyer balance changed by more than once.")
    if delta_escrow != 10:
        pass_ok = False
        print("❌ FAIL: Buyer escrow changed by more than once.")

    if pass_ok:
        print("\n✅ TEST 5 PASSED")
    else:
        print("\n❌ TEST 5 FAILED (your idempotency is not safe)")

if __name__ == "__main__":
    main()
