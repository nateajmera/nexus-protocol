import os
import time
import uuid
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== CONFIG =====
BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"   # must match your bridge verify() mapping
SELLER_ID = "seller_01"
COST = 10

# Phase 1: hammer mint with SAME idempotency key
MINT_CALLS = 60
MINT_CONCURRENCY = 30

# Phase 2: verify storm against the returned token
VERIFY_CALLS = 300
VERIFY_CONCURRENCY = 60

TIMEOUT = 30


def make_session(max_pool: int) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.15,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=max_pool, pool_maxsize=max_pool, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session(max_pool=max(MINT_CONCURRENCY, VERIFY_CONCURRENCY) * 2)


def safe_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None


def mint_once(idem_key: str):
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idem_key,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": SELLER_ID}
    try:
        r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"EXCEPTION:{type(e).__name__}:{str(e)}"


def verify_once(token: str):
    headers = {"x-seller-api-key": SELLER_API_KEY}
    try:
        r = SESSION.get(f"{VERIFY_URL}/{token}", headers=headers, timeout=TIMEOUT)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"EXCEPTION:{type(e).__name__}:{str(e)}"


def sweep_once():
    admin_key = os.environ.get("ADMIN_KEY", "")
    if not admin_key:
        return 0, "ADMIN_KEY_MISSING"
    headers = {"x-admin-key": admin_key, "x-triggered-by": "test6_combo"}
    try:
        r = SESSION.post(SWEEP_URL, headers=headers, timeout=TIMEOUT)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"EXCEPTION:{type(e).__name__}:{str(e)}"


def main():
    print("\n=== TEST 6: COMBINED STORM (idem mint + verify storm) ===")
    print(f"Bridge: {BRIDGE_BASE}")
    print(f"Mint calls: {MINT_CALLS}  Mint concurrency: {MINT_CONCURRENCY}")
    print(f"Verify calls: {VERIFY_CALLS}  Verify concurrency: {VERIFY_CONCURRENCY}")
    print(f"Seller: {SELLER_ID}")
    print("")

    # --- Phase 1: Mint storm with SAME idempotency key
    idem_key = f"combo_{uuid.uuid4()}"
    print(f"[PHASE 1] Using SAME idempotency key for all mint calls: {idem_key[:18]}...")

    mint_status = {}
    returned_tokens = []

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MINT_CONCURRENCY) as ex:
        futs = [ex.submit(mint_once, idem_key) for _ in range(MINT_CALLS)]
        for f in as_completed(futs):
            status, body = f.result()
            mint_status[status] = mint_status.get(status, 0) + 1

            if status == 200:
                j = safe_json(body)
                if j and "auth_token" in j:
                    returned_tokens.append(j["auth_token"])

    dt = time.time() - t0
    unique_tokens = sorted(set(returned_tokens))

    print(f"[MINT] done in {dt:.2f}s")
    print("[MINT STATUS COUNTS]")
    for k in sorted(mint_status.keys()):
        print(f"  {k}: {mint_status[k]}")

    print(f"Tokens returned: {len(returned_tokens)}")
    print(f"Unique tokens: {len(unique_tokens)}")
    if unique_tokens:
        print(f"Example token: {unique_tokens[0]}")
    print("")

    # Hard gate: must produce exactly one unique token
    if len(unique_tokens) != 1:
        print("❌ FAIL: idempotency mint did not return exactly 1 unique token.")
        return

    token = unique_tokens[0]

    # --- Phase 2: Verify storm
    print("[PHASE 2] VERIFY STORM on the single token (should settle exactly once)")
    verify_status = {}
    outcomes = {}  # key like "valid_true" or "valid_false:ALREADY_USED"
    top_errors = {}

    t1 = time.time()
    with ThreadPoolExecutor(max_workers=VERIFY_CONCURRENCY) as ex:
        futs = [ex.submit(verify_once, token) for _ in range(VERIFY_CALLS)]
        for f in as_completed(futs):
            status, body = f.result()
            verify_status[status] = verify_status.get(status, 0) + 1

            if status == 200:
                j = safe_json(body)
                if j and j.get("valid") is True:
                    outcomes["valid_true"] = outcomes.get("valid_true", 0) + 1
                elif j and j.get("valid") is False:
                    err = j.get("error") or "UNKNOWN"
                    key = f"valid_false:{err}"
                    outcomes[key] = outcomes.get(key, 0) + 1
                else:
                    outcomes["200_unexpected_body"] = outcomes.get("200_unexpected_body", 0) + 1
            else:
                # Track top error bodies (truncate)
                short = body[:120]
                top_errors[short] = top_errors.get(short, 0) + 1

    dt2 = time.time() - t1
    print(f"[VERIFY] done in {dt2:.2f}s")
    print("\n[VERIFY STATUS COUNTS]")
    for k in sorted(verify_status.keys()):
        print(f"  {k}: {verify_status[k]}")

    print("\n[VERIFY OUTCOMES]")
    for k in sorted(outcomes.keys()):
        print(f"  {k}: {outcomes[k]}")

    if top_errors:
        print("\n[TOP ERROR BODIES]")
        for body, count in sorted(top_errors.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  x{count}  {body}")

    # --- Optional: sweep (should sweep 0 because token got settled)
    s_status, s_body = sweep_once()
    print(f"\n[SWEEP AFTER VERIFY] status={s_status} body={s_body}")

    # --- PASS CONDITIONS
    valid_true = outcomes.get("valid_true", 0)
    already_used = outcomes.get("valid_false:ALREADY_USED", 0)

    print("\nPASS CONDITIONS:")
    print("- Mint: Unique tokens == 1")
    print(f"- Verify: valid_true == 1  (actual: {valid_true})")
    print(f"- Verify: ALREADY_USED should be the majority (actual: {already_used})")
    print("- No 500s (or extremely rare). Any duplicate tx errors = FAIL.")
    print("")
    if valid_true == 1 and already_used > 0 and verify_status.get(500, 0) == 0:
        print("✅ TEST 6 PASSED (combined storm behaves correctly)")
    else:
        print("❌ TEST 6 FAILED (inspect status counts / errors above)")
        print("If valid_true > 1 => double-spend bug.")
        print("If you see duplicate tx unique errors => settlement is not atomic.")


if __name__ == "__main__":
    main()
