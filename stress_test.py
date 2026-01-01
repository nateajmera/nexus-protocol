import os
import time
import uuid
import random
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"  # token appended
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

CONCURRENCY = 50

# How many request_access calls to make total
NUM_REQUESTS = 50

# Fraction of requests that reuse an existing idempotency key (to test idempotency)
IDEMPOTENCY_REUSE_RATE = 0.20  # 20%

# For each unique token, how many verify attempts (2 means we expect 1 success max)
VERIFY_ATTEMPTS_PER_TOKEN = 2

# Optional: fire a sweep during the test (normally 0 unless you're testing sweeping)
ENABLE_SWEEP_DURING_TEST = False
SWEEP_DELAY_SECONDS = 2  # when to fire sweep after start
ADMIN_KEY = os.environ.get("ADMIN_KEY")  # only needed if sweep enabled

# Request timeout
TIMEOUT = 15


# =========================
# HELPERS
# =========================
@dataclass
class AccessResult:
    ok: bool
    idempotency_key: str
    token: Optional[str]
    status: int
    body: str


@dataclass
class VerifyResult:
    ok: bool
    token: str
    valid: bool
    error: Optional[str]
    status: int
    body: str


def http_post_json(url: str, headers: dict, payload: dict) -> requests.Response:
    return requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)


def http_get(url: str, headers: dict) -> requests.Response:
    return requests.get(url, headers=headers, timeout=TIMEOUT)


def request_access(idempotency_key: str, seller_id: str = "seller_01") -> AccessResult:
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idempotency_key,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": seller_id}
    try:
        r = http_post_json(REQUEST_ACCESS_URL, headers, payload)
        body = r.text
        if r.status_code == 200:
            token = r.json().get("auth_token")
            return AccessResult(True, idempotency_key, token, r.status_code, body)
        return AccessResult(False, idempotency_key, None, r.status_code, body)
    except Exception as e:
        return AccessResult(False, idempotency_key, None, 0, f"EXCEPTION: {e}")


def verify_token(token: str) -> VerifyResult:
    headers = {"x-seller-api-key": SELLER_API_KEY}
    try:
        r = http_get(f"{VERIFY_URL}/{token}", headers)
        body = r.text
        if r.status_code == 200:
            j = r.json()
            valid = bool(j.get("valid"))
            err = j.get("error")
            return VerifyResult(True, token, valid, err, r.status_code, body)
        return VerifyResult(False, token, False, None, r.status_code, body)
    except Exception as e:
        return VerifyResult(False, token, False, None, 0, f"EXCEPTION: {e}")


def sweep_expired() -> Tuple[bool, int, str]:
    if not ADMIN_KEY:
        return False, -1, "ADMIN_KEY missing in environment; cannot sweep"
    try:
        r = requests.post(SWEEP_URL, headers={"x-admin-key": ADMIN_KEY}, timeout=TIMEOUT)
        if r.status_code == 200:
            swept = r.json().get("swept", None)
            return True, int(swept) if swept is not None else -1, r.text
        return False, -1, r.text
    except Exception as e:
        return False, -1, f"EXCEPTION: {e}"


# =========================
# STRESS TEST
# =========================
def main():
    print("\n=== NEXUS STRESS TEST ===")
    print(f"Bridge: {BRIDGE_BASE}")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"NUM_REQUESTS: {NUM_REQUESTS}")
    print(f"Idempotency reuse rate: {int(IDEMPOTENCY_REUSE_RATE * 100)}%")
    print(f"Verify attempts per token: {VERIFY_ATTEMPTS_PER_TOKEN}")
    print(f"Sweep during test: {ENABLE_SWEEP_DURING_TEST}")
    if ENABLE_SWEEP_DURING_TEST:
        print("NOTE: You must export ADMIN_KEY before running.\n")

    start = time.time()

    # Prepare idempotency keys (some repeated)
    base_idems: List[str] = [f"idem_{uuid.uuid4()}" for _ in range(NUM_REQUESTS)]
    idems: List[str] = []

    # Reuse some idempotency keys to verify idempotency behavior
    reuse_count = int(NUM_REQUESTS * IDEMPOTENCY_REUSE_RATE)
    reuse_pool = random.sample(base_idems, k=reuse_count) if reuse_count > 0 else []

    for i in range(NUM_REQUESTS):
        if reuse_pool and random.random() < IDEMPOTENCY_REUSE_RATE:
            idems.append(random.choice(reuse_pool))
        else:
            idems.append(base_idems[i])

    # Optional sweep thread
    sweep_thread = None
    if ENABLE_SWEEP_DURING_TEST:
        def sweep_worker():
            time.sleep(SWEEP_DELAY_SECONDS)
            ok, swept, body = sweep_expired()
            print(f"\n[SWEEP] ok={ok} swept={swept} body={body}\n")

        sweep_thread = threading.Thread(target=sweep_worker, daemon=True)
        sweep_thread.start()

    # 1) Fire request_access concurrently
    access_results: List[AccessResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(request_access, idem, "seller_01") for idem in idems]
        for f in as_completed(futures):
            access_results.append(f.result())

    # Basic stats
    ok_access = [r for r in access_results if r.ok and r.token]
    bad_access = [r for r in access_results if not r.ok]

    print(f"\n[REQUEST_ACCESS] total={len(access_results)} ok={len(ok_access)} bad={len(bad_access)}")

    if bad_access:
        # Show up to 5 failures
        print("\nTop request_access failures:")
        for r in bad_access[:5]:
            print(f"- status={r.status} idem={r.idempotency_key[:16]} body={r.body[:200]}")

    # 2) Check idempotency correctness: same idem -> same token
    idem_to_token: Dict[str, str] = {}
    idem_collisions_bad: List[Tuple[str, str, str]] = []

    for r in ok_access:
        if r.idempotency_key in idem_to_token:
            if idem_to_token[r.idempotency_key] != r.token:
                idem_collisions_bad.append((r.idempotency_key, idem_to_token[r.idempotency_key], r.token))
        else:
            idem_to_token[r.idempotency_key] = r.token

    if idem_collisions_bad:
        print("\n❌ IDEMPOTENCY FAILED: Same idempotency key returned different tokens!")
        for idem, t1, t2 in idem_collisions_bad[:5]:
            print(f"- idem={idem} token1={t1} token2={t2}")
    else:
        print("✅ Idempotency check passed (same idem => same token)")

    unique_tokens = sorted(set([r.token for r in ok_access if r.token]))
    print(f"[TOKENS] unique tokens minted={len(unique_tokens)}")

    if not unique_tokens:
        print("\n❌ No tokens minted. Most likely: buyer ran out of balance or request_access failing.")
        print("Fix: increase buyer balance in Supabase, then rerun.")
        return

    # 3) Verify each token multiple times concurrently
    verify_jobs = []
    for t in unique_tokens:
        for _ in range(VERIFY_ATTEMPTS_PER_TOKEN):
            verify_jobs.append(t)

    verify_results: List[VerifyResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(verify_token, t) for t in verify_jobs]
        for f in as_completed(futures):
            verify_results.append(f.result())

    # Analyze verify results
    by_token: Dict[str, List[VerifyResult]] = {}
    for r in verify_results:
        by_token.setdefault(r.token, []).append(r)

    double_spend_tokens = []
    invalid_unexpected = []
    ok_settlements = 0

    for token, results in by_token.items():
        successes = [x for x in results if x.ok and x.valid]
        ok_settlements += (1 if successes else 0)

        if len(successes) > 1:
            double_spend_tokens.append((token, len(successes)))

        # Anything that isn't valid should ideally be ALREADY_USED or TOKEN_EXPIRED_REFUNDED or SELLER_MISMATCH
        for x in results:
            if x.ok and not x.valid:
                if x.error not in ("ALREADY_USED", "TOKEN_EXPIRED_REFUNDED", "SELLER_MISMATCH", None):
                    invalid_unexpected.append((token, x.error, x.body))

    print(f"\n[VERIFY] total_calls={len(verify_results)} tokens={len(by_token)} settled_tokens={ok_settlements}")

    if double_spend_tokens:
        print("\n❌ DOUBLE SPEND DETECTED: token verified valid more than once!")
        for t, n in double_spend_tokens[:5]:
            print(f"- token={t} valid_count={n}")
    else:
        print("✅ No double-spend: each token had at most one valid settlement")

    if invalid_unexpected:
        print("\n⚠️ Unexpected invalid verify errors (showing up to 5):")
        for t, err, body in invalid_unexpected[:5]:
            print(f"- token={t} error={err} body={body[:200]}")
    else:
        print("✅ Verify error codes look normal")

    # Optional: wait sweep thread
    if sweep_thread:
        sweep_thread.join(timeout=1)

    elapsed = time.time() - start
    print(f"\n=== DONE in {elapsed:.2f}s ===")

    # Final pass/fail
    if idem_collisions_bad or double_spend_tokens:
        print("❌ STRESS TEST FAILED (see errors above)")
    else:
        print("✅ STRESS TEST PASSED")


if __name__ == "__main__":
    main()
