import os
import time
import uuid
import random
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

CONCURRENCY = 5
NUM_REQUESTS = 50
VERIFY_ATTEMPTS_PER_TOKEN = 2

SWEEP_EVERY_SECONDS = 3
TIMEOUT = 20

@dataclass
class AccessResult:
    ok: bool
    idem: str
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

def request_access(idem: str, seller_id: str = "seller_01") -> AccessResult:
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idem,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": seller_id}
    try:
        r = requests.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
        body = r.text
        if r.status_code == 200:
            token = r.json().get("auth_token")
            return AccessResult(True, idem, token, r.status_code, body)
        return AccessResult(False, idem, None, r.status_code, body)
    except Exception as e:
        return AccessResult(False, idem, None, 0, f"EXCEPTION: {e}")

def verify_token(token: str) -> VerifyResult:
    headers = {"x-seller-api-key": SELLER_API_KEY}
    try:
        r = requests.get(f"{VERIFY_URL}/{token}", headers=headers, timeout=TIMEOUT)
        body = r.text
        if r.status_code == 200:
            j = r.json()
            return VerifyResult(True, token, bool(j.get("valid")), j.get("error"), r.status_code, body)
        return VerifyResult(False, token, False, None, r.status_code, body)
    except Exception as e:
        return VerifyResult(False, token, False, None, 0, f"EXCEPTION: {e}")

def sweep() -> int:
    admin = os.environ.get("ADMIN_KEY")
    if not admin:
        raise RuntimeError("ADMIN_KEY missing. Run: export ADMIN_KEY='...'")
    r = requests.post(SWEEP_URL, headers={"x-admin-key": admin}, timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Sweep failed: {r.status_code} {r.text}")
    return int(r.json().get("swept", 0))

def main():
    print("\n=== NEXUS REAL-WORLD STRESS TEST (settle_all) ===")
    print(f"Bridge: {BRIDGE_BASE}")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"Requests: {NUM_REQUESTS}")
    print(f"Verify attempts per token: {VERIFY_ATTEMPTS_PER_TOKEN}")
    print(f"Sweep every: {SWEEP_EVERY_SECONDS}s (should sweep 0 if all settle)\n")

    start = time.time()

    # Mint tokens
    idems = [f"idem_{uuid.uuid4()}" for _ in range(NUM_REQUESTS)]
    access_results: List[AccessResult] = []

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(request_access, idem) for idem in idems]
        for f in as_completed(futs):
            access_results.append(f.result())

    ok = [r for r in access_results if r.ok and r.token]
    bad = [r for r in access_results if not r.ok]

    print(f"[REQUEST_ACCESS] ok={len(ok)} bad={len(bad)}")
    if bad:
        print("Top failures (up to 5):")
        for r in bad[:5]:
            print(f"- status={r.status} body={r.body[:200]}")
        print("\nIf you see RPC duplicate errors, your idempotency RPC is not race-safe.\n")

    tokens = list({r.token for r in ok if r.token})
    print(f"[TOKENS] unique minted={len(tokens)}")
    if not tokens:
        print("❌ No tokens minted. Stop.")
        return

    # Sweep loop while verifying (should be mostly 0)
    sweeping = True
    from threading import Thread
    def sweep_loop():
        nonlocal sweeping
        while sweeping:
            try:
                s = sweep()
                print(f"[SWEEP] swept={s}")
            except Exception as e:
                print(f"[SWEEP] ERROR: {e}")
            time.sleep(SWEEP_EVERY_SECONDS)
    sweeper = Thread(target=sweep_loop, daemon=True)
    sweeper.start()

    # Verify all tokens (with retries)
    verify_jobs = []
    for t in tokens:
        for _ in range(VERIFY_ATTEMPTS_PER_TOKEN):
            verify_jobs.append(t)

    verify_results: List[VerifyResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(verify_token, t) for t in verify_jobs]
        for f in as_completed(futs):
            verify_results.append(f.result())

    sweeping = False
    time.sleep(1)

    # Analyze
    by_token: Dict[str, List[VerifyResult]] = {}
    for r in verify_results:
        by_token.setdefault(r.token, []).append(r)

    double_spend = []
    for t, res in by_token.items():
        valid_count = sum(1 for x in res if x.ok and x.valid)
        if valid_count > 1:
            double_spend.append((t, valid_count))

    if double_spend:
        print("\n❌ DOUBLE SPEND DETECTED")
        for t, n in double_spend[:5]:
            print(f"- {t} valid_count={n}")
        return

    print("✅ No double-spend during settle_all storm")

    elapsed = time.time() - start
    print(f"\n=== DONE in {elapsed:.2f}s ===")
    print("Now check Supabase: tokens should be 0 and escrow should be 0.")

if __name__ == "__main__":
    main()
