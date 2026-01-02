import os
import time
import uuid
import random
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"
SWEEP_URL = f"{BRIDGE_BASE}/sweep_expired"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

CONCURRENCY = 50
NUM_REQUESTS = 50

# leave_some behavior
SETTLE_FRACTION = 0.60          # settle ~60% of tokens, let the rest expire+refund
VERIFY_ATTEMPTS_PER_SETTLED = 2 # retry verifies to prove idempotency

# Sweeping behavior
SWEEP_EVERY_SECONDS = 3         # sweep repeatedly during the run
FINAL_SWEEP_AFTER_TTL = True
TOKEN_TTL_SECONDS = 600         # MUST match server TOKEN_TTL_SECONDS (10 min)
FINAL_SWEEP_WAIT_SECONDS = 610  # wait TTL+buffer then sweep

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
    print("\n=== NEXUS REAL-WORLD STRESS TEST (leave_some) ===")
    print(f"Bridge: {BRIDGE_BASE}")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"Requests: {NUM_REQUESTS}")
    print(f"Settle fraction: {SETTLE_FRACTION}")
    print(f"Sweep every: {SWEEP_EVERY_SECONDS}s")
    print(f"Final sweep after TTL: {FINAL_SWEEP_AFTER_TTL}\n")

    start = time.time()

    # 1) Mint tokens concurrently
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

    # 2) Choose tokens to settle vs leave to expire
    random.shuffle(tokens)
    settle_count = int(len(tokens) * SETTLE_FRACTION)
    settle_tokens = tokens[:settle_count]
    leave_tokens = tokens[settle_count:]

    print(f"[PLAN] settle={len(settle_tokens)} leave_to_expire={len(leave_tokens)}")

    # 3) While verifies are running, sweep repeatedly
    sweeping = True

    def sweep_loop():
        nonlocal sweeping
        # sweep a few times while verification is happening
        while sweeping:
            try:
                s = sweep()
                print(f"[SWEEP] swept={s}")
            except Exception as e:
                print(f"[SWEEP] ERROR: {e}")
            time.sleep(SWEEP_EVERY_SECONDS)

    # Start sweeper
    from threading import Thread
    sweeper = Thread(target=sweep_loop, daemon=True)
    sweeper.start()

    # 4) Verify settle tokens with retries
    verify_jobs = []
    for t in settle_tokens:
        for _ in range(VERIFY_ATTEMPTS_PER_SETTLED):
            verify_jobs.append(t)

    verify_results: List[VerifyResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(verify_token, t) for t in verify_jobs]
        for f in as_completed(futs):
            verify_results.append(f.result())

    # stop sweeping after verify storm
    sweeping = False
    time.sleep(1)

    # 5) Analyze verify results
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
        print("Stop. This must never happen.")
        return

    print("✅ No double-spend during settle storm")

    # 6) Wait for the left tokens to expire, then final sweep
    if FINAL_SWEEP_AFTER_TTL and leave_tokens:
        print(f"\n[WAIT] Waiting {FINAL_SWEEP_WAIT_SECONDS}s for expiry...")
        time.sleep(FINAL_SWEEP_WAIT_SECONDS)

        s = sweep()
        print(f"[FINAL SWEEP] swept={s}")

        # Optional: verify one of the expired tokens to see ALREADY_USED
        sample = random.choice(leave_tokens)
        vr = verify_token(sample)
        print(f"[EXPIRED VERIFY SAMPLE] valid={vr.valid} error={vr.error}")

    elapsed = time.time() - start
    print(f"\n=== DONE in {elapsed:.2f}s ===")
    print("Now check Supabase: escrow should be 0 and tokens should be 0 (after final sweep).")


if __name__ == "__main__":
    main()
