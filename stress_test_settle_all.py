import time
import uuid
import requests
from dataclasses import dataclass
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

CONCURRENCY = 50
NUM_REQUESTS = 50
VERIFY_ATTEMPTS_PER_TOKEN = 2

TIMEOUT = 60  # give Render a bit more breathing room



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

    adapter = HTTPAdapter(
        pool_connections=max_pool,
        pool_maxsize=max_pool,
        max_retries=retries,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session(max_pool=CONCURRENCY * 2)


def request_access(idem: str, seller_id: str = "seller_01") -> AccessResult:
    headers = {
        "x-api-key": BUYER_API_KEY,
        "x-idempotency-key": idem,
        "Content-Type": "application/json",
    }
    payload = {"seller_id": seller_id}
    try:
        r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
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
        r = SESSION.get(f"{VERIFY_URL}/{token}", headers=headers, timeout=TIMEOUT)
        body = r.text
        if r.status_code == 200:
            j = r.json()
            return VerifyResult(True, token, bool(j.get("valid")), j.get("error"), r.status_code, body)
        return VerifyResult(False, token, False, None, r.status_code, body)
    except Exception as e:
        return VerifyResult(False, token, False, None, 0, f"EXCEPTION: {e}")


def main():
    print("\n=== NEXUS REAL-WORLD STRESS TEST (settle_all) ===")
    print(f"Bridge: {BRIDGE_BASE}")
    print(f"Concurrency: {CONCURRENCY}")
    print(f"Requests: {NUM_REQUESTS}")
    print(f"Verify attempts per token: {VERIFY_ATTEMPTS_PER_TOKEN}")
    print("Sweep: DISABLED (settle_all should not need sweep)\n")

    start = time.time()

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
        print("Top failures (up to 8):")
        for r in bad[:8]:
            print(f"- status={r.status} body={r.body[:220]}")

    tokens = list({r.token for r in ok if r.token})
    print(f"\n[TOKENS] unique minted={len(tokens)}")
    if not tokens:
        print("❌ No tokens minted. Stop.")
        return

    verify_jobs = []
    for t in tokens:
        for _ in range(VERIFY_ATTEMPTS_PER_TOKEN):
            verify_jobs.append(t)

    verify_results: List[VerifyResult] = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(verify_token, t) for t in verify_jobs]
        for f in as_completed(futs):
            verify_results.append(f.result())

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

    print("\n✅ No double-spend during settle_all storm")

    elapsed = time.time() - start
    print(f"\n=== DONE in {elapsed:.2f}s ===")
    print("Now check Supabase: tokens should be 0 and escrow should be 0.")


if __name__ == "__main__":
    main()
