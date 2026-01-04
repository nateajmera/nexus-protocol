import uuid
import json
import requests
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRIDGE_BASE = "https://nexus-protocol.onrender.com"
REQUEST_ACCESS_URL = f"{BRIDGE_BASE}/request_access"
VERIFY_URL = f"{BRIDGE_BASE}/verify"

BUYER_API_KEY = "TEST_KEY_1"
SELLER_API_KEY = "SELLER_KEY_1"

NUM_TOKENS = 20
VERIFY_CONCURRENCY = 15          # do NOT start at 50 on Render free-tier
VERIFY_ATTEMPTS_PER_TOKEN = 8
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

SESSION = make_session(max_pool=VERIFY_CONCURRENCY * 2)

def mint_token():
    idem = f"storm_{uuid.uuid4()}"
    headers = {"x-api-key": BUYER_API_KEY, "x-idempotency-key": idem}
    payload = {"seller_id": "seller_01", "ttl_seconds": 600}
    r = SESSION.post(REQUEST_ACCESS_URL, headers=headers, json=payload, timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Mint failed {r.status_code}: {r.text}")
    return r.json()["auth_token"]

def verify_token(token: str):
    headers = {"x-seller-api-key": SELLER_API_KEY}
    r = SESSION.get(f"{VERIFY_URL}/{token}", headers=headers, timeout=TIMEOUT)
    return r.status_code, r.text

def safe_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None

def main():
    print("\n=== TEST 4: VERIFY STORM (double-spend protection) ===")
    print(f"Minting {NUM_TOKENS} tokens...")
    tokens = [mint_token() for _ in range(NUM_TOKENS)]
    print("✅ Minted.")

    status_counts = Counter()
    outcome_counts = Counter()
    sample_errors = defaultdict(int)

    tasks = []
    with ThreadPoolExecutor(max_workers=VERIFY_CONCURRENCY) as ex:
        for t in tokens:
            for _ in range(VERIFY_ATTEMPTS_PER_TOKEN):
                tasks.append(ex.submit(verify_token, t))

        for f in as_completed(tasks):
            status, body = f.result()
            status_counts[status] += 1

            j = safe_json(body)
            if status != 200:
                sample_errors[f"{status} {body[:140]}"] += 1
                continue

            if not isinstance(j, dict):
                sample_errors[f"200 NON_JSON {body[:140]}"] += 1
                continue

            # Expected shapes:
            # {"valid": true, "buyer_id": "..."}
            # {"valid": false, "error": "ALREADY_USED"}
            v = j.get("valid")
            if v is True:
                outcome_counts["valid_true"] += 1
            elif v is False:
                outcome_counts[f"valid_false:{j.get('error')}"] += 1
            else:
                outcome_counts["weird_200_shape"] += 1
                sample_errors[f"200 WEIRD {body[:140]}"] += 1

    print("\n[STATUS COUNTS]")
    for k, v in status_counts.most_common():
        print(f"  {k}: {v}")

    print("\n[OUTCOMES]")
    for k, v in outcome_counts.most_common():
        print(f"  {k}: {v}")

    print("\n[TOP ERROR BODIES]")
    for k, v in sorted(sample_errors.items(), key=lambda x: -x[1])[:8]:
        print(f"  x{v}  {k}")

    print("\nExpected pass conditions:")
    print(f"- valid_true should equal NUM_TOKENS ({NUM_TOKENS})")
    print("- valid_false:ALREADY_USED should be the majority of the rest")
    print("- tokens should end at 0, buyer escrow at 0\n")

if __name__ == "__main__":
    main()
