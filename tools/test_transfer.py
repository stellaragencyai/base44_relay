#!/usr/bin/env python3
# tools/test_transfer.py
import os, json, hmac, hashlib, time, uuid, requests
from dotenv import load_dotenv, find_dotenv

# --- Load ROOT .env regardless of CWD ---
ENV_PATH = find_dotenv(filename=".env", usecwd=True)
if ENV_PATH:
    load_dotenv(ENV_PATH, override=True)
    print(f"[test] Loaded .env: {ENV_PATH}")
else:
    load_dotenv(override=True)
    print("[test/warn] .env not found via find_dotenv; relying on process env.")

API_KEY    = (os.getenv("BYBIT_API_KEY") or "").strip()
API_SECRET = (os.getenv("BYBIT_API_SECRET") or "").strip()
NETWORK    = (os.getenv("NETWORK", "mainnet") or "mainnet").lower()
SUB_UID    = (os.getenv("BYBIT_SUB_UID") or "").strip()  # optional

BASE = "https://api-testnet.bybit.com" if NETWORK == "testnet" else "https://api.bybit.com"
RECV_WINDOW = "20000"

# One session, explicitly bypass system proxies (fixes weird 404 from local proxy placeholders)
session = requests.Session()
session.trust_env = False  # ignore HTTP(S)_PROXY env vars
session.proxies = {}       # no proxy

def sign_v5(ts: str, body_json: str) -> str:
    prehash = f"{ts}{API_KEY}{RECV_WINDOW}{body_json}"
    return hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).hexdigest()

def headers(ts: str, body_json: str) -> dict:
    h = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sign_v5(ts, body_json),
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }
    if SUB_UID:
        h["X-BAPI-SUB-ACCOUNT-UID"] = SUB_UID
    return h

def get_public_time():
    r = session.get(f"{BASE}/v5/market/time", timeout=15)
    print(f"[test] GET /v5/market/time → {r.status_code} {r.text[:120]}")

def check_api_key():
    ts = str(int(time.time() * 1000))
    body = ""  # GET: empty body in signing
    h = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sign_v5(ts, body),
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
    }
    if SUB_UID:
        h["X-BAPI-SUB-ACCOUNT-UID"] = SUB_UID
    url = f"{BASE}/v5/user/query-api"
    r = session.get(url, headers=h, timeout=20)
    print(f"[test] GET /v5/user/query-api → {r.status_code} {r.text[:200]}")

def transfer(coin: str, amount: float):
    if not API_KEY or not API_SECRET:
        print("[test/error] Missing BYBIT_API_KEY or BYBIT_API_SECRET in .env")
        return
    ts = str(int(time.time() * 1000))
    payload = {
        "transferId": str(uuid.uuid4()),
        "coin": coin,
        "amount": str(amount),
        "fromAccountType": "UNIFIED",
        "toAccountType": "FUND"
    }
    body = json.dumps(payload, separators=(",", ":"))
    url = f"{BASE}/v5/asset/transfer"
    print(f"[test] POST {url}")
    print(f"[test] Req body: {body}")
    r = session.post(url, headers=headers(ts, body), data=body, timeout=20)
    print(f"[test] HTTP {r.status_code}: {r.text}")

if __name__ == "__main__":
    print(f"[test] network={NETWORK} base={BASE}")
    get_public_time()     # connectivity sanity
    check_api_key()       # key validity & perms
    transfer("USDT", 1.0) # do a tiny transfer
