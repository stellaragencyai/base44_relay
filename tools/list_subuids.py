# tools/list_subuids.py
import os, time, hmac, hashlib, json, uuid
from pathlib import Path
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

API_KEY    = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
ENV        = (os.getenv("BYBIT_ENV", "mainnet") or "mainnet").lower().strip()
BASE       = "https://api-testnet.bybit.com" if ENV == "testnet" else "https://api.bybit.com"

def sign(timestamp_ms: str, recv_window: str, body: str) -> str:
    # v5 sign: sha256(timestamp+apiKey+recvWindow+body)
    payload = f"{timestamp_ms}{API_KEY}{recv_window}{body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def authed_post(path: str, body: dict):
    ts = str(int(time.time() * 1000))
    rw = "5000"
    j = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    sig = sign(ts, rw, j)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sig,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": rw,
        "Content-Type": "application/json",
    }
    url = f"{BASE}{path}"
    resp = requests.post(url, headers=headers, data=j, timeout=15)
    return resp.status_code, resp.text

def authed_get(path: str, params: dict):
    ts = str(int(time.time() * 1000))
    rw = "5000"
    # For GET, body is "{}" in v5 signing when no body
    body = "{}"
    sig = sign(ts, rw, body)
    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sig,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": rw,
        "Content-Type": "application/json",
    }
    url = f"{BASE}{path}"
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    return resp.status_code, resp.text

def main():
    if not API_KEY or not API_SECRET:
        print("[list_subuids] Missing BYBIT_API_KEY/SECRET in .env")
        return
    print(f"[list_subuids] Env: {ENV} base={BASE}")

    # Try endpoint #1: transfer sub-member list (often available)
    try:
        code, text = authed_post("/v5/asset/transfer/query-sub-member-list", {"limit": 200})
        print(f"[try1] /v5/asset/transfer/query-sub-member-list → {code}")
        if code == 200:
            js = json.loads(text)
            if js.get("retCode") in (0, "0") and js.get("result"):
                rows = js["result"].get("list") or js["result"].get("rows") or js["result"]
                uids = []
                if isinstance(rows, list):
                    for r in rows:
                        uid = r.get("subMemberId") or r.get("memberId") or r.get("uid")
                        name = r.get("subMemberName") or r.get("nickname") or ""
                        if uid:
                            uids.append((str(uid), name))
                if uids:
                    print("Sub UIDs found:")
                    for uid, name in uids:
                        print(f"  - {uid}  {('('+name+')') if name else ''}")
                    return
                else:
                    print("[try1] No rows in result (or schema unknown).")
            else:
                print(f"[try1] retCode={js.get('retCode')} retMsg={js.get('retMsg')}")
        else:
            print(f"[try1] HTTP {code}: {text[:200]}")
    except Exception as e:
        print(f"[try1] exception: {e}")

    # Try endpoint #2: (alt) user sub-member ids (some accounts)
    try:
        code, text = authed_get("/v5/user/submember/member-ids", {})
        print(f"[try2] /v5/user/submember/member-ids → {code}")
        if code == 200:
            js = json.loads(text)
            if js.get("retCode") in (0, "0") and js.get("result"):
                arr = js["result"].get("memberIds") or js["result"].get("list") or []
                if isinstance(arr, list) and arr:
                    print("Sub UIDs found:")
                    for uid in arr:
                        print(f"  - {uid}")
                    return
                else:
                    print("[try2] No memberIds in result.")
            else:
                print(f"[try2] retCode={js.get('retCode')} retMsg={js.get('retMsg')}")
        else:
            print(f"[try2] HTTP {code}: {text[:200]}")
    except Exception as e:
        print(f"[try2] exception: {e}")

    print("No sub UIDs returned. If you definitely have subs, grab them from the web UI (path above).")

if __name__ == "__main__":
    main()
