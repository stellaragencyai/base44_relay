import os, json, hmac, hashlib, time, uuid, requests
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
BASE       = "https://api.bybit.com"   # use https://api-testnet.bybit.com if testing

def sign(ts, body):
    payload = f"{ts}{API_KEY}20000{body}"
    return hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def headers(ts, body):
    return {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": sign(ts, body),
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": "20000",
        "Content-Type": "application/json"
    }

def transfer(coin, amount):
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
    r = requests.post(url, headers=headers(ts, body), data=body, timeout=15)
    print(f"HTTP {r.status_code}: {r.text}")

if __name__ == "__main__":
    transfer("USDT", 1.0)  # sends 1 USDT from UNIFIED → FUND
