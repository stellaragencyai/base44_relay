#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, requests

RELAY_BASE  = (os.getenv("EXECUTOR_RELAY_BASE") or os.getenv("RELAY_BASE") or "http://127.0.0.1:5000").rstrip("/")
RELAY_TOKEN = (os.getenv("EXECUTOR_RELAY_TOKEN") or os.getenv("RELAY_TOKEN") or "").strip()

HDRS = {
    "Authorization": f"Bearer {RELAY_TOKEN}",
    "ngrok-skip-browser-warning": "true"
}

def _assert_token():
    if not RELAY_TOKEN:
        raise RuntimeError("Missing relay token: set EXECUTOR_RELAY_TOKEN or RELAY_TOKEN in .env")

def get(path, params=None, timeout=15):
    _assert_token()
    r = requests.get(f"{RELAY_BASE}{path}", headers=HDRS, params=params or {}, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("401 from relay (token mismatch)")
    return r.json()

def post(path, body=None, timeout=20):
    _assert_token()
    r = requests.post(f"{RELAY_BASE}{path}", headers=HDRS, json=body or {}, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("401 from relay (token mismatch)")
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status": r.status_code}

def proxy(method, path, params=None, body=None, timeout=20):
    payload = {"method": method.upper(), "path": path}
    if params: payload["params"] = params
    if body:   payload["body"]   = body
    return post("/bybit/proxy", body=payload, timeout=timeout)

def equity_unified():
    j = get("/bybit/wallet/balance", params={"accountType": "UNIFIED"})
    total = 0.0
    for acct in (j.get("result",{}) or {}).get("list",[]) or []:
        try: total += float(acct.get("totalEquity",0))
        except: pass
    return total

def ticker(symbol):
    j = get("/v5/market/tickers", params={"category":"linear","symbol":symbol})
    data = ((j.get("result",{}) or {}).get("list",[]) or [])
    return data[0] if data else {}

def klines(symbol, interval="1", limit=50):
    env = {"category":"linear","symbol":symbol,"interval":interval,"limit":limit}
    resp = proxy("GET", "/v5/market/kline", params=env)
    body = (resp.get("primary",{}) or {}).get("body",{})
    return ((body.get("result",{}) or {}).get("list",[]) or [])
