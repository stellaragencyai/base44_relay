#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/relay_client.py — robust relay HTTP helpers with sane env loading, retries, and shape-tolerant parsing.

Env keys honored (checked in this order):
  Token: EXECUTOR_RELAY_TOKEN -> RELAY_TOKEN
  Base:  EXECUTOR_RELAY_BASE  -> RELAY_BASE -> RELAY_URL (default http://127.0.0.1:5000)

Headers sent:
  Authorization: Bearer <token>
  X-Relay-Token: <token>
  Accept: application/json
  Content-Type: application/json
  ngrok-skip-browser-warning: true
"""

from __future__ import annotations
import os, json, time, typing
from typing import Any, Dict, Optional
import requests

# --- dotenv: load the project .env and OVERRIDE stale env vars ---
_ENV_PATH = None
def _load_env_once():
    global _ENV_PATH
    if _ENV_PATH is not None:
        return
    try:
        from dotenv import load_dotenv, find_dotenv
        _ENV_PATH = os.environ.get("BASE44_ENV_PATH") or find_dotenv(filename=".env", usecwd=True)
        if _ENV_PATH:
            load_dotenv(_ENV_PATH, override=True)
        else:
            load_dotenv(override=True)
    except Exception:
        _ENV_PATH = None

_load_env_once()

# --- env helpers (resolve fresh each call, so edits to .env take effect) ---
def _env(k: str, default: str = "") -> str:
    return (os.getenv(k, default) or "").strip()

def _get_token() -> str:
    return _env("EXECUTOR_RELAY_TOKEN") or _env("RELAY_TOKEN")

def _get_base() -> str:
    base = _env("EXECUTOR_RELAY_BASE") or _env("RELAY_BASE") or _env("RELAY_URL") or "http://127.0.0.1:5000"
    return base.rstrip("/")

def _headers(tok: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {tok}",
        "X-Relay-Token": tok,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true",
    }

def _assert_token_base() -> tuple[str, str]:
    tok = _get_token()
    if not tok:
        raise RuntimeError("Missing relay token: set EXECUTOR_RELAY_TOKEN or RELAY_TOKEN in .env")
    base = _get_base()
    if not base.startswith("http"):
        raise RuntimeError(f"Bad relay base URL: {base!r}")
    return tok, base

# --- small retry helper for transient 5xx/429 ---
def _request_with_retries(method: str, url: str, *, headers: Dict[str, str], params: Optional[dict]=None,
                          data: Optional[str]=None, timeout: int=25, max_retries: int=2):
    for attempt in range(max_retries + 1):
        try:
            r = requests.request(method, url, headers=headers, params=params or {}, data=data, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(min(2.0 * (attempt + 1), 6.0))
                continue
            return r
        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            time.sleep(min(2.0 * (attempt + 1), 6.0))
    return r  # type: ignore

# --- low-level HTTP to relay (NOT the Bybit proxy) ---
def get(path: str, params: Optional[dict]=None, timeout: int=20) -> Any:
    tok, base = _assert_token_base()
    url = f"{base}/{path.lstrip('/')}"
    r = _request_with_retries("GET", url, headers=_headers(tok), params=params, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("401 from relay (token mismatch)")
    try:
        return r.json()
    except Exception:
        return r.text

def post(path: str, body: Optional[dict]=None, timeout: int=25) -> Any:
    tok, base = _assert_token_base()
    url = f"{base}/{path.lstrip('/')}"
    payload = json.dumps(body or {})
    r = _request_with_retries("POST", url, headers=_headers(tok), data=payload, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("401 from relay (token mismatch)")
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status": r.status_code}

# --- generic Bybit proxy via relay ---
def proxy(method: str, path: str, params: Optional[dict]=None, body: Optional[dict]=None, timeout: int=25) -> Any:
    """
    Hit relay's /bybit/proxy which signs and forwards to Bybit.
    Handles both dict and text responses by returning dict when possible.
    """
    payload = {"method": method.upper(), "path": path}
    if params:
        payload["params"] = params
    if body:
        payload["body"] = body
    resp = post("/bybit/proxy", body=payload, timeout=timeout)
    # post() already tried to json() decode; if it's text, try once more
    if isinstance(resp, str):
        try:
            return json.loads(resp)
        except Exception:
            return {"raw": resp}
    return resp

# --- convenience: equity, ticker, klines with shape tolerance ---
def _coerce_json(x: Any) -> dict:
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}

def equity_unified(coin: str = "USDT", sub_uid: Optional[str] = None) -> float:
    """
    Try relay-native helper first: /bybit/wallet/balance (unified)
    Fallback to bybit proxy: /v5/account/wallet-balance
    """
    try:
        j = get("/bybit/wallet/balance", params={"accountType": "UNIFIED", "coin": coin, **({"subUid": sub_uid} if sub_uid else {})})
        data = _coerce_json(j)
        lst = ((data.get("result") or {}).get("list") or []) if isinstance(data, dict) else []
        total = 0.0
        for acct in lst:
            try:
                if sub_uid and str(acct.get("subUid", "")) != str(sub_uid):
                    continue
                # prefer coin breakdown if available
                coins = acct.get("coin") or []
                if coins:
                    for c in coins:
                        if str(c.get("coin","")).upper() == coin.upper():
                            total += float(c.get("equity", 0))
                else:
                    total += float(acct.get("totalEquity", 0))
            except Exception:
                pass
        if total > 0:
            return total
    except Exception:
        pass

    # Fallback: proxy call
    params = {"accountType": "UNIFIED", "coin": coin}
    if sub_uid:
        params["subUid"] = str(sub_uid)
    j = proxy("GET", "/v5/account/wallet-balance", params=params)
    data = _coerce_json(j)
    result = data.get("result") or {}
    lst = result.get("list") or []
    if not lst:
        return 0.0
    entry = lst[0]
    # coin array preferred
    coin_rows = entry.get("coin") or []
    if coin_rows:
        for c in coin_rows:
            if str(c.get("coin","")).upper() == coin.upper():
                try:
                    return float(c.get("equity", 0))
                except Exception:
                    return 0.0
    # fall back to totalEquity
    try:
        return float(entry.get("totalEquity", 0))
    except Exception:
        return 0.0

def ticker(symbol: str) -> dict:
    """
    Returns first row from Bybit /v5/market/tickers for given symbol.
    Uses proxy to avoid relying on relay-native mirrors.
    """
    j = proxy("GET", "/v5/market/tickers", params={"category": "linear", "symbol": symbol})
    data = _coerce_json(j)
    lst = ((data.get("result") or {}).get("list") or [])
    return lst[0] if lst else {}

def klines(symbol: str, interval: str = "1", limit: int = 50) -> list:
    """
    Returns list rows from Bybit /v5/market/kline. Handles both wrapped and direct shapes.
    """
    env = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    resp = proxy("GET", "/v5/market/kline", params=env)
    data = _coerce_json(resp)
    # Some relays nest under {"primary":{"body":{result:{list:[...]}}}}
    if "primary" in data:
        body = (data.get("primary") or {}).get("body") or {}
        data = _coerce_json(body)
    result = data.get("result") or {}
    lst = result.get("list") or []
    return lst if isinstance(lst, list) else []

# --- utilities ---
def debug_env() -> str:
    tok = _get_token()
    base = _get_base()
    masked = (tok[:4] + "…" + tok[-4:]) if tok and len(tok) > 8 else (tok or "<empty>")
    return f"relay_base={base} token={masked} env_path={_ENV_PATH or '<none>'}"

def refresh_env() -> None:
    """
    Re-load .env on demand (e.g., after editing from a running session).
    """
    try:
        from dotenv import load_dotenv
        if _ENV_PATH:
            load_dotenv(_ENV_PATH, override=True)
        else:
            load_dotenv(override=True)
    except Exception:
        pass
