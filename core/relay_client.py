#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Relay Client — hardened, sub-account aware, backwards compatible

Key upgrades:
- Reads relay base from: RELAY_URL | BASE44_RELAY_URL | EXECUTOR_RELAY_BASE | RELAY_BASE | default http://127.0.0.1:5000
- Reads token from: RELAY_TOKEN | BASE44_RELAY_TOKEN | EXECUTOR_RELAY_TOKEN
- Unified requests.Session with Bearer; 15s GET / 20s POST timeouts
- Robust JSON handling and HTTP error raising with readable messages
- `bybit_proxy(..., extra={'subUid': ...})` and `proxy(..., extra=...)` for per-subaccount routing
- `equity_unified(coin=None, extra=None)` returns numeric equity; respects subUid context
- Health helpers: `is_token_ok()`, `/diag/ping` support
- Safe Telegram sender with tiny retry
- Keeps your prior bot-friendly wrappers, but prefer `extra={'subUid': '260417078'}` over memberId params
"""

from __future__ import annotations

import os
import json
import csv
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Env / Globals
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

# Base URL priority: explicit ngrok > executor vars > fallback localhost:5000
_RELAY_BASE = (
    os.getenv("RELAY_URL")
    or os.getenv("BASE44_RELAY_URL")
    or os.getenv("EXECUTOR_RELAY_BASE")
    or os.getenv("RELAY_BASE")
    or "http://127.0.0.1:5000"
).rstrip("/")

# Token priority: explicit > executor
_RELAY_TOKEN = (
    os.getenv("RELAY_TOKEN")
    or os.getenv("BASE44_RELAY_TOKEN")
    or os.getenv("EXECUTOR_RELAY_TOKEN")
    or ""
).strip()

TG_TOKEN   = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not _RELAY_BASE:
    raise RuntimeError("Missing relay base URL. Set RELAY_URL (preferred) or EXECUTOR_RELAY_BASE in .env")
if not _RELAY_TOKEN:
    raise RuntimeError("Missing relay bearer token. Set RELAY_TOKEN (or EXECUTOR_RELAY_TOKEN) in .env")

# Unified session
_SESSION = requests.Session()
_SESSION.headers.update({
    "Authorization": f"Bearer {_RELAY_TOKEN}",
    "ngrok-skip-browser-warning": "true",
    "Content-Type": "application/json",
})

def _u(path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return f"{_RELAY_BASE}{path}"

def _json_or_text(resp: requests.Response) -> dict:
    """Return JSON if possible; else include raw text."""
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "raw": resp.text[:2000]}

def _raise_for_auth(resp: requests.Response):
    if resp.status_code == 401:
        raise RuntimeError("401 from relay: token mismatch or not provided")
    if resp.status_code == 403:
        raise RuntimeError("403 from relay: forbidden (IP allowlist? token role?)")

# ──────────────────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(text: str, parse_mode: Optional[str] = None) -> None:
    """Best-effort Telegram message with micro-retry. No exceptions leak."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    for _ in range(2):
        try:
            r = requests.post(url, json=payload, timeout=8)
            if r.ok:
                return
        except Exception:
            time.sleep(0.8)

# ──────────────────────────────────────────────────────────────────────────────
# Raw relay HTTP
# ──────────────────────────────────────────────────────────────────────────────
def relay_get(path: str, params: Optional[dict] = None, timeout: int = 15) -> dict:
    try:
        r = _SESSION.get(_u(path), params=params or {}, timeout=timeout)
        _raise_for_auth(r)
        return _json_or_text(r)
    except Exception as e:
        return {"error": str(e), "path": path, "params": params or {}}

def relay_post(path: str, body: Optional[dict] = None, timeout: int = 20) -> dict:
    try:
        r = _SESSION.post(_u(path), json=body or {}, timeout=timeout)
        _raise_for_auth(r)
        return _json_or_text(r)
    except Exception as e:
        return {"error": str(e), "path": path, "body": body or {}}

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Bybit proxy (returns FULL relay envelope)
# Supports `extra` for context (e.g., {"subUid":"260417078"})
# ──────────────────────────────────────────────────────────────────────────────
def bybit_proxy(method: str, path: str, params: Optional[dict] = None,
                body: Optional[dict] = None, extra: Optional[dict] = None,
                timeout: int = 20) -> dict:
    """
    Request through the relay. Returns full envelope:
      {"primary":{"status":...,"body":{...}}, "fallback":{...}, "error":?...}
    """
    payload = {"method": method.upper(), "path": path}
    if params:
        payload["params"] = params
    if body:
        payload["body"] = body
    if extra:
        payload["extra"] = extra  # e.g., {"subUid": "260417078"}
    return relay_post("/bybit/proxy", payload, timeout=timeout)

# ──────────────────────────────────────────────────────────────────────────────
# Bot-friendly proxy (returns Bybit JSON BODY directly)
# ──────────────────────────────────────────────────────────────────────────────
def proxy(method: str, path: str, params: Optional[dict] = None,
          body: Optional[dict] = None, extra: Optional[dict] = None,
          timeout: int = 20) -> dict:
    env = bybit_proxy(method, path, params=params, body=body, extra=extra, timeout=timeout)
    if isinstance(env, dict) and "primary" in env:
        try:
            return env["primary"]["body"]
        except Exception:
            return env
    return env

# ──────────────────────────────────────────────────────────────────────────────
# Quick data helpers (sub-account aware via `extra`)
# ──────────────────────────────────────────────────────────────────────────────
def equity_unified(coin: Optional[str] = None, extra: Optional[dict] = None) -> float:
    """
    Returns numeric totalEquity for UNIFIED wallet. If `extra={'subUid':...}` is provided,
    equity is fetched in that sub-account context.
    """
    params = {"accountType": "UNIFIED"}
    if coin:
        params["coin"] = coin
    env = bybit_proxy("GET", "/v5/account/wallet-balance", params=params, extra=extra)
    # Prefer full envelope parsing (more robust to relay format)
    try:
        body = (env.get("primary", {}) or {}).get("body", {}) if isinstance(env, dict) else {}
        result = (body.get("result", {}) or {})
        total = 0.0
        for acct in (result.get("list", []) or []):
            try:
                total += float(acct.get("totalEquity", 0))
            except Exception:
                pass
        return total
    except Exception:
        # Fallback if relay returned raw Bybit JSON
        try:
            result = (env.get("result", {}) or {})
            total = 0.0
            for acct in (result.get("list", []) or []):
                total += float(acct.get("totalEquity", 0))
            return total
        except Exception:
            return 0.0

def ticker(symbol: str, category: str = "linear") -> dict:
    j = proxy("GET", "/v5/market/tickers", params={"category": category, "symbol": symbol})
    return ((j.get("result", {}) or {}).get("list", []) or [{}])[0]

def klines(symbol: str, interval: str = "1", limit: int = 50, category: str = "linear") -> list:
    params = {"category": category, "symbol": symbol, "interval": interval, "limit": limit}
    env = bybit_proxy("GET", "/v5/market/kline", params=params)
    body = (env.get("primary", {}) or {}).get("body", {}) if isinstance(env, dict) else {}
    return ((body.get("result", {}) or {}).get("list", []) or [])

# ──────────────────────────────────────────────────────────────────────────────
# Registry helpers (CSV / JSON)
# ──────────────────────────────────────────────────────────────────────────────
def load_sub_uids(csv_path: str = "registry/sub_uids.csv",
                  map_path: str = "registry/sub_map.json") -> Tuple[List[str], Dict[str, str]]:
    uids: List[str] = []
    name_map: Dict[str, str] = {}
    p = Path(csv_path)
    if p.exists():
        with p.open(newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for row in rd:
                val = (row.get("sub_uid") or "").strip()
                if val:
                    uids.append(val)
    mp = Path(map_path)
    if mp.exists():
        try:
            name_map = json.loads(mp.read_text(encoding="utf-8"))
        except Exception:
            name_map = {}
    return uids, name_map

def pretty_name(uid: str, name_map: Dict[str, str]) -> str:
    return name_map.get(uid, uid)

# ──────────────────────────────────────────────────────────────────────────────
# Legacy convenience wrappers (compat)
# Note: prefer `extra={'subUid': '<UID>'}` instead of passing memberId in params.
# ──────────────────────────────────────────────────────────────────────────────
def get_wallet_balance(accountType: str = "UNIFIED", memberId: Optional[str] = None) -> dict:
    params = {"accountType": accountType}
    if memberId:
        params["memberId"] = memberId
    return proxy("GET", "/v5/account/wallet-balance", params=params)

def get_positions(category: str = "linear", symbol: Optional[str] = None,
                  memberId: Optional[str] = None, settleCoin: str = "USDT") -> dict:
    p = {"category": category}
    if symbol:
        p["symbol"] = symbol
    if memberId:
        p["memberId"] = memberId
    if category.lower() == "linear" and settleCoin:
        p["settleCoin"] = settleCoin
    return proxy("GET", "/v5/position/list", params=p)

def get_open_orders(category: str = "linear", symbol: Optional[str] = None,
                    memberId: Optional[str] = None, openOnly: int = 1) -> dict:
    p = {"category": category, "openOnly": openOnly}
    if symbol:
        p["symbol"] = symbol
    if memberId:
        p["memberId"] = memberId
    return proxy("GET", "/v5/order/realtime", params=p)

def get_order_history(category: str = "linear", symbol: Optional[str] = None,
                      memberId: Optional[str] = None, limit: int = 200) -> dict:
    p = {"category": category, "limit": limit}
    if symbol:
        p["symbol"] = symbol
    if memberId:
        p["memberId"] = memberId
    return proxy("GET", "/v5/order/history", params=p)

def get_execution_list(category: str = "linear", symbol: Optional[str] = None,
                       memberId: Optional[str] = None, limit: int = 200) -> dict:
    p = {"category": category, "limit": limit}
    if symbol:
        p["symbol"] = symbol
    if memberId:
        p["memberId"] = memberId
    return proxy("GET", "/v5/execution/list", params=p)

def get_ticker(symbol: str, category: str = "linear") -> dict:
    return proxy("GET", "/v5/market/tickers", params={"category": category, "symbol": symbol})

def get_balance_unified(member_id: str) -> dict:
    return bybit_proxy("GET", "/v5/account/wallet-balance",
                       params={"accountType": "UNIFIED", "memberId": member_id})

def get_positions_linear(member_id: str, symbol: Optional[str] = None) -> dict:
    params = {"category": "linear", "memberId": member_id}
    if symbol:
        params["symbol"] = symbol
    return bybit_proxy("GET", "/v5/position/list", params=params)

def get_open_orders_linear(member_id: str, symbol: Optional[str] = None) -> dict:
    params = {"category": "linear", "memberId": member_id}
    if symbol:
        params["symbol"] = symbol
    return bybit_proxy("GET", "/v5/order/realtime", params=params)

def get_closed_pnl(member_id: str, symbol: Optional[str] = None) -> dict:
    params = {"category": "linear", "memberId": member_id}
    if symbol:
        params["symbol"] = symbol
    return bybit_proxy("GET", "/v5/position/closed-pnl", params=params)

# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
def is_token_ok() -> bool:
    """True if relay answers /diag/ping with 200 JSON."""
    try:
        r = _SESSION.get(_u("/diag/ping"), timeout=5)
        _raise_for_auth(r)
        _ = r.json()
        return True
    except Exception:
        return False

if __name__ == "__main__":
    print(relay_get("/diag/time"))
