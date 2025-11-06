#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Client Helpers — Finalized (5000-bound, hardened, compat-safe)

- Loads env + relay token (hard-fail if missing).
- Relay HTTP helpers (5000/ngrok default; never 8080 fallback).
- Telegram send (quiet, with minimal retry).
- Registry CSV/JSON helpers.
- Bybit v5 proxy helpers:
    • Low-level: bybit_proxy(method, path, params|body)
    • Bot-friendly: proxy(method, path, params|body) → returns primary.body
    • Convenience: get_wallet_balance, get_positions, get_open_orders, etc.
- Compatibility: accepts subUid or memberId in helpers.
- Headers include Authorization and x-relay-token for picky relays.
"""

from __future__ import annotations

import os, json, csv, time
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import requests
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Env / Globals
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

RELAY_URL   = (os.getenv("RELAY_URL") or os.getenv("BASE44_RELAY_URL") or "https://127.0.0.1:5000").rstrip("/")
RELAY_TOKEN = (os.getenv("RELAY_TOKEN") or os.getenv("BASE44_RELAY_TOKEN") or "").strip()
TG_TOKEN    = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TG_CHAT_ID  = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not RELAY_URL:
    raise RuntimeError("RELAY_URL missing in .env (expected your ngrok https URL)")
if not RELAY_TOKEN:
    raise RuntimeError("RELAY_TOKEN missing in .env (expected your relay bearer token)")

# unified session
_SESSION = requests.Session()
_SESSION.headers.update({
    "Authorization": f"Bearer {RELAY_TOKEN}",
    "x-relay-token": RELAY_TOKEN,
    "ngrok-skip-browser-warning": "true",
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
    "User-Agent": "Base44-Client/1.1",
})

def _relay_url(path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return f"{RELAY_URL}{path}"

# ──────────────────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(text: str, *, priority: str = "info") -> None:
    """Send a Telegram message quietly with minimal retry. Never raises."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    prefix = {
        "error": "❌",
        "warn": "⚠️",
        "success": "✅",
        "info": "ℹ️",
    }.get(priority, "ℹ️")
    payload = {"chat_id": TG_CHAT_ID, "text": f"{prefix} {text}", "disable_web_page_preview": True}
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=8)
            if r.ok:
                return
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(8.0, 0.4 * (2 ** attempt))); continue
            return
        except Exception:
            time.sleep(min(8.0, 0.4 * (2 ** attempt)))

# ──────────────────────────────────────────────────────────────────────────────
# Raw relay HTTP
# ──────────────────────────────────────────────────────────────────────────────
def relay_get(path: str, params: Optional[dict] = None, *, timeout: int = 15) -> dict:
    try:
        r = _SESSION.get(_relay_url(path), params=params or {}, timeout=timeout)
        return _safe_json(r)
    except Exception as e:
        return {"error": str(e)}

def relay_post(path: str, body: Optional[dict] = None, *, timeout: int = 20) -> dict:
    try:
        r = _SESSION.post(_relay_url(path), json=body or {}, timeout=timeout)
        return _safe_json(r)
    except Exception as e:
        return {"error": str(e)}

def _safe_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "text": resp.text[:400]}

# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics
# ──────────────────────────────────────────────────────────────────────────────
def relay_ok() -> bool:
    """Probe /health, then /diag/time. True if either returns 200 JSON-ish."""
    try:
        j = relay_get("/health")
        if isinstance(j, dict) and (j.get("ok") is True or j.get("status") == 200):
            return True
    except Exception:
        pass
    try:
        j = relay_get("/diag/time")
        return bool(j) and isinstance(j, dict)
    except Exception:
        return False

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Bybit proxy (returns FULL relay envelope)
# ──────────────────────────────────────────────────────────────────────────────
def bybit_proxy(method: str, path: str, params: Optional[dict] = None,
                body: Optional[dict] = None, *, timeout: int = 20) -> dict:
    """
    Returns full relay envelope:
      {"primary":{"status":...,"body":{...}}, "fallback":{...}, "error":?}
    """
    payload = {"method": method.upper(), "path": path}
    m = method.upper()
    if m == "GET":
        payload["params"] = params or {}
    else:
        payload["body"] = body or {}
    return relay_post("/bybit/proxy", payload, timeout=timeout)

# ──────────────────────────────────────────────────────────────────────────────
# Bot-friendly proxy (returns Bybit JSON BODY directly)
# ──────────────────────────────────────────────────────────────────────────────
def proxy(method: str, path: str, params: Optional[dict] = None,
          body: Optional[dict] = None, *, timeout: int = 20) -> dict:
    """Preferred for bots: returns primary.body JSON directly, tolerates odd envelopes."""
    env = bybit_proxy(method, path, params=params, body=body, timeout=timeout)
    # Common shapes: {"primary":{"body":{...}}}, or already Bybit-like
    try:
        if isinstance(env, dict):
            primary = env.get("primary")
            if isinstance(primary, dict) and "body" in primary:
                return primary.get("body") or {}
            # Some relays flatten to {"result":..., "retCode":...}
            if "retCode" in env or "result" in env:
                return env
    except Exception:
        pass
    return env if isinstance(env, dict) else {"error": "bad_proxy_env"}

# ──────────────────────────────────────────────────────────────────────────────
# Registry helpers (CSV / JSON)
# ──────────────────────────────────────────────────────────────────────────────
def load_sub_uids(csv_path: str = "sub_uids.csv",
                  map_path: str = "sub_map.json") -> Tuple[List[str], Dict[str, str]]:
    """
    Returns (uids, name_map). name_map tries to read either a flat map or {subs:{uid:{name:..}}}
    """
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
            raw = json.loads(mp.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and "subs" in raw and isinstance(raw["subs"], dict):
                # base44_registry format
                for uid, rec in raw["subs"].items():
                    nm = (rec or {}).get("name") or uid
                    name_map[str(uid)] = str(nm)
            elif isinstance(raw, dict):
                # assume {uid:name}
                for k, v in raw.items():
                    name_map[str(k)] = str(v)
        except Exception:
            name_map = {}
    return uids, name_map

def pretty_name(uid: str, name_map: Dict[str, str]) -> str:
    return name_map.get(uid, uid)

# ──────────────────────────────────────────────────────────────────────────────
# Bybit quick data helpers (accept subUid or memberId)
# ──────────────────────────────────────────────────────────────────────────────
def _sub_param(subUid: Optional[str] = None, memberId: Optional[str] = None) -> Dict[str, str]:
    # Prefer explicit subUid if passed; otherwise memberId; else none
    if subUid:
        return {"subUid": str(subUid)}
    if memberId:
        # Some endpoints expect memberId; relay should pass-through either way.
        return {"memberId": str(memberId)}
    return {}

def get_wallet_balance(accountType: str = "UNIFIED",
                       coin: Optional[str] = None,
                       subUid: Optional[str] = None,
                       memberId: Optional[str] = None,
                       **extra) -> dict:
    params: Dict[str, Any] = {"accountType": accountType}
    if coin:
        params["coin"] = coin
    params.update(_sub_param(subUid=subUid, memberId=memberId))
    params.update(extra or {})
    return proxy("GET", "/v5/account/wallet-balance", params=params)

def get_positions(category: str = "linear",
                  settleCoin: Optional[str] = "USDT",
                  symbol: Optional[str] = None,
                  subUid: Optional[str] = None,
                  memberId: Optional[str] = None,
                  **extra) -> dict:
    p: Dict[str, Any] = {"category": category}
    if category.lower() == "linear" and settleCoin:
        p["settleCoin"] = settleCoin
    if symbol:
        p["symbol"] = symbol
    p.update(_sub_param(subUid=subUid, memberId=memberId))
    p.update(extra or {})
    return proxy("GET", "/v5/position/list", params=p)

def get_positions_linear(settleCoin: str = "USDT",
                         symbol: Optional[str] = None,
                         subUid: Optional[str] = None,
                         memberId: Optional[str] = None,
                         **extra) -> dict:
    return get_positions("linear", settleCoin=settleCoin, symbol=symbol, subUid=subUid, memberId=memberId, **extra)

def get_open_orders(category: str = "linear",
                    symbol: Optional[str] = None,
                    openOnly: int = 1,
                    subUid: Optional[str] = None,
                    memberId: Optional[str] = None,
                    **extra) -> dict:
    p: Dict[str, Any] = {"category": category, "openOnly": openOnly}
    if symbol:
        p["symbol"] = symbol
    p.update(_sub_param(subUid=subUid, memberId=memberId))
    p.update(extra or {})
    return proxy("GET", "/v5/order/realtime", params=p)

def get_order_history(category: str = "linear",
                      symbol: Optional[str] = None,
                      limit: int = 200,
                      subUid: Optional[str] = None,
                      memberId: Optional[str] = None,
                      **extra) -> dict:
    p: Dict[str, Any] = {"category": category, "limit": limit}
    if symbol:
        p["symbol"] = symbol
    p.update(_sub_param(subUid=subUid, memberId=memberId))
    p.update(extra or {})
    return proxy("GET", "/v5/order/history", params=p)

def get_execution_list(category: str = "linear",
                       symbol: Optional[str] = None,
                       limit: int = 200,
                       subUid: Optional[str] = None,
                       memberId: Optional[str] = None,
                       **extra) -> dict:
    p: Dict[str, Any] = {"category": category, "limit": limit}
    if symbol:
        p["symbol"] = symbol
    p.update(_sub_param(subUid=subUid, memberId=memberId))
    p.update(extra or {})
    return proxy("GET", "/v5/execution/list", params=p)

def get_ticker(symbol: str, category: str = "linear") -> dict:
    return proxy("GET", "/v5/market/tickers", params={"category": category, "symbol": symbol})

# ──────────────────────────────────────────────────────────────────────────────
# Legacy convenience wrappers (return FULL envelope for backward compat)
# ──────────────────────────────────────────────────────────────────────────────
def get_balance_unified(member_id: str) -> dict:
    return bybit_proxy("GET", "/v5/account/wallet-balance",
                       params={"accountType": "UNIFIED", "memberId": member_id})

def get_positions_linear_legacy(member_id: str, symbol: Optional[str] = None) -> dict:
    params = {"category": "linear", "memberId": member_id}
    if symbol:
        params["symbol"] = symbol
    return bybit_proxy("GET", "/v5/position/list", params=params)

def get_open_orders_linear_legacy(member_id: str, symbol: Optional[str] = None) -> dict:
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
# Admin helpers
# ──────────────────────────────────────────────────────────────────────────────
def relay_health() -> dict:
    return relay_get("/health")

def relay_time() -> dict:
    return relay_get("/diag/time")

def get_subuids() -> dict:
    """If your relay exposes /bybit/subuids, return its body; else {}."""
    try:
        j = relay_get("/bybit/subuids")
        if isinstance(j, dict) and ("result" in j or "subs" in j or "list" in j or "primary" in j):
            return j
    except Exception:
        pass
    return {}

# ──────────────────────────────────────────────────────────────────────────────
# Self-test
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[base44_client] relay     : {RELAY_URL}")
    print(f"[base44_client] token set : {bool(RELAY_TOKEN)}")
    print(f"[base44_client] probe     : {'OK' if relay_ok() else 'FAIL'}")
    try:
        wb = get_wallet_balance(accountType="UNIFIED")
        print(f"[base44_client] wallet retCode={wb.get('retCode')}")
    except Exception as e:
        print(f"[base44_client] wallet error: {e}")
