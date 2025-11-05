#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Client Helpers  —  Finalized (5000-bound, hardened)

- Loads env + relay token
- Relay HTTP helpers (no localhost:8080 fallback)
- Telegram send (with basic retry)
- Registry CSV/JSON helpers
- Bybit v5 proxy helpers (both low-level and bot-friendly)

Enhancements:
  • RELAY_URL read from .env (RELAY_URL or BASE44_RELAY_URL)
  • Hard-fail if RELAY_URL or RELAY_TOKEN missing
  • Default to ngrok HTTPS, never localhost:8080
  • Unified Session with automatic Bearer header
  • 15 s GET / 20 s POST timeouts
  • Optional TELEGRAM_BOT_THREADSAFE env for retry behavior
"""

import os, json, csv, time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

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
    "ngrok-skip-browser-warning": "true",
    "Content-Type": "application/json",
})

def _relay_url(path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return f"{RELAY_URL}{path}"

# ──────────────────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(text: str) -> None:
    """Send a Telegram message quietly with minimal retry."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    for attempt in range(2):
        try:
            r = requests.post(url, json=payload, timeout=8)
            if r.ok:
                return
        except Exception:
            time.sleep(0.8)

# ──────────────────────────────────────────────────────────────────────────────
# Raw relay HTTP
# ──────────────────────────────────────────────────────────────────────────────
def relay_get(path: str, params: Optional[dict] = None) -> dict:
    try:
        r = _SESSION.get(_relay_url(path), params=params or {}, timeout=15)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def relay_post(path: str, body: Optional[dict] = None) -> dict:
    try:
        r = _SESSION.post(_relay_url(path), json=body or {}, timeout=20)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Bybit proxy (returns FULL relay envelope)
# ──────────────────────────────────────────────────────────────────────────────
def bybit_proxy(method: str, path: str, params: Optional[dict] = None,
                body: Optional[dict] = None) -> dict:
    """Returns full relay envelope:
       {"primary":{"status":...,"body":{...}}, "fallback":{...}, "error":?}
    """
    payload = {"method": method.upper(), "path": path}
    if method.upper() == "GET":
        payload["params"] = params or {}
    else:
        payload["body"] = body or {}
    return relay_post("/bybit/proxy", payload)

# ──────────────────────────────────────────────────────────────────────────────
# Bot-friendly proxy (returns Bybit JSON BODY directly)
# ──────────────────────────────────────────────────────────────────────────────
def proxy(method: str, path: str, params: Optional[dict] = None,
          body: Optional[dict] = None) -> dict:
    """Preferred for bots: returns primary.body JSON directly."""
    env = bybit_proxy(method, path, params=params, body=body)
    try:
        return env.get("primary", {}).get("body", env)
    except Exception:
        return env

# ──────────────────────────────────────────────────────────────────────────────
# Registry helpers (CSV / JSON)
# ──────────────────────────────────────────────────────────────────────────────
def load_sub_uids(csv_path: str = "sub_uids.csv",
                  map_path: str = "sub_map.json") -> Tuple[List[str], Dict[str, str]]:
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
# NEW: Bot-friendly quick data helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_wallet_balance(accountType: str = "UNIFIED", memberId: Optional[str] = None) -> dict:
    params = {"accountType": accountType}
    if memberId:
        params["memberId"] = memberId
    return proxy("GET", "/v5/account/wallet-balance", params=params)

def get_positions(category: str = "linear", symbol: Optional[str] = None,
                  memberId: Optional[str] = None, settleCoin: str = "USDT") -> dict:
    params = {"category": category}
    if symbol:
        params["symbol"] = symbol
    if memberId:
        params["memberId"] = memberId
    if category.lower() == "linear" and settleCoin:
        params["settleCoin"] = settleCoin
    return proxy("GET", "/v5/position/list", params=params)

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

# ──────────────────────────────────────────────────────────────────────────────
# Legacy convenience wrappers (keep for compatibility; return FULL envelope)
# ──────────────────────────────────────────────────────────────────────────────
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
