#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Client Helpers
- Loads env + token
- Relay HTTP helpers
- Telegram send
- Registry CSV/JSON helpers
- Bybit v5 proxy helpers (both low-level and bot-friendly)
"""

import os, json, csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Env / Globals
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

RELAY_BASE  = os.getenv("RELAY_BASE", "http://127.0.0.1:8080").rstrip("/")
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "")
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")

if not RELAY_TOKEN:
    raise RuntimeError("RELAY_TOKEN missing in .env")

HEADERS = {
    "Authorization": f"Bearer {RELAY_TOKEN}",
    # Lets API calls bypass ngrok browser interstitial if RELAY_BASE is an ngrok URL
    "ngrok-skip-browser-warning": "true"
}

# ──────────────────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text},
            timeout=10
        )
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Raw relay HTTP
# ──────────────────────────────────────────────────────────────────────────────
def relay_get(path: str, params: Optional[dict] = None) -> dict:
    url = f"{RELAY_BASE}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=25)
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code, "text": r.text}

def relay_post(path: str, body: Optional[dict] = None) -> dict:
    url = f"{RELAY_BASE}{path}"
    r = requests.post(url, headers=HEADERS, json=body or {}, timeout=30)
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code, "text": r.text}

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Bybit proxy (returns FULL relay envelope)
# ──────────────────────────────────────────────────────────────────────────────
def bybit_proxy(method: str, path: str, params: Optional[dict] = None,
                body: Optional[dict] = None) -> dict:
    """
    Low-level: returns the full relay response envelope:
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
    """
    Preferred for bots: returns the primary.body JSON directly (retCode/retMsg/result).
    If the envelope isn't present (unexpected), returns the raw response.
    """
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
#   → These are what your bots (pnl_logger, coach, risk_daemon, tp_sl_manager) should call.
#   → They return Bybit JSON (retCode/retMsg/result).
# ──────────────────────────────────────────────────────────────────────────────
def get_wallet_balance(accountType: str = "UNIFIED", memberId: Optional[str] = None) -> dict:
    """
    Returns wallet balance body via relay.
    - If memberId is provided, queries that sub-account; otherwise master.
    """
    params = {"accountType": accountType}
    if memberId:
        params["memberId"] = memberId
    return proxy("GET", "/v5/account/wallet-balance", params=params)

def get_positions(category: str = "linear", symbol: Optional[str] = None,
                  memberId: Optional[str] = None, settleCoin: str = "USDT") -> dict:
    """
    Returns positions list body via relay.
    - category: 'linear' (default), 'inverse', etc.
    - symbol: optional filter
    - memberId: optional sub-account member id
    - settleCoin: required by Bybit for some 'linear' calls; default 'USDT'
    """
    params = {"category": category}
    if symbol:
        params["symbol"] = symbol
    if memberId:
        params["memberId"] = memberId
    if category.lower() == "linear" and settleCoin:
        params["settleCoin"] = settleCoin
    return proxy("GET", "/v5/position/list", params=params)

# (Optional but handy) More helpers in the same "body-returning" style:
def get_open_orders(category: str = "linear", symbol: Optional[str] = None,
                    memberId: Optional[str] = None, openOnly: int = 1) -> dict:
    p = {"category": category, "openOnly": openOnly}
    if symbol: p["symbol"] = symbol
    if memberId: p["memberId"] = memberId
    return proxy("GET", "/v5/order/realtime", params=p)

def get_order_history(category: str = "linear", symbol: Optional[str] = None,
                      memberId: Optional[str] = None, limit: int = 200) -> dict:
    p = {"category": category, "limit": limit}
    if symbol: p["symbol"] = symbol
    if memberId: p["memberId"] = memberId
    return proxy("GET", "/v5/order/history", params=p)

def get_execution_list(category: str = "linear", symbol: Optional[str] = None,
                       memberId: Optional[str] = None, limit: int = 200) -> dict:
    p = {"category": category, "limit": limit}
    if symbol: p["symbol"] = symbol
    if memberId: p["memberId"] = memberId
    return proxy("GET", "/v5/execution/list", params=p)

def get_ticker(symbol: str, category: str = "linear") -> dict:
    return proxy("GET", "/v5/market/tickers", params={"category": category, "symbol": symbol})

# ──────────────────────────────────────────────────────────────────────────────
# Legacy convenience wrappers (keep for compatibility; return FULL envelope)
#   → You can delete these later if nothing uses them.
# ──────────────────────────────────────────────────────────────────────────────
def get_balance_unified(member_id: str) -> dict:
    return bybit_proxy(
        "GET",
        "/v5/account/wallet-balance",
        params={"accountType": "UNIFIED", "memberId": member_id}
    )

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
