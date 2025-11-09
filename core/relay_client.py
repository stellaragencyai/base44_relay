#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/relay_client.py — hardened, sub-account aware, automation-ready

Key features
- Reads relay base from: RELAY_URL | BASE44_RELAY_URL | EXECUTOR_RELAY_BASE | RELAY_BASE | default http://127.0.0.1:5000
- Reads token from: RELAY_TOKEN | BASE44_RELAY_TOKEN | EXECUTOR_RELAY_TOKEN
- Unified requests.Session with Bearer; retries + backoff (idempotent GET/POST)
- Robust JSON unwrapping across relay shapes (envelope/raw)
- `extra={'subUid': ...}` passthrough for sub-account routing, plus context helper with `with_sub`
- Convenience v5 wrappers: create_limit, cancel_order, create_sl_market, set_tp_sl, get_positions, get_open_orders, etc.
- Health helpers: `is_token_ok()`, `/diag/ping` probe
- Safe Telegram sender with notifier fallback
"""

from __future__ import annotations

import os
import json
import csv
import time
import random
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Callable

import requests
from requests import Response
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Env / Globals
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

_RELAY_BASE = (
    os.getenv("RELAY_URL")
    or os.getenv("BASE44_RELAY_URL")
    or os.getenv("EXECUTOR_RELAY_BASE")
    or os.getenv("RELAY_BASE")
    or "http://127.0.0.1:5000"
).rstrip("/")

_RELAY_TOKEN = (
    os.getenv("RELAY_TOKEN")
    or os.getenv("BASE44_RELAY_TOKEN")
    or os.getenv("EXECUTOR_RELAY_TOKEN")
    or ""
).strip()

HTTP_TIMEOUT_S = int(os.getenv("HTTP_TIMEOUT_S", "15") or "15")
BYBIT_PUBLIC = (os.getenv("BYBIT_BASE_URL") or "https://api.bybit.com").rstrip("/")

DEFAULT_SUB_UID = (os.getenv("DEFAULT_SUB_UID") or "").strip()

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
    "User-Agent": "Base44-RelayClient/1.4",
})

# Per-process default subUid context
_default_extra: Dict[str, Any] = {"subUid": DEFAULT_SUB_UID} if DEFAULT_SUB_UID else {}

def _u(path: str) -> str:
    path = path if path.startswith("/") else f"/{path}"
    return f"{_RELAY_BASE}{path}"

def _json_or_text(resp: Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {"status": resp.status_code, "raw": (resp.text or "")[:2000]}

def _raise_for_auth(resp: Response) -> None:
    if resp.status_code == 401:
        raise RuntimeError("401 from relay: token missing/invalid")
    if resp.status_code == 403:
        raise RuntimeError("403 from relay: forbidden (role/allowlist)")

def _backoff(i: int, base: float = 0.35, cap: float = 2.5) -> float:
    # jittered exponential backoff
    return min(cap, base * (2 ** i)) * (0.9 + random.random() * 0.2)

def _retry_call(fn: Callable[[], Response], *, retries=2) -> Response:
    last_exc: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            r = fn()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(_backoff(i))
                continue
            return r
        except Exception as e:
            last_exc = e
            time.sleep(_backoff(i))
    if last_exc:
        raise last_exc
    raise RuntimeError("relay request failed after retries")

# ──────────────────────────────────────────────────────────────────────────────
# Telegram convenience (uses tools.notifier_telegram if present)
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(text: str, parse_mode: Optional[str] = None) -> None:
    try:
        from tools.notifier_telegram import tg  # type: ignore
        tg.send_text(text, parse_mode=parse_mode)
        return
    except Exception:
        pass
    # ultra-minimal fallback; best-effort only
    tok = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    chat = os.getenv("TELEGRAM_CHAT_ID") or ""
    if not tok or not chat:
        return
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    payload = {"chat_id": chat, "text": text}
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
# Raw relay HTTP with retry
# ──────────────────────────────────────────────────────────────────────────────
def relay_get(path: str, params: Optional[dict] = None, timeout: int = HTTP_TIMEOUT_S) -> dict:
    try:
        def go():
            return _SESSION.get(_u(path), params=params or {}, timeout=timeout)
        r = _retry_call(go)
        _raise_for_auth(r)
        return _json_or_text(r)
    except Exception as e:
        return {"error": str(e), "path": path, "params": params or {}}

def relay_post(path: str, body: Optional[dict] = None, timeout: int = HTTP_TIMEOUT_S) -> dict:
    try:
        def go():
            return _SESSION.post(_u(path), json=body or {}, timeout=timeout)
        r = _retry_call(go)
        _raise_for_auth(r)
        return _json_or_text(r)
    except Exception as e:
        return {"error": str(e), "path": path, "body": body or {}}

# ──────────────────────────────────────────────────────────────────────────────
# Envelope helpers
# ──────────────────────────────────────────────────────────────────────────────
def _unwrap_body(env: Any) -> dict:
    """
    Accepts:
      - {"primary":{"status":...,"body":{...}}}
      - raw Bybit body { "retCode": 0, "result": {...} }
      - {"status": 200, "raw": "..."}  → return {}
    Returns safe dict.
    """
    if isinstance(env, dict):
        primary = env.get("primary")
        if isinstance(primary, dict):
            body = primary.get("body")
            if isinstance(body, dict):
                return body
        if "retCode" in env or "result" in env:
            return env
    return {}

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Bybit proxy (returns FULL relay envelope)
# Supports `extra` context (e.g., {"subUid":"260417078"})
# ──────────────────────────────────────────────────────────────────────────────
def bybit_proxy(method: str, path: str, params: Optional[dict] = None,
                body: Optional[dict] = None, extra: Optional[dict] = None,
                timeout: int = HTTP_TIMEOUT_S) -> dict:
    payload: Dict[str, Any] = {"method": method.upper(), "path": path}
    if params:
        payload["params"] = params
    if body:
        payload["body"] = body
    merged_extra = dict(_default_extra)
    if extra:
        merged_extra.update(extra)
    if merged_extra:
        payload["extra"] = merged_extra
    return relay_post("/bybit/proxy", body=payload, timeout=timeout)

# ──────────────────────────────────────────────────────────────────────────────
# Bot-friendly proxy (returns Bybit JSON BODY directly)
# Accepts both params= and qs= for compatibility.
# ──────────────────────────────────────────────────────────────────────────────
def proxy(method: str, path: str, *,
          params: Optional[dict] = None,
          qs: Optional[dict] = None,
          body: Optional[dict] = None,
          extra: Optional[dict] = None,
          timeout: int = HTTP_TIMEOUT_S) -> dict:
    if qs and not params:
        params = qs
    env = bybit_proxy(method, path, params=params, body=body, extra=extra, timeout=timeout)
    return _unwrap_body(env) or env

# ──────────────────────────────────────────────────────────────────────────────
# Sub-account context helper
# ──────────────────────────────────────────────────────────────────────────────
@contextmanager
def with_sub(sub_uid: str):
    """
    Temporarily set default subUid context for all proxy/bybit_proxy calls in this process.
    Usage:
        with with_sub("260417078"):
            proxy("GET", "/v5/position/list", params={"category":"linear"})
    """
    global _default_extra
    old = dict(_default_extra)
    try:
        _default_extra = {"subUid": str(sub_uid).strip()}
        yield
    finally:
        _default_extra = old

# ──────────────────────────────────────────────────────────────────────────────
# Quick data helpers (sub-account aware via `extra` or with_sub)
# ──────────────────────────────────────────────────────────────────────────────
def equity_unified(coin: Optional[str] = None, extra: Optional[dict] = None) -> float:
    params = {"accountType": "UNIFIED"}
    if coin:
        params["coin"] = coin
    env = bybit_proxy("GET", "/v5/account/wallet-balance", params=params, extra=extra)
    body = _unwrap_body(env)
    try:
        result = (body.get("result") or {})
        total = 0.0
        for acct in (result.get("list") or []):
            total += float(acct.get("totalEquity", 0))
        return total
    except Exception:
        return 0.0

def ticker(symbol: str, category: str = "linear") -> dict | None:
    sym = (symbol or "").upper().strip()
    if not sym:
        return None
    j = proxy("GET", "/v5/market/tickers", params={"category": category, "symbol": sym})
    try:
        arr = ((j.get("result") or {}).get("list") or [])
        if arr:
            rec = dict(arr[0])
            for k in ("bid1Price", "ask1Price", "lastPrice"):
                if k in rec:
                    rec[k] = str(rec[k])
            return rec
    except Exception:
        pass
    # public fallback
    try:
        r = requests.get(
            f"{BYBIT_PUBLIC}/v5/market/tickers",
            params={"category": category, "symbol": sym},
            timeout=HTTP_TIMEOUT_S,
        )
        if r.ok:
            data = r.json()
            if data.get("retCode") == 0:
                arr = ((data.get("result") or {}).get("list") or [])
                return arr[0] if arr else None
    except Exception:
        return None
    return None

def klines(symbol: str, interval: str = "1", limit: int = 50, category: str = "linear") -> list:
    params = {"category": category, "symbol": symbol, "interval": interval, "limit": limit}
    body = proxy("GET", "/v5/market/kline", params=params)
    return ((body.get("result") or {}).get("list") or []) if isinstance(body, dict) else []

# ──────────────────────────────────────────────────────────────────────────────
# v5 Order helpers
# ──────────────────────────────────────────────────────────────────────────────
def create_limit(symbol: str, side: str, qty: float, price: float, *,
                 category: str = "linear", post_only: bool = True,
                 reduce_only: bool = True, link_id: Optional[str] = None,
                 extra: Optional[dict] = None) -> dict:
    p = {
        "category": category,
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": f"{qty}",
        "price": f"{price:.10f}".rstrip("0").rstrip("."),
        "timeInForce": "PostOnly" if post_only else "GoodTillCancel",
        "reduceOnly": reduce_only,
    }
    if link_id:
        p["orderLinkId"] = link_id
    return proxy("POST", "/v5/order/create", body=p, extra=extra)

def cancel_order(symbol: str, *, order_id: Optional[str] = None,
                 link_id: Optional[str] = None, category: str = "linear",
                 extra: Optional[dict] = None) -> dict:
    if not order_id and not link_id:
        raise ValueError("cancel_order requires order_id or link_id")
    p = {"category": category, "symbol": symbol}
    if order_id:
        p["orderId"] = order_id
    if link_id:
        p["orderLinkId"] = link_id
    return proxy("POST", "/v5/order/cancel", body=p, extra=extra)

def create_sl_market(symbol: str, side: str, qty: float, trigger_price: float, *,
                     trigger_by: str = "MARKPRICE", category: str = "linear",
                     extra: Optional[dict] = None) -> dict:
    p = {
        "category": category,
        "symbol": symbol,
        "side": "Sell" if side == "Buy" else "Buy",
        "orderType": "Market",
        "qty": f"{qty}",
        "reduceOnly": True,
        "triggerPrice": f"{trigger_price:.10f}".rstrip("0").rstrip("."),
        "triggerBy": trigger_by.upper(),
        "tpslMode": "Partial"
    }
    return proxy("POST", "/v5/order/create", body=p, extra=extra)

def set_tp_sl(symbol: str, side: str, *, tp: Optional[float] = None,
              sl: Optional[float] = None, category: str = "linear",
              extra: Optional[dict] = None) -> dict:
    p: Dict[str, Any] = {"category": category, "symbol": symbol, "positionIdx": 0}
    if tp is not None:
        p["takeProfit"] = f"{tp:.10f}".rstrip("0").rstrip(".")
    if sl is not None:
        p["stopLoss"] = f"{sl:.10f}".rstrip("0").rstrip(".")
    return proxy("POST", "/v5/position/trading-stop", body=p, extra=extra)

def get_open_orders(category: str = "linear", symbol: Optional[str] = None,
                    extra: Optional[dict] = None, openOnly: int = 1) -> dict:
    p = {"category": category, "openOnly": openOnly}
    if symbol:
        p["symbol"] = symbol
    return proxy("GET", "/v5/order/realtime", params=p, extra=extra)

def get_positions(category: str = "linear", symbol: Optional[str] = None,
                  extra: Optional[dict] = None, settleCoin: str = "USDT") -> dict:
    p = {"category": category}
    if symbol:
        p["symbol"] = symbol
    if category.lower() == "linear" and settleCoin:
        p["settleCoin"] = settleCoin
    return proxy("GET", "/v5/position/list", params=p, extra=extra)

def get_wallet_balance(accountType: str = "UNIFIED", extra: Optional[dict] = None) -> dict:
    return proxy("GET", "/v5/account/wallet-balance", params={"accountType": accountType}, extra=extra)

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
            try:
                rd = csv.DictReader(f)
                for row in rd:
                    val = (row.get("sub_uid") or "").strip()
                    if val:
                        uids.append(val)
            except Exception:
                f.seek(0)
                for line in f:
                    s = line.strip().split(",")[0].strip()
                    if s and s.lower() != "sub_uid":
                        uids.append(s)
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
# ──────────────────────────────────────────────────────────────────────────────
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
    try:
        r = _SESSION.get(_u("/diag/ping"), timeout=5)
        _raise_for_auth(r)
        _ = r.json()
        return True
    except Exception:
        return False

if __name__ == "__main__":
    print(relay_get("/diag/time"))
    print("token_ok:", is_token_ok())
