#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Relay — Hardened + Diagnostics (Flask)

- Auth: Bearer or x-relay-token (also ?token= for quick curls)
- Generic proxy:        POST /bybit/proxy      (Bybit v5 only; signed)
- Native helpers:       GET  /bybit/wallet/balance
                        GET  /bybit/positions
                        GET  /bybit/tickers
                        GET  /bybit/subuids
- Base44 helpers:       GET  /getAccountData   ← ALWAYS returns an ARRAY of accounts
                        GET  /getEquityCurve
- Legacy shims:         GET  /v1/wallet/balance
                        GET  /v1/order/realtime
                        GET  /v1/position/list
                        GET  /v5/position/list  (compat)
                        GET  /v5/market/tickers (compat)
- Diagnostics:          GET  /health
                        GET  /diag/time
                        GET  /diag/bybit       (auth sanity)
                        GET  /heartbeat        (TG ping)
                        GET  /diag/telegram    (verifies token + recent chat IDs)

.env keys:
  RELAY_TOKEN=...
  ALLOWED_ORIGINS=http://localhost:5000
  BYBIT_ENV=mainnet  # or testnet
  BYBIT_BASE=        # optional; auto-picked from BYBIT_ENV if empty
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_RECV_WINDOW=20000
  RELAY_TIMEOUT=25
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...
  RELAY_HOST=0.0.0.0
  RELAY_PORT=8080
"""

from __future__ import annotations

import os
import time
import hmac
import json
import csv
import hashlib
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import requests
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Config / Env
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

ROOT             = Path(__file__).resolve().parent
RELAY_TOKEN      = (os.getenv("RELAY_TOKEN") or "").strip()
ALLOWED_ORIGINS  = [o.strip() for o in (os.getenv("ALLOWED_ORIGINS") or "").split(",") if o.strip()]
BYBIT_ENV        = (os.getenv("BYBIT_ENV") or "mainnet").strip().lower()
BYBIT_BASE       = (os.getenv("BYBIT_BASE") or "").strip().rstrip("/")
if not BYBIT_BASE:
    BYBIT_BASE = "https://api-testnet.bybit.com" if BYBIT_ENV == "testnet" else "https://api.bybit.com"

BYBIT_API_KEY    = (os.getenv("BYBIT_API_KEY") or "").strip()
BYBIT_API_SECRET = (os.getenv("BYBIT_API_SECRET") or "").strip()

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

RECV_WINDOW     = (os.getenv("BYBIT_RECV_WINDOW") or "20000").strip()
TIMEOUT_S       = float(os.getenv("RELAY_TIMEOUT") or "25")

if not RELAY_TOKEN:
    raise RuntimeError("RELAY_TOKEN missing in .env")
if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    raise RuntimeError("BYBIT_API_KEY/BYBIT_API_SECRET missing in .env")

app = Flask(__name__)
CORS(app, origins=ALLOWED_ORIGINS or ["*"])

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("base44_relay")

# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def _json_ok(**payload):
    return jsonify({"ok": True, **payload})

def _json_err(code: int, message: str, **extra):
    resp = jsonify({"ok": False, "error": message, **extra})
    return make_response(resp, code)

def tg_send(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
    except Exception:
        pass

def _tg_get_updates() -> Dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "no_token"}
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates", timeout=10)
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _get_token_from_request() -> Optional[str]:
    # Accept either Bearer or x-relay-token; also ?token= for quick tests
    auth = request.headers.get("Authorization", "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    hdr = request.headers.get("x-relay-token", "").strip()
    if hdr:
        return hdr
    q = request.args.get("token", "").strip()
    if q:
        return q
    return None

def require_auth(func):
    def wrapper(*args, **kwargs):
        _ = request.headers.get("ngrok-skip-browser-warning")  # ignored
        token = _get_token_from_request()
        if not token or token != RELAY_TOKEN:
            return _json_err(401, "unauthorized")
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    wrapper.__doc__  = func.__doc__
    return wrapper

# ──────────────────────────────────────────────────────────────────────────────
# Bybit signing / request helpers (v5)
# ──────────────────────────────────────────────────────────────────────────────
def _canonical_query(params: Optional[dict]) -> str:
    if not params:
        return ""
    pairs = []
    for k in sorted(params.keys()):
        v = params[k]
        if v is None:
            continue
        pairs.append(f"{k}={v}")
    return "&".join(pairs)

def _bybit_headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }

def _sign_payload(method: str, params: Optional[dict], body: Optional[dict]) -> Tuple[str, str]:
    """Return (timestamp_ms, signature) for v5.
    prehash = f"{ts}{api_key}{recv_window}{query_string_or_body}"""
    ts = str(int(time.time() * 1000))
    if method.upper() == "GET":
        payload_str = _canonical_query(params)
    else:
        payload_str = json.dumps(body or {}, separators=(",", ":"))
    prehash = f"{ts}{BYBIT_API_KEY}{RECV_WINDOW}{payload_str}"
    sign = hmac.new(BYBIT_API_SECRET.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
    return ts, sign

def _http_call(method: str, path: str, params: Optional[dict], body: Optional[dict]) -> Tuple[int, Any]:
    """Low-level HTTP with proper signing. Returns (status_code, parsed_or_text)."""
    method = method.upper()
    ts, sign = _sign_payload(method, params, body)
    headers = _bybit_headers(ts, sign)
    url = f"{BYBIT_BASE}{path}"
    try:
        if method == "GET":
            r = requests.get(url, params=params or {}, headers=headers, timeout=TIMEOUT_S)
        elif method == "DELETE":
            r = requests.delete(url, json=body or {}, headers=headers, timeout=TIMEOUT_S)
        else:
            r = requests.post(url, json=body or {}, headers=headers, timeout=TIMEOUT_S)

        try:
            data = r.json()
        except Exception:
            data = r.text

        if r.status_code == 401:
            hint = {
                "hint": "Bybit 401: check API key/secret, IP allowlist, permissions; ensure v5 signing and correct endpoint.",
                "endpoint": path,
                "bybit_base": BYBIT_BASE,
                "recvWindow": RECV_WINDOW,
            }
            if isinstance(data, dict):
                data = {**data, **hint}
            else:
                data = {"raw": data, **hint}

        return r.status_code, data
    except requests.RequestException as e:
        return 599, {"error": "request_exception", "detail": str(e)}

def bybit_proxy_internal(payload: dict) -> Dict[str, Any]:
    method = (payload.get("method") or "GET").upper()
    path   = payload.get("path") or ""
    params = payload.get("params") or {}
    body   = payload.get("body") or {}

    status_p, body_p = _http_call(method, path, params, body)
    if status_p in (408, 425, 429, 500, 502, 503, 504, 599):
        status_f, body_f = _http_call(method, path, params, body)
    else:
        status_f, body_f = status_p, body_p

    top_error = None
    if isinstance(body_p, dict) and (body_p.get("retCode") not in (0, None)):
        top_error = "bybit_error"

    return {
        "primary":  {"status": status_p, "body": body_p},
        "fallback": {"status": status_f, "body": body_f},
        **({"error": top_error} if top_error else {}),
    }

def _passthrough_primary(prox: Dict[str, Any]):
    """Return primary body/status like a normal API, not the proxy envelope."""
    primary = prox.get("primary", {})
    status  = int(primary.get("status", 200))
    body    = primary.get("body")
    try:
        resp = make_response(jsonify(body), status)
    except Exception:
        resp = make_response(body or "", status)
    return resp

# ──────────────────────────────────────────────────────────────────────────────
# Registry helpers for sub accounts
# ──────────────────────────────────────────────────────────────────────────────
def _load_sub_uids() -> List[str]:
    p = ROOT / "registry" / "sub_uids.csv"
    out: List[str] = []
    if p.exists():
        with p.open("r", newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if not row:
                    continue
                cand = (row[0] or "").strip()
                if cand and cand.isdigit():
                    out.append(cand)
    return out

def _pretty_name(uid: str) -> str:
    mp = ROOT / "registry" / "sub_map.json"
    if mp.exists():
        try:
            js = json.loads(mp.read_text(encoding="utf-8"))
            nm = (js.get(uid) or {}).get("name") or (js.get(uid) or {}).get("label")
            if nm:
                return nm
        except Exception:
            pass
    return f"sub:{uid}"

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return _json_ok(
        service="base44_relay",
        env=BYBIT_ENV,
        bybit_base=BYBIT_BASE,
        api_key_present=bool(BYBIT_API_KEY),
        recvWindow=RECV_WINDOW,
        hasDiagTime=True
    )

@app.get("/diag/time")
def diag_time():
    return jsonify({"ok": True, "localEpochMs": int(time.time() * 1000)})

@app.get("/diag/bybit")
@require_auth
def diag_bybit():
    prox = bybit_proxy_internal({
        "method": "GET",
        "path": "/v5/account/wallet-balance",
        "params": {"accountType": "UNIFIED"},
    })
    return _passthrough_primary(prox)

@app.get("/heartbeat")
@require_auth
def heartbeat():
    note = request.args.get("note") or "heartbeat"
    tg_send(f"💓 Base44 Relay heartbeat — {note}")
    return _json_ok(message="sent")

@app.get("/diag/telegram")
@require_auth
def diag_telegram():
    """Quick check: shows recent chat_ids seen by the bot so you can copy the right one."""
    data = _tg_get_updates()
    chats = []
    try:
        for upd in data.get("result", []):
            msg = upd.get("message") or upd.get("channel_post") or {}
            chat = msg.get("chat") or {}
            cid = chat.get("id")
            title = chat.get("title")
            uname = chat.get("username")
            if cid:
                chats.append({"chat_id": cid, "title": title, "username": uname})
    except Exception:
        pass
    return _json_ok(ok=data.get("ok", False), chats=chats)

# ---- Generic proxy (v5 only) ----
@app.post("/bybit/proxy")
@require_auth
def bybit_proxy():
    payload = request.get_json(silent=True) or {}
    if "path" not in payload:
        return _json_err(400, "missing 'path'")
    path = payload["path"]
    if not path.startswith("/v5/"):
        return _json_err(400, "only /v5/* paths are allowed")
    prox = bybit_proxy_internal(payload)
    return jsonify(prox)

# ---- Native helpers ----
@app.get("/bybit/wallet/balance")
@require_auth
def wallet_balance_native():
    params = {"accountType": request.args.get("accountType", "UNIFIED")}
    coin   = request.args.get("coin")
    # Bybit v5 uses memberId for sub accounts on many endpoints
    member = request.args.get("memberId") or request.args.get("subUid")
    if coin: params["coin"] = coin
    if member: params["memberId"] = member
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/account/wallet-balance", "params": params})
    return _passthrough_primary(prox)

@app.get("/bybit/positions")
@require_auth
def positions_native():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    settle = request.args.get("settleCoin") or request.args.get("settle") or "USDT"
    member = request.args.get("memberId") or request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if settle: params["settleCoin"] = settle
    if member: params["memberId"] = member
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/bybit/tickers")
@require_auth
def tickers_native():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    if symbol: params["symbol"] = symbol
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/market/tickers", "params": params})
    return _passthrough_primary(prox)

# ---- Sub-UIDs (master only) ----
@app.get("/bybit/subuids")
@require_auth
def bybit_subuids():
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/user/query-sub-members", "params": {}})
    body = prox.get("primary", {}).get("body", {}) or {}
    uids = []
    try:
        for item in (body.get("result", {}) or {}).get("list", []) or []:
            uid = item.get("uid") or item.get("memberId") or item.get("subMemberId")
            if uid: uids.append(str(uid))
    except Exception:
        pass
    if not uids and prox.get("error"):
        return _json_err(502, "bybit_error", proxy=prox)
    return _json_ok(source="user/query-sub-members", sub_uids=uids)

# ---- Base44 helpers ----
@app.route("/getAccountData", methods=["GET", "POST"])
@require_auth
def get_account_data():
    """
    Returns an ARRAY of accounts, always.
    Each item: { uid, label, accountType, equity, walletBalance, unrealisedPnl }
    Includes main, plus any registry/ sub_uids.csv if present.
    """
    coin = request.args.get("coin", "USDT")
    account_type = request.args.get("accountType", "UNIFIED")

    accounts = []

    # main
    params = {"accountType": account_type, "coin": coin}
    d_main = bybit_proxy_internal({"method": "GET", "path": "/v5/account/wallet-balance", "params": params})
    body_m = d_main.get("primary", {}).get("body", {}) or {}
    lst_m  = (((body_m.get("result") or {}).get("list")) or [])
    if lst_m:
        r = lst_m[0]
        accounts.append({
            "uid": "main",
            "label": "main",
            "accountType": account_type,
            "equity": float(r.get("totalEquity") or 0),
            "walletBalance": float(r.get("walletBalance") or 0),
            "unrealisedPnl": float(r.get("unrealisedPnl") or 0),
        })

    # subs (optional)
    for uid in _load_sub_uids():
        params = {"accountType": account_type, "coin": coin, "memberId": uid}
        d_sub = bybit_proxy_internal({"method": "GET", "path": "/v5/account/wallet-balance", "params": params})
        body_s = d_sub.get("primary", {}).get("body", {}) or {}
        lst_s  = (((body_s.get("result") or {}).get("list")) or [])
        if lst_s:
            r = lst_s[0]
            accounts.append({
                "uid": uid,
                "label": _pretty_name(uid),
                "accountType": account_type,
                "equity": float(r.get("totalEquity") or 0),
                "walletBalance": float(r.get("walletBalance") or 0),
                "unrealisedPnl": float(r.get("unrealisedPnl") or 0),
            })

    # normalize to array, always
    if not isinstance(accounts, list):
        accounts = [accounts] if accounts else []

    return jsonify(accounts), 200

@app.route("/getEquityCurve", methods=["GET", "POST"])
@require_auth
def get_equity_curve():
    prox = bybit_proxy_internal({
        "method": "GET",
        "path": "/v5/account/wallet-balance",
        "params": {"accountType": "UNIFIED"},
    })
    body = prox.get("primary", {}).get("body", {}) or {}
    total = 0.0
    try:
        for acct in (body.get("result", {}) or {}).get("list", []) or []:
            total += float(acct.get("totalEquity", 0))
    except Exception:
        pass
    return _json_ok(equityCurve=[{"t": int(time.time()*1000), "v": total}], totalEquity=total)

# ---- Legacy/compat shims (UI callers) ----
@app.get("/v1/wallet/balance")
@require_auth
def legacy_wallet_balance():
    params = {"accountType": request.args.get("accountType", "UNIFIED")}
    coin   = request.args.get("coin")
    member = request.args.get("memberId") or request.args.get("subUid")
    if coin: params["coin"] = coin
    if member: params["memberId"] = member
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/account/wallet-balance", "params": params})
    return _passthrough_primary(prox)

@app.get("/v1/order/realtime")
@require_auth
def legacy_order_realtime():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    if symbol: params["symbol"] = symbol
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/order/realtime", "params": params})
    return _passthrough_primary(prox)

@app.get("/v1/position/list")
@require_auth
def legacy_position_list_v1():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    settle = request.args.get("settleCoin") or "USDT"
    member = request.args.get("memberId") or request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if settle: params["settleCoin"] = settle
    if member: params["memberId"] = member
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/v5/position/list")
@require_auth
def compat_position_list_v5():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    settle = request.args.get("settleCoin") or "USDT"
    member = request.args.get("memberId") or request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if settle: params["settleCoin"] = settle
    if member: params["memberId"] = member
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/v5/market/tickers")
@require_auth
def compat_market_tickers_v5():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    if symbol: params["symbol"] = symbol
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/market/tickers", "params": params})
    return _passthrough_primary(prox)

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = os.getenv("RELAY_HOST", "0.0.0.0")
    try:
        port = int(os.getenv("RELAY_PORT", "8080"))
    except ValueError:
        port = 8080
    log.info(f"Starting Base44 Relay on http://{host}:{port} → {BYBIT_BASE}")
    log.info(f"Loaded from: {os.path.abspath(__file__)}")
    app.run(host=host, port=port, debug=False)
