#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Relay — Hardened (Flask)
- Auth: Bearer or x-relay-token
- Generic proxy: POST /bybit/proxy  (Bybit v5 only)
- Native helpers:  GET /bybit/wallet/balance, /bybit/positions, /bybit/tickers, /bybit/subuids
- Legacy shims:    /v1/wallet/balance, /v1/order/realtime, /v1/position/list → mapped to v5
- Diagnostics:     /health, /heartbeat, /diag/bybit (auth sanity), /diag/time (clock check)
- Telegram heartbeat supported

.env keys:
  RELAY_TOKEN=...
  ALLOWED_ORIGINS=http://localhost:5000
  BYBIT_ENV=mainnet  # or testnet
  BYBIT_BASE=        # optional; auto-picked from BYBIT_ENV if empty
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_RECV_WINDOW=10000
  RELAY_TIMEOUT=25
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...
  RELAY_HOST=127.0.0.1
  RELAY_PORT=5000
"""

import os
import time
import hmac
import json
import hashlib
import logging
from typing import Optional, Tuple, Dict, Any
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from dotenv import load_dotenv
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Config / Env
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

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

RECV_WINDOW     = (os.getenv("BYBIT_RECV_WINDOW") or "10000").strip()  # give ourselves more headroom
TIMEOUT_S       = float(os.getenv("RELAY_TIMEOUT") or "25")

if not RELAY_TOKEN:
    raise RuntimeError("RELAY_TOKEN missing in .env")
if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    raise RuntimeError("BYBIT_API_KEY/BYBIT_API_SECRET missing in .env")

app = Flask(__name__)
CORS(app, origins=ALLOWED_ORIGINS or ["*"])

logging.basicConfig(level=logging.INFO)
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
    """Bybit v5 signature expects k=v pairs joined by &, sorted by key.
    Do NOT URL-encode for the string-to-sign. Exclude None values.
    """
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
    # X-BAPI-SIGN-TYPE can be sent as 2, but it’s optional for most accounts.
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }

def _sign_payload(method: str, params: Optional[dict], body: Optional[dict]) -> Tuple[str, str]:
    """Return (timestamp_ms, signature) for v5.
    prehash = f"{ts}{api_key}{recv_window}{query_string_or_body}"
    GET → use canonical query string
    POST → use compact JSON string ({} if empty)
    """
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
            # Send actual query params (requests will encode), but sign over canonical unencoded
            r = requests.get(url, params=params or {}, headers=headers, timeout=TIMEOUT_S)
        else:
            r = requests.post(url, json=body or {}, headers=headers, timeout=TIMEOUT_S)
        # Try JSON, otherwise return text
        try:
            data = r.json()
        except Exception:
            data = r.text

        # Helpful mapping for 401s
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

    # call twice like your original, but only retry on transient
    status_p, body_p = _http_call(method, path, params, body)
    if status_p in (408, 425, 429, 500, 502, 503, 504, 599):
        status_f, body_f = _http_call(method, path, params, body)
    else:
        status_f, body_f = status_p, body_p

    # annotate top error
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
    )

@app.get("/diag/time")
def diag_time():
    # quick sanity for local clock skew
    return _json_ok(localEpochMs=int(time.time() * 1000))

@app.get("/diag/bybit")
@require_auth
def diag_bybit():
    """Cheap auth sanity: call a harmless v5 endpoint that requires a valid signature."""
    prox = bybit_proxy_internal({
        "method": "GET",
        "path": "/v5/account/wallet-balance",
        "params": {"accountType": "UNIFIED"},
    })
    return _passthrough_primary(prox)

@app.post("/heartbeat")
@app.get("/heartbeat")
@require_auth
def heartbeat():
    note = request.args.get("note") or (request.get_json(silent=True) or {}).get("note") or "heartbeat"
    tg_send(f"💓 Base44 Relay heartbeat — {note}")
    return _json_ok(message="sent")

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
    params = {
        "accountType": request.args.get("accountType", "UNIFIED")
    }
    coin   = request.args.get("coin")
    subUid = request.args.get("subUid")
    if coin: params["coin"] = coin
    if subUid: params["subUid"] = subUid
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/account/wallet-balance", "params": params})
    return _passthrough_primary(prox)

@app.get("/bybit/positions")
@require_auth
def positions_native():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    subUid = request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if subUid: params["subUid"] = subUid
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/bybit/tickers")
@require_auth
def tickers_native():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    subUid = request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if subUid: params["subUid"] = subUid
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

# ---- Base44 helper routes ----
@app.get("/getAccountData")
@require_auth
def get_account_data():
    prox = bybit_proxy_internal({
        "method": "GET",
        "path": "/v5/account/wallet-balance",
        "params": {"accountType": "UNIFIED"},
    })
    body = prox.get("primary", {}).get("body", {}) or {}
    return _json_ok(account=body)

@app.get("/getEquityCurve")
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
    # map to v5 to kill 401s on old path
    params = {"accountType": request.args.get("accountType", "UNIFIED")}
    coin   = request.args.get("coin")
    subUid = request.args.get("subUid")
    if coin: params["coin"] = coin
    if subUid: params["subUid"] = subUid
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
    if symbol: params["symbol"] = symbol
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/v5/position/list")
@require_auth
def compat_position_list_v5():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    subUid = request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if subUid: params["subUid"] = subUid
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/position/list", "params": params})
    return _passthrough_primary(prox)

@app.get("/v5/market/tickers")
@require_auth
def compat_market_tickers_v5():
    params = {"category": request.args.get("category", "linear")}
    symbol = request.args.get("symbol")
    subUid = request.args.get("subUid")
    if symbol: params["symbol"] = symbol
    if subUid: params["subUid"] = subUid
    prox = bybit_proxy_internal({"method": "GET", "path": "/v5/market/tickers", "params": params})
    return _passthrough_primary(prox)

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Single source of truth; no 8080 surprise
    host = os.getenv("RELAY_HOST", "127.0.0.1")
    try:
        port = int(os.getenv("RELAY_PORT", "5000"))
    except ValueError:
        port = 5000
    log.info(f"Starting Base44 Relay on http://{host}:{port} → {BYBIT_BASE}")
    app.run(host=host, port=port, debug=False)
