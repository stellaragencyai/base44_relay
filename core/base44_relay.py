#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Relay — Finalized
- Secure Bearer-auth gateway for Bybit v5 API
- Single proxy endpoint: POST /bybit/proxy
- Master helper: bybit_proxy_internal(payload) used by routes
- Sub-UIDs endpoint: GET /bybit/subuids  (requires master key)
- Legacy shims:
    GET /v1/wallet/balance   -> /v5/account/wallet-balance
    GET /v1/order/realtime   -> /v5/order/realtime
    GET /v1/position/list    -> /v5/position/list
- Health + Telegram heartbeat
"""

import os
import time
import hmac
import json
import hashlib
import logging
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlencode

import requests
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Config / Env
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

RELAY_TOKEN   = os.getenv("RELAY_TOKEN", "").strip()
RELAY_SECRET  = os.getenv("RELAY_SECRET", "").strip()  # reserved for future internal HMAC
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]

BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
BYBIT_BASE       = (os.getenv("BYBIT_BASE") or "https://api.bybit.com").strip().rstrip("/")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "").strip()

RECV_WINDOW = os.getenv("BYBIT_RECV_WINDOW", "5000").strip()
TIMEOUT_S   = float(os.getenv("RELAY_TIMEOUT", "25"))

if not RELAY_TOKEN:
    raise RuntimeError("RELAY_TOKEN missing in .env")

app = Flask(__name__)
CORS(app, origins=ALLOWED_ORIGINS or ["*"])

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("base44_relay")


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def _json_ok(**payload):
    resp = jsonify({"ok": True, **payload})
    return resp

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

def require_bearer(func):
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        _ = request.headers.get("ngrok-skip-browser-warning")  # ignored; just bypasses ngrok page
        if not auth.lower().startswith("bearer "):
            return _json_err(403, "forbidden")
        token = auth.split(" ", 1)[1].strip()
        if token != RELAY_TOKEN:
            return _json_err(403, "forbidden")
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    wrapper.__doc__  = func.__doc__
    return wrapper


# ──────────────────────────────────────────────────────────────────────────────
# Bybit signing / request helpers (v5)
# ──────────────────────────────────────────────────────────────────────────────
def _bybit_headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": sign,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }

def _sign_payload(method: str, params: Optional[dict], body: Optional[dict]) -> Tuple[str, str, str]:
    """
    Returns (timestamp, payload_str, sign_hex)
    For GET: payload is querystring e.g. "a=1&b=2"
    For POST: payload is compact JSON string e.g. '{"a":1,"b":2}'
    Prehash: ts + api_key + recv_window + payload
    """
    ts = str(int(time.time() * 1000))

    if method == "GET":
        query = urlencode(params or {}, doseq=True)
        payload_str = query
    else:
        payload_str = json.dumps(body or {}, separators=(",", ":"))

    prehash = f"{ts}{BYBIT_API_KEY}{RECV_WINDOW}{payload_str}"
    sign = hmac.new(BYBIT_API_SECRET.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
    return ts, payload_str, sign

def _http_call(method: str, path: str, params: Optional[dict], body: Optional[dict]) -> Tuple[int, Any]:
    """
    Executes a single HTTP call to Bybit with proper signing.
    Returns (status_code, parsed_json_or_text)
    """
    method = method.upper()
    ts, payload_str, sign = _sign_payload(method, params, body)
    headers = _bybit_headers(ts, sign)
    url = f"{BYBIT_BASE}{path}"

    try:
        if method == "GET":
            r = requests.get(url, params=params or {}, headers=headers, timeout=TIMEOUT_S)
        else:
            r = requests.post(url, json=body or {}, headers=headers, timeout=TIMEOUT_S)

        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text
    except requests.RequestException as e:
        return 599, {"error": "request_exception", "detail": str(e)}

def bybit_proxy_internal(payload: dict) -> Dict[str, Any]:
    """
    Generic proxy helper other routes can reuse.
    Expected payload:
      {"method":"GET|POST", "path":"/v5/...", "params":{...}}  # for GET
      {"method":"POST", "path":"/v5/...", "body":{...}}        # for POST
    Returns a structured object with 'primary' and 'fallback' (same call twice),
    and a top-level 'error' if Bybit retCode != 0 (when JSON).
    """
    method = (payload.get("method") or "GET").upper()
    path   = payload.get("path") or ""
    params = payload.get("params")
    body   = payload.get("body")

    # primary attempt
    status_p, body_p = _http_call(method, path, params, body)

    # fallback attempt (simple re-try; keeps the shape Nolan saw earlier)
    status_f, body_f = _http_call(method, path, params, body)

    # normalize top-level error when JSON contains retCode != 0
    top_error = None
    try:
        ret = (body_p or {}).get("retCode")
        if ret not in (0, None):
            top_error = "bybit_error"
    except AttributeError:
        pass

    result = {
        "primary":  {"status": status_p, "body": body_p},
        "fallback": {"status": status_f, "body": body_f},
    }
    if top_error:
        result["error"] = top_error
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return _json_ok(
        service="base44_relay",
        bybit_base=BYBIT_BASE,
        api_key_present=bool(BYBIT_API_KEY),
        origins=ALLOWED_ORIGINS or ["*"]
    )

@app.post("/heartbeat")
@require_bearer
def heartbeat():
    data = request.get_json(silent=True) or {}
    note = data.get("note", "heartbeat")
    msg = f"💓 Base44 Relay heartbeat — {note}"
    tg_send(msg)
    return _json_ok(message="sent")

@app.post("/bybit/proxy")
@require_bearer
def bybit_proxy():
    payload = request.get_json(silent=True) or {}
    if "path" not in payload:
        return _json_err(400, "missing 'path'")

    # Force v5 paths only for safety
    path = payload.get("path", "")
    if not path.startswith("/v5/"):
        return _json_err(400, "only /v5/* paths are allowed")

    out = bybit_proxy_internal(payload)
    return jsonify(out)

@app.get("/bybit/subuids")
@require_bearer
def bybit_subuids():
    """
    Returns list of sub account UIDs (requires MASTER key).
    """
    payload = {"method": "GET", "path": "/v5/user/query-sub-members", "params": {}}
    prox = bybit_proxy_internal(payload)

    uids = []
    body = prox.get("primary", {}).get("body", {}) or {}
    try:
        lst = body.get("result", {}).get("list", []) or []
        for item in lst:
            uid = item.get("uid") or item.get("memberId") or item.get("subMemberId")
            if uid:
                uids.append(str(uid))
    except Exception:
        pass

    if not uids and prox.get("error"):
        return jsonify({"ok": False, "error": "bybit_error", **prox})

    return jsonify({"ok": True, "source": "user/query-sub-members", "sub_uids": uids})

# ── Legacy compatibility shims — quiet old /v1/* callers ──────────────────────
@app.get("/v1/wallet/balance")
@require_bearer
def legacy_wallet_balance():
    payload = {
        "method": "GET",
        "path": "/v5/account/wallet-balance",
        "params": {"accountType": "UNIFIED"}
    }
    prox = bybit_proxy_internal(payload)
    return jsonify(prox)

@app.get("/v1/order/realtime")
@require_bearer
def legacy_order_realtime():
    # Map typical old realtime call to v5
    # Accept ?symbol=... (optional), defaults to category=linear
    symbol = request.args.get("symbol")
    params = {"category": "linear"}
    if symbol:
        params["symbol"] = symbol
    payload = {
        "method": "GET",
        "path": "/v5/order/realtime",
        "params": params
    }
    prox = bybit_proxy_internal(payload)
    return jsonify(prox)

@app.get("/v1/position/list")
@require_bearer
def legacy_position_list():
    # Accept ?symbol=... (optional); defaults to category=linear
    symbol = request.args.get("symbol")
    params = {"category": "linear"}
    if symbol:
        params["symbol"] = symbol
    payload = {
        "method": "GET",
        "path": "/v5/position/list",
        "params": params
    }
    prox = bybit_proxy_internal(payload)
    return jsonify(prox)

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = os.getenv("RELAY_HOST", "127.0.0.1")
    port = int(os.getenv("RELAY_PORT", "8080"))
    app.run(host=host, port=port, debug=False)
