#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Relay — Hardened + Normalization + Diagnostics (Flask)

- Auth: Bearer or x-relay-token (also ?token= for quick curls)
- Generic proxy:        POST /bybit/proxy      (Bybit v5 only; signed)  → adds body.normalized
- Native helpers:       GET  /bybit/wallet/balance                        → adds normalized
                        GET  /bybit/positions
                        GET  /bybit/tickers
                        GET  /bybit/subuids
- Base44 helpers:       GET  /getAccountData   ← ALWAYS returns an ARRAY of accounts
                         Each item includes: equity, walletBalance, unrealisedPnl, availableBalance
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
                        GET  /diag/wallet-normalized  ← proves availableBalance is present
- Status UI:            GET  /status           (aggregate JSON, auth required)
                        GET  /ui/status        (tiny HTML dashboard; fetches /status with your token)
"""

from __future__ import annotations

import os
import time
import hmac
import json
import csv
import math
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
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
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
# Normalization for wallet-balance (adds availableBalance)
# ──────────────────────────────────────────────────────────────────────────────
def _to_f(x, default=0.0) -> float:
    try:
        if x is None or x == "" or (isinstance(x, str) and x.lower() == "null"):
            return float(default)
        return float(x)
    except Exception:
        return float(default)

def normalize_wallet_balance(bybit_body: dict, request_params: dict | None = None) -> dict | None:
    """
    Normalize Bybit wallet-balance body to always include availableBalance per coin,
    plus totals per account. Safe for UNIFIED with optional memberId/subUid passthrough.
    """
    try:
        if not isinstance(bybit_body, dict):
            return None
        if int(bybit_body.get("retCode", -1)) != 0:
            return None

        result = bybit_body.get("result", {}) or {}
        accounts_list = result.get("list", []) or []
        req_member = (request_params or {}).get("memberId") or (request_params or {}).get("subUid")

        normalized_accounts = []
        for account in accounts_list:
            account_type = account.get("accountType", "UNIFIED")
            total_equity = _to_f(account.get("totalEquity"))

            coins_out = []
            for c in (account.get("coin", []) or []):
                symbol          = c.get("coin", "UNKNOWN")
                equity          = _to_f(c.get("equity"))
                wallet_balance  = _to_f(c.get("walletBalance"))
                order_margin    = _to_f(c.get("totalOrderIM"))
                position_margin = _to_f(c.get("totalPositionIM"))
                maint_margin    = _to_f(c.get("totalPositionMM"))
                unreal          = _to_f(c.get("unrealisedPnl"))
                realised        = _to_f(c.get("cumRealisedPnl"))
                avail_wd        = c.get("availableToWithdraw")
                avail_wd        = (_to_f(avail_wd) if avail_wd is not None else None)

                # availableBalance = max(WB - OrderIM - PositionIM, 0). If WB missing, derive from equity - unreal.
                if wallet_balance > 0:
                    available_balance = wallet_balance - order_margin - position_margin
                elif equity > 0:
                    computed_wb = equity - unreal
                    available_balance = computed_wb - order_margin - position_margin
                else:
                    available_balance = 0.0

                if not math.isfinite(available_balance):
                    available_balance = 0.0
                available_balance = max(available_balance, 0.0)

                coins_out.append({
                    "coin": symbol,
                    "equity": equity,
                    "walletBalance": wallet_balance,
                    "orderMargin": order_margin,
                    "positionMargin": position_margin,
                    "maintMargin": maint_margin,
                    "unrealisedPnl": unreal,
                    "realisedPnl": realised,
                    "availableToWithdraw": avail_wd,
                    "availableBalance": available_balance
                })

            totals = {
                "availableBalance": float(sum(x["availableBalance"] for x in coins_out)),
                "walletBalance": float(sum(x["walletBalance"]   for x in coins_out)),
                "orderMargin": float(sum(x["orderMargin"]       for x in coins_out)),
                "positionMargin": float(sum(x["positionMargin"] for x in coins_out)),
            }

            acc = {
                "accountType": account_type,
                "totalEquity": total_equity,
                "coins": coins_out,
                "totals": totals
            }
            if req_member:
                acc["memberId"] = str(req_member)
            normalized_accounts.append(acc)

        return {"accounts": normalized_accounts}
    except Exception as e:
        log.error(f"[normalize_wallet_balance] {e}")
        return None

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
# Status helpers (for /status and UI)
# ──────────────────────────────────────────────────────────────────────────────
def _read_json_file(p: Path, default):
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _tail_lines(path: Path, n: int = 50) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 2048
            data = b""
            while size > 0 and data.count(b"\n") <= n:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
            return [ln.decode("utf-8", "replace").strip() for ln in data.splitlines()[-n:]]
    except Exception:
        try:
            return path.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]
        except Exception:
            return []

def _calc_gross_exposure_linear() -> Tuple[float, dict]:
    status, body = _http_call("GET", "/v5/position/list", {"category": "linear"}, None)
    gross = 0.0
    detail: Dict[str, float] = {}
    if status == 200 and isinstance(body, dict):
        for p in (body.get("result") or {}).get("list") or []:
            try:
                sym = str(p.get("symbol") or "")
                sz = float(p.get("size") or 0)
                px = float(p.get("avgPrice") or 0)
                g = abs(sz * px)
                gross += g
                if sym:
                    detail[sym] = detail.get(sym, 0.0) + g
            except Exception:
                continue
    return gross, detail

def _total_equity_unified() -> float:
    status, body = _http_call("GET", "/v5/account/wallet-balance", {"accountType":"UNIFIED"}, None)
    total = 0.0
    if status == 200 and isinstance(body, dict):
        for acct in (body.get("result") or {}).get("list") or []:
            try:
                total += float(acct.get("totalEquity") or 0)
            except Exception:
                pass
    return total

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

@app.get("/diag/wallet-normalized")
@require_auth
def diag_wallet_normalized():
    """Signed sanity check that availableBalance exists in normalized output."""
    params = {"accountType": "UNIFIED"}
    status, body = _http_call("GET", "/v5/account/wallet-balance", params, None)
    if status != 200 or not isinstance(body, dict):
        return _json_err(502, "bybit_error", status=status, body=body)
    norm = normalize_wallet_balance(body, params) or {}
    ok = False
    sample = []
    try:
        accs = norm.get("accounts") or []
        if accs:
            sample = (accs[0].get("coins") or [])[:2]
            ok = all("availableBalance" in c for c in sample)
    except Exception:
        ok = False
    return _json_ok(has_availableBalance=ok, sample=sample, retCode=body.get("retCode"))

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

    # Attach normalization when asking wallet-balance
    try:
        if path.endswith("/v5/account/wallet-balance"):
            primary = prox.get("primary", {}) or {}
            body = primary.get("body", {}) or {}
            params = payload.get("params") or {}
            normalized = normalize_wallet_balance(body, params)
            if isinstance(body, dict):
                body["normalized"] = normalized
                prox["primary"]["body"] = body
    except Exception as e:
        log.error(f"[proxy] normalization error: {e}")

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
    status, body = _http_call("GET", "/v5/account/wallet-balance", params, None)
    if not isinstance(body, dict):
        return make_response(body or "", status)
    body["normalized"] = normalize_wallet_balance(body, params)
    return make_response(jsonify(body), status)

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
    Each item: { uid, label, accountType, equity, walletBalance, unrealisedPnl, availableBalance }
    Includes main, plus any registry/ sub_uids.csv if present.
    """
    coin = request.args.get("coin", "USDT")
    account_type = request.args.get("accountType", "UNIFIED")

    accounts: List[Dict[str, Any]] = []

    def _pull_account(member_id: str | None, label: str):
        params = {"accountType": account_type, "coin": coin}
        if member_id:
            params["memberId"] = member_id
        status, body = _http_call("GET", "/v5/account/wallet-balance", params, None)
        if status != 200 or not isinstance(body, dict):
            return
        norm = normalize_wallet_balance(body, params) or {}
        # derive totals safely
        total_equity = 0.0
        wallet_balance = 0.0
        unreal = 0.0
        avail = 0.0
        try:
            # prefer normalized totals if present
            accs = (norm.get("accounts") or [])
            if accs:
                totals = accs[0].get("totals") or {}
                avail = float(totals.get("availableBalance") or 0.0)
                wallet_balance = float(totals.get("walletBalance") or 0.0)
                total_equity = float(accs[0].get("totalEquity") or 0.0)
            else:
                # fall back to raw shape
                lst = ((body.get("result") or {}).get("list") or [])
                if lst:
                    a0 = lst[0]
                    total_equity = float(a0.get("totalEquity") or 0.0)
                    # Bybit doesn't give account-level walletBalance explicitly; compute from coins if available
                    coins = a0.get("coin") or []
                    wallet_balance = float(sum(float(c.get("walletBalance") or 0.0) for c in coins))
                    unreal = float(sum(float(c.get("unrealisedPnl") or 0.0) for c in coins))
                    # compute available from coins if missing normalization
                    order_im = float(sum(float(c.get("totalOrderIM") or 0.0) for c in coins))
                    pos_im   = float(sum(float(c.get("totalPositionIM") or 0.0) for c in coins))
                    avail = max(wallet_balance - order_im - pos_im, 0.0)
        except Exception:
            pass

        accounts.append({
            "uid": member_id or "main",
            "label": label,
            "accountType": account_type,
            "equity": total_equity,
            "walletBalance": wallet_balance,
            "unrealisedPnl": unreal,
            "availableBalance": avail
        })

    # main
    _pull_account(None, "main")
    # subs (optional)
    for uid in _load_sub_uids():
        _pull_account(uid, _pretty_name(uid))

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
    status, body = _http_call("GET", "/v5/account/wallet-balance", params, None)
    if not isinstance(body, dict):
        return make_response(body or "", status)
    body["normalized"] = normalize_wallet_balance(body, params)
    return make_response(jsonify(body), status)

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

# ---- Aggregated status JSON (auth required) ----
@app.get("/status")
@require_auth
def status_aggregate():
    """
    Aggregated, low-latency system status for the UI.
    Includes: equity, gross exposure, breaker hint (file), recent signals.
    """
    # breaker hint from file fallback (same file executor checks)
    risk_state = _read_json_file(ROOT / ".state" / "risk_state.json", {})
    breaker_on = bool(risk_state.get("breach") or risk_state.get("breaker") or risk_state.get("active"))

    equity = _total_equity_unified()
    gross, gross_by = _calc_gross_exposure_linear()

    # last signals
    sig_path = (ROOT / "signals" / "observed.jsonl")
    sig_lines = _tail_lines(sig_path, 25)
    recent_signals = []
    for ln in sig_lines:
        try:
            js = json.loads(ln)
            recent_signals.append({
                "ts": int(js.get("ts", 0)),
                "symbol": str(js.get("symbol","")).upper(),
                "signal": str(js.get("signal","") or js.get("dir","")).upper(),
                "features_class": (js.get("features") or {}).get("class")
            })
        except Exception:
            continue

    return _json_ok(
        env=BYBIT_ENV,
        bybit_base=BYBIT_BASE,
        breaker=breaker_on,
        equity=round(equity, 2),
        gross_exposure=round(gross, 2),
        gross_by_symbol={k: round(v,2) for k,v in gross_by.items()},
        signals=recent_signals[-15:]
    )

# ---- Minimal HTML dashboard (token via ?token=) ----
@app.get("/ui/status")
def ui_status():
    """
    Minimal dashboard. Pass token via ?token=... or it will prompt.
    """
    tok = request.args.get("token","")
    html = f"""
<!doctype html>
<meta charset="utf-8">
<title>Base44 Status</title>
<style>
  body {{ font: 14px/1.4 system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 20px; }}
  .row {{ display:flex; gap:24px; flex-wrap:wrap; }}
  .card {{ border:1px solid #ddd; border-radius:10px; padding:14px; min-width:280px; box-shadow:0 1px 3px rgba(0,0,0,.05); }}
  h1 {{ margin:0 0 12px 0; font-size:20px; }}
  table {{ border-collapse: collapse; width:100%; }}
  td, th {{ padding:4px 8px; border-bottom:1px solid #eee; text-align:left; }}
  code {{ background:#f6f8fa; padding:2px 4px; border-radius:4px; }}
</style>
<div class="row">
  <div class="card"><h1>Overview</h1>
    <div id="ov"></div>
  </div>
  <div class="card"><h1>Gross by Symbol</h1>
    <table id="gross"></table>
  </div>
  <div class="card" style="flex:1 1 420px;"><h1>Recent Signals</h1>
    <table id="sig"></table>
  </div>
</div>
<script>
const tokenParam = new URLSearchParams(location.search).get('token') || '{tok}';
async function fetchStatus() {{
  const hdrs = tokenParam ? {{'x-relay-token': tokenParam}} : {{}};
  if(!tokenParam) {{
    const t = prompt('Relay token? (stored in this tab only)');
    if(!t) return;
    location.search = '?token='+encodeURIComponent(t);
    return;
  }}
  const r = await fetch('/status', {{headers: hdrs}});
  const js = await r.json().catch(()=>({{ok:false}}));
  if(!js.ok) {{
    document.body.innerHTML = '<p>Auth failed. Add ?token=YOUR_TOKEN to the URL.</p>';
    return;
  }}
  const pct = js.equity>0 ? (js.gross_exposure/js.equity*100).toFixed(1) : '0.0';
  document.getElementById('ov').innerHTML =
    `<div>Env: <b>${{js.env}}</b> → <code>${{js.bybit_base}}</code></div>
     <div>Breaker: <b style="color:${{js.breaker?'#c00':'#090'}}">${{js.breaker?'ON':'OFF'}}</b></div>
     <div>Equity: <b>${{js.equity.toFixed(2)}}</b></div>
     <div>Gross: <b>${{js.gross_exposure.toFixed(2)}}</b> (${{pct}}% of equity)</div>`;
  const gross = js.gross_by_symbol || {{}};
  const gtbl = Object.entries(gross).sort((a,b)=>b[1]-a[1]).map(([k,v])=>`<tr><td>${{k}}</td><td style="text-align:right">${{v.toFixed(2)}}</td></tr>`).join('');
  document.getElementById('gross').innerHTML = '<tr><th>Symbol</th><th>Gross</th></tr>'+gtbl;
  const sigs = (js.signals||[]).slice().reverse().map(s=>`<tr><td>${{new Date(s.ts||0).toLocaleTimeString()}}</td><td>${{s.symbol}}</td><td>${{s.signal}}</td><td>${{s.features_class||""}}</td></tr>`).join('');
  document.getElementById('sig').innerHTML = '<tr><th>Time</th><th>Symbol</th><th>Dir</th><th>Class</th></tr>'+sigs;
}}
fetchStatus(); setInterval(fetchStatus, 6000);
</script>
"""
    return make_response(html, 200, {"Content-Type": "text/html; charset=utf-8"})

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    host = os.getenv("RELAY_HOST", "0.0.0.0")
    try:
        port = int(os.getenv("RELAY_PORT", "5000"))
    except ValueError:
        port = 5000
    log.info(f"Starting Base44 Relay on http://{host}:{port} → {BYBIT_BASE}")
    log.info(f"Loaded from: {os.path.abspath(__file__)}")
    app.run(host=host, port=port, debug=False)
