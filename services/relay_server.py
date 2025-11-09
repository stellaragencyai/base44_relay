#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
services/relay_server.py — thin Bybit v5 relay using core.bybit_client

Exposed endpoints (subset your bots need):
  POST /v5/order/create                     -> by.place_order(...)
  GET  /v5/market/tickers                   -> by.get_ticker(...)
  GET  /v5/account/wallet-balance           -> by.get_wallet_balance(accountType=UNIFIED)
  GET  /v5/market/instruments-info          -> by.get_instruments_info(...)
  GET  /v5/market/orderbook                 -> by.get_orderbook(...)
  GET  /b44/equity?coin=USDT                -> helper: unified equity in USD (prefers usdValue)

Auth:
  RELAY_TOKEN required as Bearer token (Authorization: Bearer <token>)

Env:
  RELAY_HOST=0.0.0.0
  RELAY_PORT=5000
  RELAY_TOKEN=change_me
  LOG_LEVEL=INFO|DEBUG

  # Bybit creds (used by core.bybit_client under the hood)
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_ENV=mainnet|testnet
"""

from __future__ import annotations
import os, json, time
from typing import Dict, Any
from flask import Flask, request, jsonify, abort

# core stack
from core.bybit_client import Bybit
from core.logger import get_logger

HOST = os.getenv("RELAY_HOST", "0.0.0.0")
PORT = int(os.getenv("RELAY_PORT", "5000") or 5000)
TOKEN = os.getenv("RELAY_TOKEN", "")
LOG   = get_logger("services.relay")

app = Flask(__name__)
by = Bybit()

# best-effort time sync
try:
    by.sync_time()
except Exception as e:
    LOG.warning("time sync failed: %s", e)

# ---------- tiny auth + limiter ----------

# very small IP+path token bucket
_BUCKET: Dict[str, Dict[str, Any]] = {}
RATE_CAP = 30      # requests per window
RATE_WIN = 3.0     # seconds

def _rate_key() -> str:
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "local")
    return f"{ip}:{request.path}"

def _limiter_ok() -> bool:
    k = _rate_key()
    now = time.time()
    b = _BUCKET.get(k) or {"t": now, "n": 0}
    if now - b["t"] > RATE_WIN:
        b = {"t": now, "n": 1}
    else:
        b["n"] += 1
    _BUCKET[k] = b
    return b["n"] <= RATE_CAP

@app.before_request
def _auth_and_limit():
    if not TOKEN:
        abort(500, "RELAY_TOKEN not set")
    auth = (request.headers.get("Authorization") or "").split()
    if len(auth) != 2 or auth[0].lower() != "bearer" or auth[1] != TOKEN:
        abort(401)
    if not _limiter_ok():
        abort(429, "too many requests")

# ---------- helpers ----------
def _ok(data: Any) -> Any:
    return jsonify({"retCode": 0, "retMsg": "OK", "result": data})

def _bad(ret_msg: str, ret_code: int = -1) -> Any:
    return jsonify({"retCode": ret_code, "retMsg": ret_msg, "result": {}}), 400

# ---------- endpoints ----------

@app.get("/v5/market/tickers")
def market_tickers():
    category = request.args.get("category", "linear")
    symbol   = request.args.get("symbol", "")
    ok, data, err = by.get_tickers(category=category, symbol=symbol)
    if not ok:
        return _bad(err or "ticker error")
    return _ok(data.get("result", {}))

@app.get("/v5/account/wallet-balance")
def wallet_balance():
    accountType = request.args.get("accountType", "UNIFIED")
    ok, data, err = by.get_wallet_balance(accountType=accountType)
    if not ok:
        return _bad(err or "wallet error")
    return _ok(data.get("result", {}))

@app.get("/v5/market/instruments-info")
def instruments_info():
    category = request.args.get("category", "linear")
    symbol   = request.args.get("symbol")  # optional
    ok, data, err = by.get_instruments_info(category=category, symbol=symbol)
    if not ok:
        return _bad(err or "instr error")
    return _ok(data.get("result", {}))

@app.get("/v5/market/orderbook")
def orderbook():
    category = request.args.get("category", "linear")
    symbol   = request.args.get("symbol", "")
    limit    = int(request.args.get("limit", "1") or 1)
    ok, data, err = by.get_orderbook(category=category, symbol=symbol, limit=limit)
    if not ok:
        return _bad(err or "orderbook error")
    return _ok(data.get("result", {}))

@app.post("/v5/order/create")
def order_create():
    try:
        body = request.get_json(force=True, silent=False) or {}
    except Exception:
        return _bad("bad json")
    # normalize inputs
    payload = {
        "category": body.get("category", "linear"),
        "symbol":   body.get("symbol"),
        "side":     body.get("side"),
        "orderType":body.get("orderType", "Limit"),
        "qty":      body.get("qty"),
        "price":    body.get("price"),
        "timeInForce": body.get("timeInForce", "PostOnly" if body.get("price") else "IOC"),
        "reduceOnly":  bool(body.get("reduceOnly", False)),
        "orderLinkId": body.get("orderLinkId"),
        "positionIdx": body.get("positionIdx"),
        "triggerDirection": body.get("triggerDirection"),
        "triggerPrice": body.get("triggerPrice"),
        "takeProfit": body.get("takeProfit"),
        "stopLoss":   body.get("stopLoss"),
    }
    ok, data, err = by.place_order(**{k:v for k,v in payload.items() if v is not None})
    if not ok:
        return _bad(err or "place error")
    return _ok(data.get("result", {}))

# helper used by core.relay_client.equity_unified()
@app.get("/b44/equity")
def b44_equity():
    coin = (request.args.get("coin") or "USDT").upper()
    ok, data, err = by.get_wallet_balance(accountType="UNIFIED")
    if not ok:
        return jsonify({"ok": False, "error": err or "wallet error"}), 502
    wallets = (data.get("result") or {}).get("list") or []
    eq = 0.0
    if wallets:
        for c in wallets[0].get("coin", []) or []:
            usd = float(c.get("usdValue") or 0.0)
            if usd == 0 and (c.get("coin") or "").upper() in {"USDT","USDC"}:
                usd = float(c.get("walletBalance") or 0.0)
            eq += usd
    return jsonify({"ok": True, "equity": eq, "coin": coin})

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})

# ---------- main ----------
if __name__ == "__main__":
    app.run(host=HOST, port=PORT, threaded=True)
