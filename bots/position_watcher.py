#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
positions_watcher.py — Bybit v5 private WebSocket (positions + wallet)

Reads:
  BYBIT_ENV=mainnet|testnet
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_RECV_WINDOW=10000 (optional)
  WS_RECONNECT_BACKOFF=2,4,8,16,32 (optional)
  LOG_LEVEL=INFO|DEBUG (optional)

Emits compact JSON logs for position and wallet deltas.
"""

import os, json, time, hmac, hashlib, logging
from dotenv import load_dotenv
from websocket import WebSocketApp

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("positions_watcher")

ENV   = (os.getenv("BYBIT_ENV") or "mainnet").strip().lower()
KEY   = (os.getenv("BYBIT_API_KEY") or "").strip()
SECRET= (os.getenv("BYBIT_API_SECRET") or "").strip()
RECVW = (os.getenv("BYBIT_RECV_WINDOW") or "10000").strip()
BACKOFF = [int(x) for x in (os.getenv("WS_RECONNECT_BACKOFF") or "2,4,8,16,32").split(",")]

if not KEY or not SECRET:
    raise SystemExit("Missing BYBIT_API_KEY / BYBIT_API_SECRET in .env")

WS_PRIVATE = "wss://stream.bybit.com/v5/private" if ENV == "mainnet" else "wss://stream-testnet.bybit.com/v5/private"

def sign_auth(ts_ms: int) -> str:
    payload = str(ts_ms) + KEY + RECVW
    return hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _on_open(ws: WebSocketApp):
    log.info(f"WS open → {WS_PRIVATE}")
    ts = int(time.time()*1000)
    signature = sign_auth(ts)
    auth = {"op":"auth","args":[KEY, str(ts), signature, RECVW]}
    ws.send(json.dumps(auth))
    log.info("auth sent")

def _on_message(ws: WebSocketApp, message: str):
    try:
        data = json.loads(message)
    except Exception:
        log.debug(f"non-json: {message[:120]}")
        return

    # auth acks
    if "op" in data or data.get("success") is True:
        log.debug(f"ctrl: {data}")
        # after successful auth, subscribe
        if data.get("op") == "auth" and data.get("success"):
            sub = {"op":"subscribe", "args":["position","wallet","order"]}  # order is often useful
            ws.send(json.dumps(sub))
            log.info("subscribed: position, wallet, order")
        return

    topic = data.get("topic")
    if topic == "position":
        for item in data.get("data", []):
            out = {
                "t": data.get("ts", int(time.time()*1000)),
                "topic":"position",
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "size": item.get("size"),
                "entry": item.get("avgPrice"),
                "mark": item.get("markPrice"),
                "liq": item.get("liqPrice"),
                "unPnl": item.get("unrealisedPnl"),
            }
            print(json.dumps(out, separators=(",",":")))
    elif topic == "wallet":
        for item in data.get("data", []):
            out = {
                "t": data.get("ts", int(time.time()*1000)),
                "topic":"wallet",
                "accountType": item.get("accountType"),
                "totalEquity": item.get("totalEquity"),
                "availableBalance": item.get("availableBalance"),
            }
            print(json.dumps(out, separators=(",",":")))
    elif topic == "order":
        for item in data.get("data", []):
            out = {
                "t": data.get("ts", int(time.time()*1000)),
                "topic":"order",
                "symbol": item.get("symbol"),
                "orderId": item.get("orderId"),
                "status": item.get("orderStatus"),
                "side": item.get("side"),
                "price": item.get("price"),
                "qty": item.get("qty"),
                "linkId": item.get("orderLinkId"),
                "reduceOnly": item.get("reduceOnly"),
            }
            print(json.dumps(out, separators=(",",":")))
    else:
        log.debug(f"other: {data}")

def _on_error(ws: WebSocketApp, err):
    log.warning(f"WS error: {err}")

def _on_close(ws: WebSocketApp, code, msg):
    log.warning(f"WS close code={code} msg={msg}")

def run_forever():
    attempt = 0
    while True:
        try:
            ws = WebSocketApp(
                WS_PRIVATE,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error(f"fatal ws exception: {e}")
        delay = BACKOFF[min(attempt, len(BACKOFF)-1)]
        log.info(f"reconnect in {delay}s")
        time.sleep(delay)
        attempt += 1

if __name__ == "__main__":
    log.info(f"positions_watcher starting → {WS_PRIVATE}")
    run_forever()
