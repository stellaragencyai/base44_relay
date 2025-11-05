#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tickers_feed.py — Bybit v5 public WebSocket (linear) with reconnect/backoff.

Reads:
  BYBIT_ENV=mainnet|testnet
  TP_SYMBOL_WHITELIST=comma,separated,symbols
  WS_RECONNECT_BACKOFF=2,4,8,16,32   (optional)
  LOG_LEVEL=INFO|DEBUG               (optional)

Outputs compact JSON log lines of tick updates.
"""

import os, json, time, threading, logging
from typing import List
from dotenv import load_dotenv
from websocket import WebSocketApp

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tickers_feed")

ENV = (os.getenv("BYBIT_ENV") or "mainnet").strip().lower()
SYMS = [s.strip().upper() for s in (os.getenv("TP_SYMBOL_WHITELIST") or "").split(",") if s.strip()]
BACKOFF = [int(x) for x in (os.getenv("WS_RECONNECT_BACKOFF") or "2,4,8,16,32").split(",")]

WS_PUBLIC = "wss://stream.bybit.com/v5/public/linear" if ENV == "mainnet" else "wss://stream-testnet.bybit.com/v5/public/linear"

if not SYMS:
    log.warning("TP_SYMBOL_WHITELIST empty; subscribing to nothing. Set it in .env.")
TOPICS = [f"tickers.{s}" for s in SYMS]

def _on_open(ws: WebSocketApp):
    log.info(f"WS open → {WS_PUBLIC}")
    if TOPICS:
        sub = {"op": "subscribe", "args": TOPICS}
        ws.send(json.dumps(sub))
        log.info(f"subscribed: {TOPICS}")

def _on_message(ws: WebSocketApp, message: str):
    try:
        data = json.loads(message)
    except Exception:
        log.debug(f"non-json message: {message[:100]}")
        return
    # heartbeat/operation acks
    if "op" in data or "success" in data or data.get("type") in ("snapshot","delta"):
        log.debug(f"ctrl: {data}")
        return
    topic = data.get("topic")
    ts = data.get("ts")
    if topic and "tickers." in topic:
        for item in data.get("data", []):
            out = {
                "t": ts or int(time.time()*1000),
                "topic": topic,
                "symbol": item.get("symbol"),
                "bid1": item.get("bid1Price"),
                "ask1": item.get("ask1Price"),
                "last": item.get("lastPrice"),
                "mark": item.get("markPrice"),
                "index": item.get("indexPrice"),
                "volume24h": item.get("turnover24h"),
            }
            print(json.dumps(out, separators=(",",":")))

def _on_error(ws: WebSocketApp, err):
    log.warning(f"WS error: {err}")

def _on_close(ws: WebSocketApp, code, msg):
    log.warning(f"WS close code={code} msg={msg}")

def run_forever():
    attempt = 0
    while True:
        try:
            ws = WebSocketApp(
                WS_PUBLIC,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error(f"fatal ws exception: {e}")
        # backoff
        delay = BACKOFF[min(attempt, len(BACKOFF)-1)]
        log.info(f"reconnect in {delay}s")
        time.sleep(delay)
        attempt += 1

if __name__ == "__main__":
    log.info(f"tickers_feed starting → {WS_PUBLIC} symbols={SYMS}")
    run_forever()
