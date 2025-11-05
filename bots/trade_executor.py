#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_executor.py — DRY executor that consumes normalized signals and
prints the exact relay calls it would make (no orders sent until LIVE=true).

ENV:
  EXECUTOR_RELAY_BASE=http://127.0.0.1:5000
  EXECUTOR_RELAY_TOKEN=...
  LIVE=false
  LOG_LEVEL=INFO|DEBUG
  SIGNAL_DIR=signals
"""

import os, json, time, logging, pathlib
import requests
from dotenv import load_dotenv

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("trade_executor")

BASE = os.getenv("EXECUTOR_RELAY_BASE", "http://127.0.0.1:5000").rstrip("/")
TOK  = (os.getenv("EXECUTOR_RELAY_TOKEN") or os.getenv("RELAY_TOKEN") or "").strip()
LIVE = (os.getenv("LIVE") or "false").lower() == "true"
SIGDIR = pathlib.Path(__file__).resolve().parents[1] / (os.getenv("SIGNAL_DIR") or "signals")
QUEUE = SIGDIR / "observed.jsonl"

if not TOK:
    raise SystemExit("Missing EXECUTOR_RELAY_TOKEN / RELAY_TOKEN in env")

HDRS = {"Authorization": f"Bearer {TOK}"}

def get_equity():
    try:
        r = requests.get(f"{BASE}/bybit/wallet/balance", headers=HDRS, params={"accountType":"UNIFIED"}, timeout=10)
        j = r.json()
        total = 0.0
        for acct in (j.get("result",{}) or {}).get("list",[]) or []:
            total += float(acct.get("totalEquity",0))
        return total
    except Exception as e:
        log.warning(f"equity fetch failed: {e}")
        return 0.0

def place_entry(symbol, side, qty, price=None, order_tag="B44"):
    payload = {
        "path": "/v5/order/create",
        "method": "POST",
        "body": {
            "category": "linear",
            "symbol": symbol,
            "side": side.upper(),
            "orderType": "Limit" if price else "Market",
            "qty": str(qty),
            **({"price": str(price)} if price else {}),
            "timeInForce": "PostOnly" if price else "IOC",
            "orderLinkId": f"{order_tag}-{int(time.time()*1000)}"
        }
    }
    if not LIVE:
        log.info(f"[DRY] would POST /bybit/proxy {json.dumps(payload,separators=(',',':'))}")
        return {"ok": True, "dry": True}
    r = requests.post(f"{BASE}/bybit/proxy", headers=HDRS, json=payload, timeout=15)
    return r.json()

def main():
    log.info(f"executor LIVE={LIVE} relay={BASE}")
    if not QUEUE.exists():
        log.info(f"signal queue not found: {QUEUE}")
        return
    equity = get_equity()
    for line in QUEUE.read_text(encoding="utf-8").splitlines():
        try:
            sig = json.loads(line)
        except Exception:
            continue
        sym = sig.get("symbol"); sub = sig.get("sub"); typ = sig.get("signal")
        if not sym or not typ: 
            continue
        # toy sizing: 0.01% equity per signal, you will replace with proper calc
        risk_pct = float(sig.get("params",{}).get("risk_per_trade_pct", 0.0001))
        notional = equity * risk_pct
        # naive qty placeholder — replace with instrument info later
        qty = max(0.001, round(notional / 10, 3))
        side = "Buy" if typ.upper().startswith("LONG") else "Sell"
        res = place_entry(sym, side, qty, price=None, order_tag="B44")
        log.info(f"signal→order sub={sub} sym={sym} type={typ} qty={qty} res={str(res)[:160]}")
    log.info("executor done")

if __name__ == "__main__":
    main()
