#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_executor.py — sizes with instrument rules, checks spread, places DRY orders by default.

ENV:
  EXECUTOR_RELAY_BASE=http://127.0.0.1:5000  # or https ngrok URL
  EXECUTOR_RELAY_TOKEN=...
  LIVE=false
  LOG_LEVEL=INFO|DEBUG
  SIGNAL_DIR=signals
"""

import os, json, time, logging, pathlib
from dotenv import load_dotenv

# Use module-level functions, not a class
import core.relay_client as rc
from core.instruments import load_or_fetch, round_price, round_qty

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("trade_executor")

LIVE = (os.getenv("LIVE") or "false").lower() == "true"
ROOT = pathlib.Path(__file__).resolve().parents[1]
SIGDIR = ROOT / (os.getenv("SIGNAL_DIR") or "signals")
QUEUE = SIGDIR / "observed.jsonl"

def place_entry(symbol, side, qty, price=None, order_tag="B44"):
    body = {
        "category": "linear",
        "symbol": symbol,
        "side": side.upper(),
        "orderType": "Limit" if price else "Market",
        "qty": str(qty),
        **({"price": f"{price:.10f}".rstrip("0").rstrip(".")} if price else {}),
        "timeInForce": "PostOnly" if price else "IOC",
        "orderLinkId": f"{order_tag}-{int(time.time()*1000)}"
    }
    payload = {"method": "POST", "path": "/v5/order/create", "body": body}

    if not LIVE:
        log.info(f"[DRY] /bybit/proxy {json.dumps(payload, separators=(',',':'))}")
        return {"ok": True, "dry": True}

    # Signed request via relay
    return rc.proxy("POST", "/v5/order/create", body=body)

def bps(a, b):
    if not a or not b:
        return 999999
    try:
        return abs((a - b) / ((a + b) / 2.0)) * 10000
    except ZeroDivisionError:
        return 999999

def run():
    if not QUEUE.exists():
        log.info(f"signal queue not found: {QUEUE}")
        return

    # Gather unique symbols from signals
    syms, sigs = set(), []
    for line in QUEUE.read_text(encoding="utf-8").splitlines():
        try:
            s = json.loads(line)
        except Exception:
            continue
        sigs.append(s)
        if s.get("symbol"):
            syms.add(s["symbol"])

    if not sigs:
        log.info("no signals to process")
        return

    # Load instrument metadata for rounding/legality
    instr = load_or_fetch(sorted(list(syms)))

    equity = rc.equity_unified()
    if equity <= 0:
        log.warning("equity=0 or fetch failed; using 10 USDT as dummy base")
        equity = 10.0

    processed = 0
    for s in sigs:
        sym = s.get("symbol")
        typ = str(s.get("signal","")).upper()
        sub = s.get("sub")
        params = s.get("params") or {}
        maker_only = bool(params.get("maker_only", True))
        spread_max_bps = float(params.get("spread_max_bps", 8))
        risk_pct = float(params.get("risk_per_trade_pct", 0.0001))
        tag = "B44"

        if typ not in ("LONG_TEST","LONG_BREAKOUT","LONG"):
            continue

        meta = instr.get(sym)
        if not meta:
            log.warning(f"{sym}: no instrument meta; skipping")
            continue
        tick = meta["tickSize"]; step = meta["lotStep"]; minq = meta["minQty"]

        # Live quote from relay
        tk = rc.ticker(sym)
        if not tk:
            log.warning(f"{sym}: no ticker")
            continue
        try:
            bid = float(tk.get("bid1Price") or 0)
            ask = float(tk.get("ask1Price") or 0)
            last = float(tk.get("lastPrice") or 0)
        except Exception:
            log.warning(f"{sym}: bad ticker fields")
            continue

        spr_bps = bps(ask, bid)
        if maker_only and spr_bps > spread_max_bps:
            log.info(f"{sym}: spread {spr_bps:.1f}bps > {spread_max_bps}bps; skip")
            continue

        # Toy stop distance placeholder; real logic will replace this
        stop_dist = 0.005
        notional = equity * risk_pct
        raw_qty = notional / max(last, 1e-9)
        qty = round_qty(raw_qty, step, minq)
        if qty <= 0:
            log.info(f"{sym}: qty {raw_qty:.8f} -> {qty} after rounding; skip")
            continue

        # Maker BUY: rest at bid or a hair below to avoid taker
        price = round_price(bid, tick) if maker_only else None

        res = place_entry(sym, "Buy", qty, price=price if maker_only else None, order_tag=tag)
        log.info(f"sub={sub} sym={sym} type={typ} maker={maker_only} qty={qty} bid={bid} ask={ask} spr={spr_bps:.1f}bps res={(str(res)[:200])}")
        processed += 1

    log.info(f"done processed={processed}, LIVE={LIVE}")

if __name__ == "__main__":
    base = os.getenv('EXECUTOR_RELAY_BASE') or os.getenv('RELAY_BASE') or 'http://127.0.0.1:5000'
    log.info(f"executor LIVE={LIVE} relay={base}")
    run()
