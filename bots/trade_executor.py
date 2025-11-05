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

import os, json, time, logging, pathlib, math
from dotenv import load_dotenv
from core import relay_client
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
    payload = {
        "path": "/v5/order/create",
        "method": "POST",
        "body": {
            "category": "linear",
            "symbol": symbol,
            "side": side.upper(),
            "orderType": "Limit" if price else "Market",
            "qty": str(qty),
            **({"price": f"{price:.10f}".rstrip("0").rstrip(".")} if price else {}),
            "timeInForce": "PostOnly" if price else "IOC",
            "orderLinkId": f"{order_tag}-{int(time.time()*1000)}"
        }
    }
    if not LIVE:
        log.info(f"[DRY] /bybit/proxy {json.dumps(payload,separators=(',',':'))}")
        return {"ok": True, "dry": True}
    return relay_client.proxy("POST", "/v5/order/create", body=payload["body"])

def bps(a, b):
    # absolute basis points difference between two prices
    if a is None or b is None:
        return 999999
    try:
        return abs((a - b) / ((a + b) / 2.0)) * 10000
    except ZeroDivisionError:
        return 999999

def run():
    if not QUEUE.exists():
        log.info(f"signal queue not found: {QUEUE}")
        return

    # Gather unique symbols from signals file
    syms = set()
    sigs = []
    for line in QUEUE.read_text(encoding="utf-8").splitlines():
        try:
            s = json.loads(line)
            sigs.append(s)
            if s.get("symbol"): syms.add(s["symbol"])
        except Exception:
            continue

    if not sigs:
        log.info("no signals to process")
        return

    # load instruments for those symbols
    instr = load_or_fetch(sorted(list(syms)))

    equity = relay_client.equity_unified()
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
            continue  # keep it simple for now

        meta = instr.get(sym)
        if not meta:
            log.warning(f"{sym}: no instrument meta; skipping")
            continue
        tick = meta["tickSize"]; step = meta["lotStep"]; minq = meta["minQty"]

        # fetch current book via tickers
        tk = relay_client.ticker(sym)
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

        # toy stop distance assumption: 0.5% to get a qty; replace with real stop math later
        stop_dist = max(0.005, 0.003)
        notional = equity * risk_pct
        # qty in contract terms (approx: notional / last)
        raw_qty = notional / max(last, 1e-9)
        qty = round_qty(raw_qty, step, minq)
        if qty <= 0:
            log.info(f"{sym}: qty {raw_qty:.8f} -> {qty} after rounding; skip")
            continue

        # choose a maker price just inside the book
        if maker_only:
            price = round_price(bid * (1 - 1e-4), tick)  # nudge 1bp below bid for longs is wrong; for LONG we should rest on bid or slightly below? For a maker BUY, place at bid or lower to avoid taker.
            price = max(price, 0.0)
        else:
            price = None

        res = place_entry(sym, "Buy", qty, price=price if maker_only else None, order_tag=tag)
        log.info(f"sub={sub} sym={sym} type={typ} maker={maker_only} qty={qty} bid={bid} ask={ask} spr={spr_bps:.1f}bps res={(str(res)[:160])}")
        processed += 1

    log.info(f"done processed={processed}, LIVE={LIVE}")

if __name__ == "__main__":
    log.info(f"executor LIVE={LIVE} relay={os.getenv('EXECUTOR_RELAY_BASE') or os.getenv('RELAY_BASE') or 'http://127.0.0.1:5000'}")
    run()
