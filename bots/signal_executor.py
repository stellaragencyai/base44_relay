#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Signal Executor (guard-aware, idempotent, maker-first)

Reads signals from signals/observed.jsonl and places entry orders via Bybit v5.
Idempotent with link_id hashing, spread checks, maker-only placement, and DB journaling.

Respects the global breaker: if guard is ON, executor skips entries but still heartbeats.

Signals expected (one per line, JSON):
{
  "ts": 1731123456789,
  "symbol": "BTCUSDT",
  "timeframe": 5,
  "signal": "LONG_BREAKOUT" | "SHORT_BREAKDOWN",
  "why": "string",
  "confidence": 0.73,
  "params": {
    "maker_only": true,
    "spread_max_bps": 8.0,
    "tag": "B44",
    "stop_dist": 85.2          # optional, executor can use for risk sizing
  },
  "features": {
    "close": 98765.43,
    "ema20": ...,
    "intra_atrp": ...,
    ...
  }
}

Env via core.config.settings (defaults shown):
  EXEC_ENABLED=true
  EXEC_POLL_SEC=2
  EXEC_HEARTBEAT_MIN=10
  EXEC_FILE=observed.jsonl
  EXEC_DIR_SIGNALS=signals

  # Sizing
  EXEC_USD_PER_TRADE=250    # if >0, derive qty = USD/price (rounded to step)
  EXEC_FIXED_QTY=0          # fallback absolute qty if budget <= 0
  EXEC_MAX_QTY=0            # optional cap; 0 means no cap

  # Risk / placement
  EXEC_SPREAD_BPS_DEFAULT=8.0
  EXEC_MAKER_ONLY_DEFAULT=true
  EXEC_SLIPPAGE_TICKS=1

  # Tagging
  EXEC_TAG=B44

Uses:
  - core.guard.guard_blocking_reason
  - core.bybit_client.Bybit
  - core.db.{insert_order,set_order_state,insert_execution}
  - tools.notifier_telegram.tg
  - core.logger
"""

from __future__ import annotations
import os, json, time, hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

from core.config import settings
from core.logger import get_logger
from core.guard import guard_blocking_reason
from core.bybit_client import Bybit
from tools.notifier_telegram import tg
from core.db import insert_order, set_order_state, insert_execution

getcontext().prec = 28
log = get_logger("bots.signal_executor")

# ---------- config ----------
ROOT = settings.ROOT
SIG_DIR = Path(getattr(settings, "DIR_SIGNALS", ROOT / "signals"))
SIG_DIR.mkdir(parents=True, exist_ok=True)
SIG_FILE = SIG_DIR / str(getattr(settings, "EXEC_FILE", "observed.jsonl"))
CURSOR_FILE = ROOT / ".state" / "executor.cursor"
CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

EXEC_ENABLED = str(getattr(settings, "EXEC_ENABLED", "true")).lower() in {"1","true","yes","on"}
EXEC_POLL_SEC = int(getattr(settings, "EXEC_POLL_SEC", 2))
EXEC_HEARTBEAT_MIN = int(getattr(settings, "EXEC_HEARTBEAT_MIN", 10))

EXEC_USD_PER_TRADE = float(getattr(settings, "EXEC_USD_PER_TRADE", 250))
EXEC_FIXED_QTY     = float(getattr(settings, "EXEC_FIXED_QTY", 0))
EXEC_MAX_QTY       = float(getattr(settings, "EXEC_MAX_QTY", 0))

EXEC_SPREAD_BPS_DEFAULT   = float(getattr(settings, "EXEC_SPREAD_BPS_DEFAULT", 8.0))
EXEC_MAKER_ONLY_DEFAULT   = str(getattr(settings, "EXEC_MAKER_ONLY_DEFAULT", "true")).lower() in {"1","true","yes","on"}
EXEC_SLIPPAGE_TICKS       = int(getattr(settings, "EXEC_SLIPPAGE_TICKS", 1))

EXEC_TAG = str(getattr(settings, "EXEC_TAG", "B44")).strip() or "B44"
BYBIT_PUBLIC = (getattr(settings, "BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/"))

# ---------- client ----------
by = Bybit()
try:
    by.sync_time()
except Exception as e:
    log.warning("time sync failed: %s", e)

# ---------- utils ----------
def _dec(x: Any) -> Decimal:
    return Decimal(str(x))

def _round_to_step(x: Decimal, step: Decimal) -> Decimal:
    steps = (x / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step

def _round_to_tick(x: Decimal, tick: Decimal) -> Decimal:
    steps = (x / tick).to_integral_value(rounding=ROUND_DOWN)
    return steps * tick

def _order_link_id(payload: Dict[str, Any]) -> str:
    """
    Deterministic id for idempotency across restarts. Includes tag+symbol+dir+price bucket.
    """
    j = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    h = hashlib.blake2s(j.encode("utf-8"), digest_size=10).hexdigest()
    base = payload.get("params", {}).get("tag") or EXEC_TAG
    return f"{base}-exe-{h}"[:36]

# ---------- market info ----------
def get_instruments_info(symbol: str) -> Dict:
    ok, data, err = by.get_instruments_info(category="linear", symbol=symbol)
    if not ok:
        raise RuntimeError(f"instruments info error: {err}")
    lst = (data.get("result") or {}).get("list") or []
    return lst[0] if lst else {}

def get_symbol_filters(symbol: str) -> Tuple[Decimal, Decimal, Decimal]:
    info = get_instruments_info(symbol)
    tick = _dec(info["priceFilter"]["tickSize"])
    step = _dec(info["lotSizeFilter"]["qtyStep"])
    minq = _dec(info["lotSizeFilter"]["minOrderQty"])
    return tick, step, minq

def get_orderbook_top(symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
    ok, data, err = by.get_orderbook(category="linear", symbol=symbol, limit=1)
    if not ok:
        return None
    res = data.get("result") or {}
    bids = res.get("b") or res.get("bids") or []
    asks = res.get("a") or res.get("asks") or []
    if not bids or not asks:
        return None
    return _dec(bids[0][0]), _dec(asks[0][0])

def last_price(symbol: str) -> Optional[Decimal]:
    ok, data, err = by.get_tickers(category="linear", symbol=symbol)
    if not ok:
        return None
    lst = (data.get("result") or {}).get("list") or []
    if not lst:
        return None
    return _dec(lst[0]["lastPrice"])

# ---------- spread / price ----------
def check_spread(symbol: str, max_bps: float, tick: Decimal) -> Tuple[bool, Optional[Decimal]]:
    ob = get_orderbook_top(symbol)
    if not ob:
        return False, None
    bid, ask = ob
    mid = (bid + ask) / Decimal(2)
    spread_bps = float(((ask - bid) / mid) * Decimal(10000)) if mid > 0 else 1e9
    return spread_bps <= max_bps, mid

def entry_price(symbol: str, direction: str, tick: Decimal, slippage_ticks: int) -> Optional[Decimal]:
    """
    Use top-of-book with small, favorable shading so PostOnly rests.
    """
    ob = get_orderbook_top(symbol)
    if not ob:
        return None
    bid, ask = ob
    if direction == "long":
        # buy limit near bid, shade down a bit
        px = bid - tick * slippage_ticks
    else:
        # sell limit near ask, shade up a bit
        px = ask + tick * slippage_ticks
    return _round_to_tick(px, tick)

# ---------- sizing ----------
def compute_qty(symbol: str, px: Decimal, step: Decimal, minq: Decimal, payload: Dict[str, Any]) -> Decimal:
    # priority: EXEC_USD_PER_TRADE, else EXEC_FIXED_QTY
    usd = Decimal(str(payload.get("params", {}).get("usd_per_trade", EXEC_USD_PER_TRADE)))
    fixed = Decimal(str(payload.get("params", {}).get("fixed_qty", EXEC_FIXED_QTY)))
    cap = Decimal(str(EXEC_MAX_QTY))

    if usd and usd > 0 and px > 0:
        q = usd / px
    elif fixed and fixed > 0:
        q = fixed
    else:
        q = Decimal("0")

    q = _round_to_step(q, step)
    if cap > 0:
        q = min(q, cap)
    if q < minq:
        q = Decimal("0")
    return q

# ---------- db helpers ----------
def db_record_new(link_id: str, symbol: str, side: str, qty: Decimal, price: Decimal, tag: str):
    insert_order(link_id, symbol, side, float(qty), float(price), tag, state="NEW")

def db_state(link_id: str, state: str, **kw):
    set_order_state(link_id, state, **kw)

def db_exec(link_id: str, qty: Decimal, price: Decimal, fee: Decimal = Decimal("0"), ts_ms: Optional[int] = None):
    insert_execution(link_id, float(qty), float(price), fee=float(fee), ts_ms=ts_ms)

# ---------- execution ----------
def place_entry(payload: Dict[str, Any]) -> None:
    symbol = payload["symbol"].upper()
    direction = "long" if str(payload.get("signal","")).upper().startswith("LONG") else "short"
    params = payload.get("params", {}) or {}
    max_bps = float(params.get("spread_max_bps", EXEC_SPREAD_BPS_DEFAULT))
    maker_only = bool(params.get("maker_only", EXEC_MAKER_ONLY_DEFAULT))
    tag = (params.get("tag") or EXEC_TAG)[:12]

    # guard/breaker
    blocked, why = guard_blocking_reason()
    if blocked:
        log.info("guard ON, skip entry %s: %s", symbol, why)
        return

    tick, step, minq = get_symbol_filters(symbol)

    ok_spread, mid = check_spread(symbol, max_bps, tick)
    if not ok_spread:
        log.info("spread too wide %s > %.2f bps — skipping %s", symbol, max_bps, symbol)
        return

    # derive price and qty
    px = entry_price(symbol, direction, tick, EXEC_SLIPPAGE_TICKS)
    if px is None:
        lp = last_price(symbol) or Decimal("0")
        px = _round_to_tick(lp, tick)
    qty = compute_qty(symbol, px, step, minq, payload)
    if qty <= 0:
        log.info("qty too small for %s (min=%s) — skipping", symbol, minq)
        return

    side = "Buy" if direction == "long" else "Sell"
    link_payload = {
        "s": symbol,
        "d": direction,
        "px": str(px),
        "q": str(qty),
        "tag": tag,
        "t": int(payload.get("ts", int(time.time()*1000))),
    }
    link_id = _order_link_id(link_payload)

    db_record_new(link_id, symbol, side, qty, px, tag)

    tif = "PostOnly" if maker_only else "GoodTillCancel"
    try:
        ok, data, err = by.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Limit",
            qty=str(qty.normalize()),
            price=str(px.normalize()),
            timeInForce=tif,
            reduceOnly=False,
            orderLinkId=link_id,
        )
    except Exception as e:
        ok, data, err = False, {}, str(e)

    if not ok:
        db_state(link_id, "REJECTED", err_code="API", err_msg=str(err))
        tg.safe_text(f"❌ Entry REJECTED {symbol} {side} {qty} @ {px} • {err}", quiet=True)
        return

    exid = (data.get("result") or {}).get("orderId") or ""
    db_state(link_id, "OPEN", exchange_id=exid)
    tg.safe_text(f"🟢 Entry PLACED {symbol} {side} {qty} @ {px} • {link_id}", quiet=True)

# ---------- tailing ----------
@dataclass
class Cursor:
    path: Path
    pos: int = 0

    def load(self):
        try:
            if self.path.exists():
                self.pos = int(self.path.read_text().strip() or "0")
        except Exception:
            self.pos = 0

    def save(self):
        try:
            self.path.write_text(str(self.pos))
        except Exception:
            pass

def tail_jsonl(file: Path, cur: Cursor) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        with open(file, "r", encoding="utf-8") as fh:
            fh.seek(cur.pos)
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
            cur.pos = fh.tell()
    except FileNotFoundError:
        cur.pos = 0
    return out

# ---------- order status refresh (lightweight) ----------
def poll_fills_for_recent(link_ids: List[str]) -> None:
    if not link_ids:
        return
    # Bybit doesn't have a direct batch lookup by link_id for fills; we can poll open orders + executions.
    # Keep it light: check each link_id open status and pull recent executions.
    for lid in link_ids[-20:]:
        try:
            ok, data, err = by.get_open_orders(category="linear", orderLinkId=lid, openOnly=1)
            if ok:
                lst = (data.get("result") or {}).get("list") or []
                if not lst:
                    # not open; set to FILLED or PARTIALLY_FILLED based on execution lookup
                    eok, edata, eerr = by.get_executions(category="linear", orderLinkId=lid, limit=50)
                    if eok:
                        exs = (edata.get("result") or {}).get("list") or []
                        filled_qty = sum(_dec(x.get("execQty","0")) for x in exs)
                        if filled_qty > 0:
                            for ex in exs:
                                try:
                                    db_exec(lid, _dec(ex.get("execQty","0")), _dec(ex.get("execPrice","0")), _dec(ex.get("execFee","0")), int(ex.get("execTime","0")))
                                except Exception:
                                    pass
                            db_state(lid, "FILLED")
                        else:
                            db_state(lid, "CLOSED")
                    else:
                        db_state(lid, "CLOSED")
                else:
                    # still open; update state
                    db_state(lid, "OPEN", exchange_id=lst[0].get("orderId"))
        except Exception as e:
            log.debug("poll fill err %s: %s", lid, e)

# ---------- main loop ----------
def main():
    if not EXEC_ENABLED:
        tg.safe_text("Executor disabled (EXEC_ENABLED=false).", quiet=True)
        log.info("Executor disabled by config.")
        return

    tg.safe_text("🟢 Signal Executor online", quiet=True)

    cur = Cursor(CURSOR_FILE)
    cur.load()

    last_hb = 0.0
    recent_links: List[str] = []

    while True:
        try:
            # heartbeat
            now = time.time()
            if EXEC_HEARTBEAT_MIN > 0 and (now - last_hb) >= EXEC_HEARTBEAT_MIN * 60:
                blocked, why = guard_blocking_reason()
                tg.safe_text(f"💓 Executor heartbeat • guard={'ON' if blocked else 'OFF'}{(' • '+why) if blocked else ''}", quiet=True)
                last_hb = now

            # consume signals
            items = tail_jsonl(SIG_FILE, cur)
            if items:
                cur.save()
            for p in items:
                try:
                    place_entry(p)
                    # track for fill polling
                    link = _order_link_id({
                        "s": p["symbol"].upper(),
                        "d": ("long" if str(p.get("signal","")).upper().startswith("LONG") else "short"),
                        "px": str(p.get("features",{}).get("close") or ""),
                        "q": str(p.get("params",{}).get("fixed_qty") or ""),
                        "tag": (p.get("params",{}).get("tag") or EXEC_TAG)[:12],
                        "t": int(p.get("ts", int(time.time()*1000))),
                    })
                    recent_links.append(link)
                except Exception as e:
                    log.warning("place_entry error: %s payload=%s", e, p)
            # light fill polling
            poll_fills_for_recent(recent_links)

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("executor loop error: %s", e)

        time.sleep(max(1, EXEC_POLL_SEC))

if __name__ == "__main__":
    main()
