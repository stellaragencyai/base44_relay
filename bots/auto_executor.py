#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Auto Executor (consume signals/observed.jsonl, maker-first entries)

What this does now
- Tails signals/observed.jsonl lines emitted by bots/signal_engine.py.
- For each new signal, enforces breaker, dedupes, validates spread, and places a maker-first limit.
- DRY_RUN safe path prints and notifies but does not touch the exchange.
- No TP/SL logic here; your TP/SL Manager owns exits. This only opens.

Core dependencies
- core.config.settings          → envs, paths, defaults
- core.logger                   → unified logging
- core.bybit_client.Bybit       → Bybit v5 client (signed)
- tools.notifier_telegram.tg    → Telegram notifications

Key settings (put in .env if you want to override defaults)
  # From core.config / signal engine
  SIG_MAKER_ONLY=true
  SIG_SPREAD_MAX_BPS=8
  SIG_TAG=B44
  SIG_DRY_RUN=true

  # Executor-specific
  EXEC_QTY_USDT=5.0            # notional in quote; used if EXEC_QTY_BASE==0
  EXEC_QTY_BASE=0.0            # set >0 to override notional sizing
  EXEC_POST_ONLY=true          # force PostOnly on maker entries
  EXEC_SYMBOLS=                # optional allowlist, e.g. BTCUSDT,ETHUSDT
  EXEC_POLL_SEC=2              # how often to check the queue when idle

Files
  signals/observed.jsonl       # input queue (append-only)
  .state/executor_seen.json    # dedupe registry for orderLinkId
  .state/executor_offset.json  # last read byte offset for resilient tailing
  .state/risk_state.json       # breaker flag file ({"breach": true}) blocks opens
"""

from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from tools.notifier_telegram import tg

log = get_logger("bots.auto_executor")
log = bind_context(log, comp="executor")

# ------------------------
# Config
# ------------------------

ROOT = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_DIR = settings.DIR_SIGNALS
QUEUE_PATH = SIGNALS_DIR / "observed.jsonl"

# Inherit common knobs from signal-engine defaults
MAKER_ONLY = bool(getattr(settings, "SIG_MAKER_ONLY", True))
SPREAD_MAX_BPS = float(getattr(settings, "SIG_SPREAD_MAX_BPS", 8.0))
TAG = str(getattr(settings, "SIG_TAG", "B44")).strip() or "B44"

DRY_RUN = bool(getattr(settings, "SIG_DRY_RUN", True))

# Executor-specific knobs
def _get_env_num(name: str, default: float) -> float:
    try:
        v = getattr(settings, name)
    except AttributeError:
        return default
    try:
        return float(v)
    except Exception:
        return default

EXEC_QTY_USDT = _get_env_num("EXEC_QTY_USDT", 5.0)    # notional in quote (e.g., USDT)
EXEC_QTY_BASE = _get_env_num("EXEC_QTY_BASE", 0.0)    # if >0, overrides notional sizing
EXEC_POST_ONLY = str(getattr(settings, "EXEC_POST_ONLY", "true")).strip().lower() in ("1","true","yes","on")
EXEC_POLL_SEC = int(getattr(settings, "EXEC_POLL_SEC", 2))

# Optional symbol allowlist
_allow_raw = str(getattr(settings, "EXEC_SYMBOLS", "") or "").strip()
EXEC_SYMBOLS: Optional[List[str]] = [s.strip().upper() for s in _allow_raw.split(",") if s.strip()] or None

# persistent registries
SEEN_FILE = STATE_DIR / "executor_seen.json"      # orderLinkId registry
OFFSET_FILE = STATE_DIR / "executor_offset.json"  # queue offset, for resilience

# Bybit client
by = Bybit()
by.sync_time()  # best-effort

# ------------------------
# Helpers
# ------------------------

def breaker_active() -> bool:
    path = STATE_DIR / "risk_state.json"
    try:
        if not path.exists():
            return False
        js = json.loads(path.read_text(encoding="utf-8"))
        return bool(js.get("breach") or js.get("breaker") or js.get("active"))
    except Exception:
        return False

def _load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("failed reading %s: %s", path.name, e)
    return default

def _save_json(path: Path, obj) -> None:
    try:
        path.write_text(json.dumps(obj, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.error("failed writing %s: %s", path.name, e)

def _mk_link_id(symbol: str, ts_ms: int, signal: str, extra: str = "") -> str:
    # Keep <= 36 chars for Bybit; compact but deterministic
    base = f"{TAG}-{symbol}-{int(ts_ms/1000)}-{signal}"
    if extra:
        base = f"{base}-{extra}"
    return base[:36]

def _fetch_best_prices(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    ok, data, err = by.get_tickers(category="linear", symbol=symbol)
    if not ok:
        log.warning("ticker fail %s: %s", symbol, err)
        return None, None, None
    try:
        items = (data.get("result") or {}).get("list") or []
        if not items:
            return None, None, None
        item = items[0]
        bid = float(item.get("bid1Price"))
        ask = float(item.get("ask1Price"))
        if bid <= 0 or ask <= 0:
            return None, None, None
        mid = (bid + ask) / 2.0
        return bid, ask, mid
    except Exception as e:
        log.warning("ticker parse fail %s: %s", symbol, e)
        return None, None, None

def _spread_bps(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 1e9
    return (ask - bid) / mid * 10000.0

def _qty_from_notional(price: float) -> str:
    if EXEC_QTY_BASE > 0:
        qty = EXEC_QTY_BASE
    else:
        notional = max(0.0, EXEC_QTY_USDT)
        qty = (notional / max(price, 1e-9))
    return f"{qty:.10f}".rstrip("0").rstrip(".") or "0"

def _direction_to_side(direction: str) -> str:
    return "Buy" if direction.lower().startswith("long") else "Sell"

# ------------------------
# Queue tailing
# ------------------------

def _read_offset() -> int:
    obj = _load_json(OFFSET_FILE, {"pos": 0})
    try:
        return int(obj.get("pos", 0))
    except Exception:
        return 0

def _write_offset(pos: int) -> None:
    _save_json(OFFSET_FILE, {"pos": int(pos)})

def _load_seen() -> Dict[str, int]:
    return _load_json(SEEN_FILE, {})

def _save_seen(seen: Dict[str, int]) -> None:
    _save_json(SEEN_FILE, seen)

def _tail_queue(path: Path, start_pos: int) -> Tuple[int, list]:
    """
    Read new lines from 'path' starting at byte offset start_pos.
    Returns (new_pos, lines[])
    """
    if not path.exists():
        return start_pos, []

    new_pos = start_pos
    lines: list = []
    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(start_pos, 0)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            lines.append(line)
        new_pos = fh.tell()
    return new_pos, lines

# ------------------------
# Core execution
# ------------------------

def _place_entry(symbol: str, side: str, link_id: str, params: Dict, price_hint: Optional[float]) -> Tuple[bool, str]:
    """
    Place a maker-first limit at the edge of the book, enforcing spread ceiling.
    """
    bid, ask, mid = _fetch_best_prices(symbol)
    if bid is None or ask is None:
        return False, "no orderbook"

    spr_bps = _spread_bps(bid, ask)
    max_bps = float(params.get("spread_max_bps", SPREAD_MAX_BPS))
    if spr_bps > max_bps:
        return False, f"spread {spr_bps:.2f} bps > max {max_bps}"

    # maker edge: longs near bid; shorts near ask
    px = bid if side == "Buy" else ask
    qty = _qty_from_notional(px)
    tif = "PostOnly" if (EXEC_POST_ONLY and MAKER_ONLY) else "GoodTillCancel"

    if DRY_RUN:
        log.info("DRY entry %s %s qty=%s px=%s spr=%.2fbps tif=%s link=%s", symbol, side, qty, px, spr_bps, tif, link_id)
        tg.safe_text(f"🟡 DRY • {symbol} • {side} qty≈{qty} @ {px} • spr {spr_bps:.2f}bps • tif={tif}", quiet=True)
        return True, "dry-run"

    ok, data, err = by.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Limit",
        qty=qty,
        price=f"{px}",
        timeInForce=tif,
        reduceOnly=False,
        orderLinkId=link_id,
        tpslMode=None,  # exits handled elsewhere
    )
    if not ok:
        return False, err or "place_order failed"
    return True, "ok"

# ------------------------
# Main loop
# ------------------------

def main() -> None:
    # Announce
    tg.safe_text(f"🟢 Executor online • maker={MAKER_ONLY} • postOnly={EXEC_POST_ONLY} • dry={DRY_RUN} • queue={QUEUE_PATH.name}", quiet=True)
    log.info("Executor online • maker=%s postOnly=%s dry=%s queue=%s", MAKER_ONLY, EXEC_POST_ONLY, DRY_RUN, QUEUE_PATH)

    seen = _load_seen()
    pos = _read_offset()

    while True:
        try:
            # Tail queue
            new_pos, lines = _tail_queue(QUEUE_PATH, pos)
            if not lines:
                time.sleep(max(1, EXEC_POLL_SEC))
                continue

            for raw in lines:
                try:
                    obj = json.loads(raw)
                except Exception:
                    log.warning("bad jsonl line (skip): %s", raw[:200])
                    continue

                ts_ms = int(obj.get("ts", 0))
                symbol = str(obj.get("symbol", "")).upper()
                tf = obj.get("timeframe")
                signal = str(obj.get("signal", ""))
                params = dict(obj.get("params") or {})
                features = dict(obj.get("features") or {})

                # Optional symbol allowlist
                if EXEC_SYMBOLS and symbol not in EXEC_SYMBOLS:
                    log.debug("skip %s (not in EXEC_SYMBOLS)", symbol)
                    continue

                # Build idempotent link id
                link_id = _mk_link_id(symbol, ts_ms, "LONG" if "LONG" in signal else "SHORT")

                # Dedup
                if link_id in seen:
                    log.debug("dup %s (already seen)", link_id)
                    continue

                # Breaker gate
                if breaker_active():
                    log.info("breaker ON: suppress open for %s (%s)", symbol, signal)
                    tg.safe_text(f"⛔ Breaker ON • skip open • {symbol} {signal}", quiet=True)
                    continue

                # Direction to side
                side = _direction_to_side("long" if "LONG" in signal.upper() else "short")

                # Attempt placement
                ok, msg = _place_entry(symbol, side, link_id, params, price_hint=None)
                seen[link_id] = int(time.time())
                _save_seen(seen)

                if ok:
                    tg.safe_text(f"✅ ENTRY • {symbol} • {side} • link={link_id}", quiet=True)
                    log.info("entry ok %s %s link=%s", symbol, side, link_id)
                else:
                    tg.safe_text(f"⚠️ ENTRY FAIL • {symbol} • {side} • {msg}", quiet=True)
                    log.warning("entry fail %s %s: %s", symbol, side, msg)

            # Commit offset after processing batch
            pos = new_pos
            _write_offset(pos)

        except Exception as e:
            log.error("executor loop error: %s", e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
