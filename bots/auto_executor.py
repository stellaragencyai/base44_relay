#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Auto Executor (consume signals/observed.jsonl, maker-first entries)
FINALIZED / unified with core stack (v2025-11-07)

What this does
- Tails signals/observed.jsonl emitted by bots/signal_engine.py.
- For each new signal:
  • Checks global breaker (core.breaker.is_active / .state/risk_state.json fallback).
  • Optional allow-list by symbol (EXEC_SYMBOLS).
  • Idempotent de-dupe via orderLinkId registry in .state/executor_seen.json.
  • Validates orderbook spread vs params.spread_max_bps or SIG_SPREAD_MAX_BPS.
  • Computes qty from either EXEC_QTY_BASE, or notional (EXEC_QTY_USDT/px), or
    risk-based if params.stop_dist is provided (uses core.portfolio_guard.guard.current_risk_value()).
  • Places maker-first PostOnly limit at best bid/ask when maker_only=true,
    or uses Market when maker_only=false (respecting EXEC_POST_ONLY).
- DRY_RUN path prints & notifies only; no exchange calls.
- Exits (TP/SL) are handled by TP/SL Manager; executor only opens.

New in this revision
- DB persistence: orders and (dry-run) executions recorded for restart safety.
- Explicit order state transitions: NEW → SENT → ACKED (FILLED simulated in dry-run).
- Robust error states: REJECTED / ERROR with retCode/retMsg captured.
- Fixed env typo (SIG_TANGLED → SIG_TAG fallback).
"""

from __future__ import annotations
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.notifier_bot import tg_send
try:
    from core.portfolio_guard import guard
except Exception:
    guard = None  # graceful degrade if not present
try:
    from core.breaker import is_active as breaker_is_active  # preferred
except Exception:
    breaker_is_active = None

# Optional structured decision log (soft dep)
try:
    from core.decision_log import log_event
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass

# DB hooks (optional; fall back cleanly if Core/ vs core/ casing differs or DB not present)
try:
    from Core.db import insert_order, set_order_state, insert_execution  # pragma: no cover
except Exception:
    try:
        from core.db import insert_order, set_order_state, insert_execution  # pragma: no cover
    except Exception:
        insert_order = set_order_state = insert_execution = None  # type: ignore

log = bind_context(get_logger("bots.auto_executor"), comp="executor")

# ------------------------
# Config
# ------------------------

ROOT: Path = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_DIR: Path = getattr(settings, "DIR_SIGNALS", ROOT / "signals")
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_PATH = SIGNALS_DIR / (getattr(settings, "SIGNAL_QUEUE_FILE", "observed.jsonl"))

# Env helpers
def _get_env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        try:
            return bool(getattr(settings, name))
        except Exception:
            return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _get_env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        try:
            return float(getattr(settings, name))
        except Exception:
            return default
    try:
        return float(v)
    except Exception:
        return default

def _get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        try:
            return int(getattr(settings, name))
        except Exception:
            return default
    try:
        return int(v)
    except Exception:
        return default

# Maker/Tag/Spread from signal-engine defaults
MAKER_ONLY      = _get_env_bool("SIG_MAKER_ONLY", True)
SPREAD_MAX_BPS  = _get_env_float("SIG_SPREAD_MAX_BPS", 8.0)
TAG             = (getattr(settings, "SIG_TAG", None) or os.getenv("SIG_TAG") or "B44").strip() or "B44"
SIG_DRY_DEFAULT = _get_env_bool("SIG_DRY_RUN", True)

# Executor-specific
EXEC_DRY_RUN         = _get_env_bool("EXEC_REALLY_DRY_RUN", SIG_DRY_DEFAULT)  # keeps old var if present
if os.getenv("EXEC_DRY_RUN") is not None:  # prefer explicit EXEC_DRY_RUN when set
    EXEC_DRY_RUN = _get_env_bool("EXEC_DRY_RUN", SIG_DRY_DEFAULT)

EXEC_QTY_USDT        = _get_env_float("EXEC_QTY_USDT", 5.0)
EXEC_QTY_BASE        = _get_env_float("EXEC_QTY_BASE", 0.0)
EXEC_POST_ONLY       = _get_env_bool("EXEC_POST_ONLY", True)
EXEC_POLL_SEC        = _get_env_int("EXEC_POLL_SEC", 2)
EXEC_MAX_SIGNAL_AGE  = _get_env_int("EXEC_MAX_SIGNAL_AGE_SEC", 120)
EXEC_ACCOUNT_UID     = (os.getenv("EXEC_ACCOUNT_UID") or "").strip() or None

# Optional symbol allowlist
_raw_allow = (os.getenv("EXEC_SYMBOL_LIST") or getattr(settings, "EXEC_SYMBOLS", "") or "").strip()
EXEC_SYMBOLS: Optional[List[str]] = [s.strip().upper() for s in _raw_allow.split(",") if s.strip()] or None

# persistent registries
SEEN_FILE   = STATE_DIR / "executor_seen.json"      # orderLinkId registry
OFFSET_FILE = STATE_DIR / "executor_offset.json"    # queue offset, for resilience

# Bybit client
by = Bybit()
try:
    by.sync_time()  # best-effort
except Exception as e:
    log.warning("time sync failed: %s", e)

# ------------------------
# Helpers
# ------------------------

def _fallback_breaker_active() -> bool:
    """File-based breaker fallback used if core.breaker.is_active is unavailable."""
    path = STATE_DIR / "risk_state.json"
    try:
        if not path.exists():
            return False
        js = json.loads(path.read_text(encoding="utf-8"))
        return bool(js.get("breach") or js.get("breaker") or js.get("active"))
    except Exception:
        return False

def breaker_active() -> bool:
    if callable(breaker_is_active):
        try:
            return bool(breaker_is_active())
        except Exception:
            return _fallback_breaker_active()
    return _fallback_breaker_active()

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

def _mk_link_id(symbol: str, ts_ms: int, signal: str, tag: str, extra: str = "") -> str:
    # Keep <= 36 chars for Bybit; compact but deterministic
    base = f"{tag}-{symbol}-{int(ts_ms/1000)}-{signal}"
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

def _qty_from_signal(price: float, params: Dict) -> float:
    """
    Sizing precedence:
      1) EXEC_QTY_BASE (>0) → fixed base qty
      2) If params.stop_dist present AND guard available → risk_value / stop_dist
      3) Fallback to notional: EXEC_QTY_USDT / price
    Returns a numeric float; formatting to string happens at request time.
    """
    if EXEC_QTY_BASE > 0:
        qty = EXEC_QTY_BASE
    elif params.get("stop_dist") and hasattr(guard, "current_risk_value"):
        try:
            risk_val = float(guard.current_risk_value())
            px_delta = float(params["stop_dist"])
            if px_delta > 0:
                qty = max(0.0, risk_val / max(px_delta, 1e-9))
            else:
                qty = max(0.0, EXEC_QTY_USDT / max(price, 1e-9))
        except Exception:
            qty = max(0.0, EXEC_QTY_USDT / max(price, 1e-9))
    else:
        qty = max(0.0, EXEC_QTY_USDT / max(price, 1e-9))
    return qty

def _format_qty(qty: float) -> str:
    txt = f"{qty:.10f}".rstrip("0").rstrip(".")
    return txt or "0"

def _direction_to_side(direction: str) -> str:
    return "Buy" if direction.lower().startswith("long") else "Sell"

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

def _tail_queue(path: Path, start_pos: int) -> Tuple[int, List[str]]:
    """
    Read new lines from 'path' starting at byte offset start_pos.
    Handles truncation (e.g., log rotated) by resetting to 0 if needed.
    """
    if not path.exists():
        return start_pos, []
    size = path.stat().st_size
    pos = start_pos if 0 <= start_pos <= size else 0

    new_pos = pos
    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(pos, 0)
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

def _record_order_state(link_id: str, symbol: str, side: str, qty_val: float, px: Optional[float],
                        state: str, exchange_id: Optional[str] = None,
                        err_code: Optional[str] = None, err_msg: Optional[str] = None) -> None:
    """
    Write order state to DB if DB module is available; otherwise no-op.
    """
    if insert_order is None or set_order_state is None:
        return
    try:
        if state == "NEW":
            insert_order(link_id, symbol, side, qty_val, px, TAG, state="NEW")
        else:
            set_order_state(link_id, state, exchange_id=exchange_id, err_code=err_code, err_msg=err_msg)
    except Exception as e:
        log.warning("DB write failed (%s %s): %s", link_id, state, e)

def _record_execution(link_id: str, qty_val: float, px: float, fee: float = 0.0) -> None:
    if insert_execution is None:
        return
    try:
        insert_execution(link_id, qty_val, px, fee=fee)
    except Exception as e:
        log.warning("DB exec write failed (%s): %s", link_id, e)

def _place_entry(symbol: str, side: str, link_id: str, params: Dict, price_hint: Optional[float]) -> Tuple[bool, str]:
    """
    Place a maker-first limit at the edge of the book (PostOnly) when maker_only, or Market otherwise.
    Enforces spread ceiling (bps). Respects DRY mode.
    Also records order lifecycle states to DB (NEW → SENT → ACKED; FILLED simulated in dry-run).
    """
    bid, ask, mid = _fetch_best_prices(symbol)
    if bid is None or ask is None:
        return False, "no orderbook"

    spr_bps = _spread_bps(bid, ask)
    max_bps = float(params.get("spread_max_bps", SPREAD_MAX_BPS))
    if spr_bps > max_bps:
        return False, f"spread {spr_bps:.2f} bps > max {max_bps}"

    maker_only = bool(params.get("maker_only", MAKER_ONLY))
    # Choose price: if hint provided and maker_only, nudge to edge that will post
    if maker_only:
        px = bid if side == "Buy" else ask
        if isinstance(price_hint, (int, float)) and price_hint > 0:
            if side == "Buy":
                px = min(px, float(price_hint))
            else:
                px = max(px, float(price_hint))
    else:
        px = None  # we’ll use Market

    qty_val = _qty_from_signal(price=px or mid, params=params)
    qty_txt = _format_qty(qty_val)
    tif = "PostOnly" if (EXEC_POST_ONLY and maker_only and px is not None) else ("ImmediateOrCancel" if px is None else "GoodTillCancel")

    # DB: NEW
    _record_order_state(link_id, symbol, side, qty_val, px, state="NEW")

    if EXEC_DRY_RUN:
        msg = f"🟡 DRY • {symbol} • {side} qty≈{qty_txt} @ {px if px else 'MKT'} • spr {spr_bps:.2f}bps • tif={tif} • link={link_id}"
        tg_send(msg, priority="info")
        log_event("executor", "entry_dry", symbol, "MAIN", {"side": side, "qty": qty_txt, "px": px, "spr_bps": spr_bps, "tif": tif, "link": link_id})
        # Simulate filled in DB for analytics continuity
        _record_order_state(link_id, symbol, side, qty_val, px, state="FILLED", exchange_id="DRY-RUN")
        _record_execution(link_id, qty_val, float(px or mid), fee=0.0)
        return True, "dry-run"

    # Build request
    req = dict(
        category="linear",
        symbol=symbol,
        side=side,  # Buy|Sell
        orderType=("Limit" if px is not None else "Market"),
        qty=str(qty_txt),
        reduceOnly=False,
        timeInForce=("PostOnly" if px is not None and EXEC_POST_ONLY and maker_only else ("IOC" if px is None else "GoodTillCancel")),
        orderLinkId=link_id,
        tpslMode=None,  # exits handled by TP/SL manager
    )
    if px is not None:
        req["price"] = f"{px}"

    # Attempt placement
    ok, data, err = by.place_order(**req)
    if not ok:
        # DB: REJECTED/ERROR
        _record_order_state(link_id, symbol, side, qty_val, px, state="REJECTED", err_code="bybit_err", err_msg=str(err or "place_order failed"))
        return False, (err or "place_order failed")

    # Parse exchange response
    try:
        result = (data.get("result") or {})
        exch_id = result.get("orderId") or result.get("order_id")
    except Exception:
        exch_id = None

    # DB: SENT → ACKED (fills handled by reconciler)
    _record_order_state(link_id, symbol, side, qty_val, px, state="SENT", exchange_id=exch_id)
    _record_order_state(link_id, symbol, side, qty_val, px, state="ACKED", exchange_id=exch_id)

    return True, "ok"

# ------------------------
# Main loop
# ------------------------

def main() -> None:
    tg_send(f"🟢 Executor online • maker={MAKER_ONLY} • postOnly={EXEC_POST_ONLY} • dry={EXEC_DRY_RUN} • queue={QUEUE_PATH.name}", priority="success")
    log.info("online • maker=%s postOnly=%s dry=%s queue=%s", MAKER_ONLY, EXEC_POST_ONLY, EXEC_DRY_RUN, QUEUE_PATH)

    seen = _load_seen()
    pos = _read_offset()

    while True:
        try:
            new_pos, lines = _tail_queue(QUEUE_PATH, pos)
            if not lines:
                time.sleep(max(1, EXEC_POLL_SEC))
                # heal truncation
                if new_pos < pos:
                    pos = new_pos
                    _write_offset(pos)
                continue

            for raw in lines:
                # parse
                try:
                    obj = json.loads(raw)
                except Exception:
                    log.warning("bad jsonl line (skip): %s", (raw[:200] + "…") if len(raw) > 200 else raw)
                    continue

                ts_ms   = int(obj.get("ts", 0))
                now_ms  = int(time.time() * 1000)
                symbol  = str(obj.get("symbol", "")).upper()
                signal  = str(obj.get("signal", "")).upper()
                params  = dict(obj.get("params") or {})
                features= dict(obj.get("features") or {})
                hint_px = None
                if "entry_price" in params:
                    try:
                        hint_px = float(params["entry_price"])
                    except Exception:
                        hint_px = None

                # optional allowlist
                if EXEC_SYMBOLS and symbol not in EXEC_SYMBOLS:
                    log.debug("skip %s (not in EXEC_SYMBOLS)", symbol)
                    continue

                # staleness filter
                if ts_ms and EXEC_MAX_SIGNAL_AGE:
                    if now_ms - ts_ms > EXEC_MAX_SIGNAL_AGE * 1000:
                        log.info("stale signal %s dropped (age=%ds)", symbol, int((now_ms - ts_ms)/1000))
                        continue

                # override tag if provided
                tag = str(params.get("tag", TAG) or "B44")
                link_id = _mk_link_id(symbol, ts_ms or now_ms, ("LONG" if "LONG" in signal else "SHORT"), tag)

                # de-dupe
                if link_id in seen:
                    log.debug("dup %s (already seen)", link_id)
                    continue

                # breaker gate
                if breaker_active():
                    msg = f"⛔ Breaker ON • skip open • {symbol} {signal}"
                    tg_send(msg, priority="warn")
                    log_event("executor", "block_breaker", symbol, "MAIN", {"signal": signal})
                    continue

                # portfolio guard gates (optional)
                if guard is not None:
                    try:
                        if not guard.allow_new_trade(symbol):
                            tg_send(f"⏸️ Guard block • {symbol} • max concurrency/symbol or daily cap hit", priority="warn")
                            log_event("executor", "block_guard", symbol, "MAIN")
                            continue
                        # risk-based sizing reads guard.current_risk_value() in _qty_from_signal
                    except Exception as e:
                        log.warning("guard check error: %s", e)

                side = "Buy" if "LONG" in signal else "Sell"

                ok, msg = _place_entry(symbol, side, link_id, params, hint_px)
                seen[link_id] = int(time.time())
                _save_seen(seen)

                if ok:
                    tg_send(f"✅ ENTRY • {symbol} • {side} • link={link_id}", priority="success")
                    log_event("executor", "entry_ok", symbol, "MAIN", {"side": side, "link": link_id})
                    log.info("entry ok %s %s link=%s", symbol, side, link_id)
                    # NOTE: TP/SL Manager will observe position and create exits
                else:
                    tg_send(f"⚠️ ENTRY FAIL • {symbol} • {side} • {msg}", priority="warn")
                    log_event("executor", "entry_fail", symbol, "MAIN", {"side": side, "error": msg})
                    log.warning("entry fail %s %s: %s", symbol, side, msg)

            # commit offset after processing batch
            pos = new_pos
            _write_offset(pos)

        except KeyboardInterrupt:
            log.info("shutdown requested by user")
            break
        except Exception as e:
            log.error("loop error: %s", e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
