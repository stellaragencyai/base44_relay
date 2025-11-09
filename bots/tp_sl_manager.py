#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (5 equal TPs + adopt-only + laddered SL)
Core-stack edition: HTTP-only sweep; reduce-only safety; guard-aware.

What it does
- Maintains 5 reduce-only TP rungs spaced by equal-R (0.5R..2.5R).
- Re-sizes ladder on position size changes; adopts foreign orders during grace.
- SL policy: pick closer of structure stop vs ATR fallback. Reduce-only safety.
- Maker-first shading so PostOnly rests; no amendments unless we own the order.
- No flattening, no flips, no market closes. Ever.
- **Guard-aware**: respects global breaker via core.guard.
  • When breaker ON: keep/ensure SL protection, pause TP ladder, cancel entry-type non-RO orders,
    and flip working orders to reduce-only where possible.
  • When breaker OFF: full TP ladder maintenance.

Env (via core.config.settings; add in .env to override)
  TP_ADOPT_EXISTING=true
  TP_CANCEL_NON_B44=false
  TP_DRY_RUN=true
  TP_STARTUP_GRACE_SEC=20
  TP_MANAGED_TAG=B44
  TP_PERIODIC_SWEEP_SEC=12

  # Grid and SL
  TP_RUNGS=5
  TP_EQUAL_R_START=0.5
  TP_EQUAL_R_STEP=0.5
  TP_SL_ATR_MULT_FALLBACK=0.45
  TP_SL_ATR_BUFFER=0.08
  TP_SL_TF=5
  TP_SL_LOOKBACK=120
  TP_SL_SWING_WIN=20

  # Maker placement
  TP_POST_ONLY=true
  TP_SPREAD_OFFSET_RATIO=0.35
  TP_MAX_MAKER_OFFSET_TICKS=5
  TP_FALLBACK_OFFSET_TICKS=2

  # Optional symbol control
  TP_SYMBOL_WHITELIST=BTCUSDT,ETHUSDT

Notes
- Uses Bybit v5 client from core.bybit_client (private endpoints for orders/positions).
- Uses tools.notifier_telegram.tg via a tiny compat wrapper (quiet by default).
"""

from __future__ import annotations
import json
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from tools.notifier_telegram import tg
from core.guard import guard_blocking_reason  # ← use the real breaker, not a file hack

# ---------- logging ----------
log = get_logger("bots.tp_sl_manager")
log = bind_context(log, comp="tpsl")
getcontext().prec = 28

# ---------- config ----------
ROOT = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

TP_ADOPT_EXISTING = str(getattr(settings, "TP_ADOPT_EXISTING", "true")).lower() in ("1","true","yes","on")
TP_CANCEL_NON_B44 = str(getattr(settings, "TP_CANCEL_NON_B44", "false")).lower() in ("1","true","yes","on")
TP_DRY_RUN        = str(getattr(settings, "TP_DRY_RUN", "true")).lower() in ("1","true","yes","on")
TP_GRACE_SEC      = int(getattr(settings, "TP_STARTUP_GRACE_SEC", 20))
TP_TAG            = (str(getattr(settings, "TP_MANAGED_TAG", "B44")).strip() or "B44")[:12]  # keep linkIds short
SWEEP_SEC         = int(getattr(settings, "TP_PERIODIC_SWEEP_SEC", 12))

RUNGS             = max(1, int(getattr(settings, "TP_RUNGS", 5)))
R_START           = Decimal(str(getattr(settings, "TP_EQUAL_R_START", 0.5)))
R_STEP            = Decimal(str(getattr(settings, "TP_EQUAL_R_STEP", 0.5)))

POST_ONLY         = str(getattr(settings, "TP_POST_ONLY", "true")).lower() in ("1","true","yes","on")
SPREAD_RATIO      = float(getattr(settings, "TP_SPREAD_OFFSET_RATIO", 0.35))
MAX_OFFSET_TICKS  = max(1, int(getattr(settings, "TP_MAX_MAKER_OFFSET_TICKS", 5)))
FALLBACK_OFFSET   = max(1, int(getattr(settings, "TP_FALLBACK_OFFSET_TICKS", 2)))

SL_ATR_MULT_FB    = float(getattr(settings, "TP_SL_ATR_MULT_FALLBACK", 0.45))
SL_ATR_BUF        = float(getattr(settings, "TP_SL_ATR_BUFFER", 0.08))
SL_TF             = str(getattr(settings, "TP_SL_TF", 5))
SL_LOOKBACK       = max(30, int(getattr(settings, "TP_SL_LOOKBACK", 120)))
SL_SWING_WIN      = max(5, int(getattr(settings, "TP_SL_SWING_WIN", 20)))

HTTP_TIMEOUT_S    = int(getattr(settings, "HTTP_TIMEOUT_S", 10))
BYBIT_PUBLIC      = (getattr(settings, "BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/"))

SYMBOL_WHITELIST  = [s.strip().upper() for s in str(getattr(settings, "TP_SYMBOL_WHITELIST", "") or "").split(",") if s.strip()]

# ---------- clients ----------
by = Bybit()
try:
    by.sync_time()
except Exception as e:
    log.warning("time sync failed: %s", e)

# ---------- notifier compat ----------
class _CompatTG:
    @staticmethod
    def send(text: str):
        try:
            tg.safe_text(text, quiet=True)
        except Exception:
            pass

def tg_send(msg: str, priority: str = "info", **kwargs):
    _CompatTG.send(msg)

# ---------- public HTTP helpers (no auth) ----------
import urllib.request, urllib.parse

def _http_get(url: str, timeout: int = HTTP_TIMEOUT_S) -> Tuple[bool, Dict, str]:
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        return False, {}, f"http error: {e}"
    try:
        data = json.loads(raw)
    except Exception:
        return False, {}, f"bad json: {raw[:300]}"
    if data.get("retCode") == 0:
        return True, data, ""
    return False, data, f"retCode={data.get('retCode')} retMsg={data.get('retMsg')}"

def _q(params: Dict[str, str]) -> str:
    return urllib.parse.urlencode(params)

def get_instruments_info(symbol: str) -> Dict:
    ok, data, err = _http_get(f"{BYBIT_PUBLIC}/v5/market/instruments-info?{_q({'category':'linear','symbol':symbol})}")
    if not ok:
        raise RuntimeError(err)
    arr = (data.get("result") or {}).get("list") or []
    return arr[0] if arr else {}

def get_orderbook_top(symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
    ok, data, err = _http_get(f"{BYBIT_PUBLIC}/v5/market/orderbook?{_q({'category':'linear','symbol':symbol,'limit':'1'})}")
    if not ok:
        log.warning("orderbook err %s: %s", symbol, err)
        return None
    r = (data.get("result") or {})
    bids = r.get("b") or r.get("bids") or []
    asks = r.get("a") or r.get("asks") or []
    if not bids or not asks:
        return None
    return Decimal(str(bids[0][0])), Decimal(str(asks[0][0]))

def get_kline(symbol: str, interval: str, limit: int) -> List[List[str]]:
    ok, data, err = _http_get(f"{BYBIT_PUBLIC}/v5/market/kline?{_q({'category':'linear','symbol':symbol,'interval':str(interval),'limit':str(limit)})}")
    if not ok:
        log.warning("kline err %s: %s", symbol, err)
        return []
    return (data.get("result") or {}).get("list") or []

# ---------- symbol filters ----------
@dataclass
class SymbolFilters:
    tick: Decimal
    step: Decimal
    min_qty: Decimal

def get_symbol_filters(symbol: str) -> SymbolFilters:
    info = get_instruments_info(symbol)
    tick = Decimal(info["priceFilter"]["tickSize"])
    step = Decimal(info["lotSizeFilter"]["qtyStep"])
    minq = Decimal(info["lotSizeFilter"]["minOrderQty"])
    return SymbolFilters(tick=tick, step=step, min_qty=minq)

def round_to_step(x: Decimal, step: Decimal) -> Decimal:
    steps = (x / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step

def round_to_tick(x: Decimal, tick: Decimal) -> Decimal:
    steps = (x / tick).to_integral_value(rounding=ROUND_DOWN)
    return steps * tick

# ---------- SL computation ----------
def _structure_stop(symbol: str, side_word: str, entry: Decimal, tick: Decimal) -> Optional[Decimal]:
    rows = get_kline(symbol, SL_TF, SL_LOOKBACK)
    if not rows:
        return None
    lows, highs, trs = [], [], []
    prev_close: Optional[Decimal] = None
    for it in rows:
        _open,_high,_low,_close = map(Decimal, [it[1], it[2], it[3], it[4]])
        lows.append(_low); highs.append(_high)
        if prev_close is not None:
            trs.append(max(_high-_low, abs(_high-prev_close), abs(_low-prev_close)))
        prev_close = _close
    atr = (sum(trs[-14:]) / Decimal(14)) if len(trs) >= 14 else Decimal(0)
    atr_buf = atr * Decimal(str(SL_ATR_BUF))
    if side_word == "long":
        stop = min(lows[-SL_SWING_WIN:]) - atr_buf
    else:
        stop = max(highs[-SL_SWING_WIN:]) + atr_buf
    return round_to_tick(stop, tick)

def _atr_fallback_stop(symbol: str, side_word: str, entry: Decimal, tick: Decimal) -> Optional[Decimal]:
    rows = get_kline(symbol, SL_TF, SL_LOOKBACK)
    if not rows:
        return None
    prev_close: Optional[Decimal] = None
    trs: List[Decimal] = []
    for it in rows:
        _open,_high,_low,_close = map(Decimal, [it[1], it[2], it[3], it[4]])
        if prev_close is not None:
            trs.append(max(_high-_low, abs(_high-prev_close), abs(_low-prev_close)))
        prev_close = _close
    if len(trs) < 14:
        return None
    atr = sum(trs[-14:]) / Decimal(14)
    move = atr * Decimal(str(SL_ATR_MULT_FB))
    stop = entry - move if side_word == "long" else entry + move
    return round_to_tick(stop, tick)

def _pick_closer(entry: Decimal, a: Optional[Decimal], b: Optional[Decimal], side_word: str, tick: Decimal) -> Decimal:
    cands = [x for x in (a, b) if x is not None]
    if cands:
        return min(cands, key=lambda s: abs(entry - s))
    # minimal buffer
    return round_to_tick(entry - tick*Decimal(2) if side_word == "long" else entry + tick*Decimal(2), tick)

def ensure_stop(symbol: str, side_word: str, entry: Decimal, pos_idx: int, tick: Decimal) -> Decimal:
    # If a stopLoss already exists on position, keep it
    try:
        ok, data, _ = by.get_positions(category="linear", symbol=symbol)
        if ok:
            lst = (data.get("result") or {}).get("list") or []
            for p in lst:
                if int(p.get("positionIdx") or 0) == int(pos_idx):
                    cur = p.get("stopLoss")
                    if cur:
                        cur_d = Decimal(str(cur))
                        if cur_d > 0:
                            return round_to_tick(cur_d, tick)
    except Exception:
        pass

    s_struct = _structure_stop(symbol, side_word, entry, tick)
    s_atr    = _atr_fallback_stop(symbol, side_word, entry, tick)
    stop = _pick_closer(entry, s_struct, s_atr, side_word, tick)

    if TP_DRY_RUN:
        tg_send(f"🛑 {symbol}: DRY_RUN would set SL at {stop}")
        return stop

    # Use position/trading-stop
    body = {"category":"linear","symbol":symbol,"positionIdx":pos_idx,"stopLoss":str(stop)}
    ok, _, err = by._request_private_json("/v5/position/trading-stop", body=body, method="POST")
    if not ok:
        log.warning("set SL failed %s: %s", symbol, err)
    else:
        tg_send(f"🛡️ {symbol} SL set {stop}")
    return stop

# ---------- maker shading ----------
def adaptive_offset_ticks(symbol: str, tick: Decimal) -> int:
    ob = get_orderbook_top(symbol)
    if not ob:
        return FALLBACK_OFFSET
    bid, ask = ob
    spread = max(Decimal("0"), ask - bid)
    if spread <= 0:
        return 1
    spread_ticks = int((spread / tick).to_integral_value(rounding=ROUND_DOWN))
    base = max(1, int(spread_ticks * SPREAD_RATIO))
    return int(min(max(base, 1), MAX_OFFSET_TICKS))

# ---------- helpers ----------
def side_to_close(side_word: str) -> str:
    return "Sell" if side_word.lower().startswith("l") else "Buy"

def managed_link(link: Optional[str]) -> bool:
    return bool(link and (TP_TAG in str(link)))

def make_link(base: str = "tp") -> str:
    s = base if base.startswith(TP_TAG) else f"{TP_TAG}-{base}"
    return s[:36]

_t0 = time.monotonic()
def in_grace() -> bool:
    return (time.monotonic() - _t0) < max(0, TP_GRACE_SEC)

def _allowed_symbol(sym: str) -> bool:
    return not SYMBOL_WHITELIST or sym.upper() in SYMBOL_WHITELIST

# ---------- order ops ----------
def fetch_open_tp_orders(symbol: str, close_side: str) -> List[dict]:
    ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
    if not ok:
        log.warning("open_orders err %s: %s", symbol, err)
        return []
    rows = (data.get("result") or {}).get("list") or []
    out = []
    for it in rows:
        try:
            if str(it.get("reduceOnly","")).lower() not in ("true","1"):
                continue
            if (it.get("side") or "") != close_side:
                continue
            if (it.get("orderType") or "") != "Limit":
                continue
            out.append(it)
        except Exception:
            continue
    return out

def list_all_open_orders(symbol: str) -> List[dict]:
    ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
    if not ok:
        log.warning("open_orders err %s: %s", symbol, err)
        return []
    return (data.get("result") or {}).get("list") or []

def place_limit_reduce(symbol: str, side: str, price: Decimal, qty: Decimal, tick: Decimal) -> Optional[str]:
    # Shade away from mid so PostOnly rests
    off = adaptive_offset_ticks(symbol, tick)
    px = price + tick*off if side == "Sell" else price - tick*off

    qtxt = f"{qty.normalize()}"
    ptxt = f"{px.normalize()}"

    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: {side} {symbol} qty={qtxt} @ {ptxt}")
        return None

    ok, data, err = by.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Limit",
        qty=qtxt,
        price=ptxt,
        timeInForce="PostOnly" if POST_ONLY else "GoodTillCancel",
        reduceOnly=True,
        orderLinkId=make_link("tp"),
    )
    if not ok:
        log.warning("place_order fail %s: %s", symbol, err)
        return None
    return (data.get("result") or {}).get("orderId")

def cancel_order(symbol: str, order_id: str, link_id: Optional[str]) -> None:
    if in_grace():
        tg_send(f"🔒 Cancel blocked by grace: {order_id}")
        return
    if (not managed_link(link_id)) and (not TP_CANCEL_NON_B44):
        tg_send(f"🔒 Keep foreign order (non-Base44): {order_id}")
        return
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: cancel orderId={order_id}")
        return
    by.cancel_order(category="linear", symbol=symbol, orderId=order_id)

# ---------- grid construction ----------
def split_even(total: Decimal, step: Decimal, minq: Decimal, n: int) -> List[Decimal]:
    if n <= 0 or total <= 0:
        return [Decimal("0")] * max(0, n)
    ideal = total / Decimal(n)
    chunks = [round_to_step(ideal, step) for _ in range(n)]

    # redistribute rounding diff
    diff = total - sum(chunks)
    if diff != 0:
        sgn = 1 if diff > 0 else -1
        diff_abs = abs(diff)
        while diff_abs >= step:
            for i in range(n):
                if diff_abs < step:
                    break
                new_q = chunks[i] + (step if sgn > 0 else -step)
                if new_q >= 0:
                    chunks[i] = new_q
                    diff_abs -= step
            if diff_abs < step:
                break

    # enforce min rung qty by pulling from larger rungs
    for i in range(n):
        if chunks[i] == 0:
            continue
        if chunks[i] < minq:
            need = minq - chunks[i]
            for j in range(n):
                if j == i:
                    continue
                give = min(need, max(Decimal(0), chunks[j] - minq))
                if give > 0:
                    chunks[j] -= give
                    chunks[i] += give
                    need -= give
                if need <= 0:
                    break
    # zero-out sub-min rungs if still impossible
    for i in range(n):
        if 0 < chunks[i] < minq:
            chunks[i] = Decimal("0")
    return chunks

def build_equal_r_targets(entry: Decimal, stop: Decimal, side_word: str, tick: Decimal) -> List[Decimal]:
    targets: List[Decimal] = []
    r_value = abs(entry - stop)
    # guard against zero-distance stops
    if r_value <= 0:
        r_value = tick * Decimal(3)
    for i in range(RUNGS):
        dist_R = R_START + Decimal(i) * R_STEP
        offset = dist_R * r_value
        raw_px = entry + offset if side_word == "long" else entry - offset
        targets.append(round_to_tick(raw_px, tick))
    return targets

# ---------- breaker-aware enforcement ----------
def enforce_reduce_only_and_cancel_entries(symbol: str) -> None:
    """
    On breaker ON:
      - cancel obvious entry orders (non-reduce-only LIMIT/MARKET)
      - flip amendable orders to reduce-only if possible
    """
    try:
        orders = list_all_open_orders(symbol)
    except Exception as e:
        log.warning("list_open_orders failed %s: %s", symbol, e)
        return

    for o in orders:
        try:
            reduce_only = str(o.get("reduceOnly", "0")).lower() in {"1", "true"}
            side = str(o.get("side", "")).upper()
            ord_type = str(o.get("orderType", "")).upper()
            link = o.get("orderLinkId")
            oid  = o.get("orderId")
            qty  = Decimal(str(o.get("qty", "0") or "0"))

            # Treat any non-reduce-only order as "entry-like" and cancel it
            if not reduce_only and ord_type in {"LIMIT", "MARKET"} and qty > 0:
                if TP_DRY_RUN:
                    tg_send(f"✋ DRY_RUN breaker: cancel entry {symbol} {side} qty={qty}")
                else:
                    ok, _, err = by.cancel_order(category="linear", orderId=oid, symbol=symbol)
                    if not ok:
                        log.warning("cancel entry failed %s: %s", symbol, err)
                continue

            # Flip to reduce-only when possible
            if not reduce_only:
                if TP_DRY_RUN:
                    tg_send(f"🔒 DRY_RUN breaker: set reduce-only on {symbol} order {oid}")
                else:
                    ok, _, err = by.amend_order(category="linear", orderId=oid, reduceOnly=True)
                    if not ok:
                        log.warning("amend reduce-only failed %s: %s", symbol, err)
        except Exception as e:
            log.warning("breaker RO enforcement err %s: %s", symbol, e)

# ---------- core sync ----------
def place_or_sync_ladder(symbol: str, side_word: str, entry: Decimal, qty: Decimal, pos_idx: int) -> None:
    filters = get_symbol_filters(symbol)
    tick, step, minq = filters.tick, filters.step, filters.min_qty
    close_side = side_to_close(side_word)

    # Always ensure SL first
    stop = ensure_stop(symbol, side_word, entry, pos_idx, tick)

    # Check guard/breaker
    blocked, why = guard_blocking_reason()
    if blocked:
        # Under breaker: enforce reduce-only on any stray orders and pause TPs.
        enforce_reduce_only_and_cancel_entries(symbol)
        tg_send(f"⛔ Breaker ON • {symbol} • SL ensured at {stop} • TPs paused ({why})")
        return

    # Build ladder targets and chunks
    targets = build_equal_r_targets(entry, stop, side_word, tick)
    target_chunks = split_even(qty, step, minq, RUNGS)
    existing = fetch_open_tp_orders(symbol, close_side)

    # Match existing rungs by proximity
    matched: List[Optional[dict]] = [None]*RUNGS
    used = set()
    tol = max(tick, tick*2)
    for ex in existing:
        try:
            px = Decimal(str(ex.get("price")))
            for i, tpx in enumerate(targets):
                if i in used:
                    continue
                if abs(px - tpx) <= tol:
                    matched[i] = ex
                    used.add(i)
                    break
        except Exception:
            continue

    adopt_only = in_grace() or TP_ADOPT_EXISTING
    placed = 0

    for i, (tpx, tq) in enumerate(zip(targets, target_chunks)):
        ex = matched[i]
        ex_id = ex.get("orderId") if ex else None
        ex_link = ex.get("orderLinkId") if ex else None

        if tq <= 0:
            if ex_id:
                cancel_order(symbol, ex_id, ex_link)
            continue

        if not ex_id:
            oid = place_limit_reduce(symbol, close_side, tpx, tq, tick)
            if oid:
                placed += 1
            continue

        # We have an existing rung near our target
        if (not managed_link(ex_link)) and adopt_only:
            # Keep foreign rung as-is during grace/adopt
            continue

        # If we own it and qty is too far off, simple policy: cancel and replace
        try:
            cur_px = Decimal(str(ex.get("price")))
            cur_qty = Decimal(str(ex.get("qty")))
        except Exception:
            cur_px, cur_qty = tpx, tq

        # Replace if price drifted beyond tolerance, or qty deviates by >= 1 step
        if abs(cur_px - tpx) > tol or abs(cur_qty - tq) >= step:
            cancel_order(symbol, ex_id, ex_link)
            place_limit_reduce(symbol, close_side, tpx, tq, tick)

    tg_send(f"✅ {symbol} ladder sync • qty={qty} • entry={entry} • stop={stop}\nTPs: {', '.join(str(x) for x in targets)}")

# ---------- sweep loop ----------
def _side_word_from_row(p: dict) -> Optional[str]:
    try:
        side_raw = (p.get("side") or "").lower()
        if side_raw.startswith("b"):
            return "long"
        if side_raw.startswith("s"):
            return "short"
    except Exception:
        pass
    # fallback using size sign if present
    try:
        sz = Decimal(p.get("size") or "0")
        if sz > 0:
            return "long"
        if sz < 0:
            return "short"
    except Exception:
        pass
    return None

def sweep_once() -> None:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        log.warning("positions err: %s", err)
        return
    rows = (data.get("result") or {}).get("list") or []
    for p in rows:
        try:
            symbol = (p.get("symbol") or "").upper()
            if not symbol or not _allowed_symbol(symbol):
                continue
            size = Decimal(str(p.get("size") or "0"))
            if size <= 0:
                continue
            side_word = _side_word_from_row(p)
            if not side_word:
                continue
            entry = Decimal(str(p.get("avgPrice") or "0"))
            if entry <= 0:
                continue
            pos_idx = int(p.get("positionIdx") or 0)
            place_or_sync_ladder(symbol, side_word, entry, abs(size), pos_idx)
        except Exception as e:
            log.warning("sweep row error: %s row=%s", e, p)

def main() -> None:
    tg_send(f"🟢 TP/SL Manager online • dry={TP_DRY_RUN} grace={TP_GRACE_SEC}s adopt={TP_ADOPT_EXISTING} tag={TP_TAG} sweep={SWEEP_SEC}s")
    # Bootstrap once
    sweep_once()
    # Loop
    while True:
        try:
            time.sleep(max(5, SWEEP_SEC))
            sweep_once()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("loop error: %s", e)
            time.sleep(SWEEP_SEC)

if __name__ == "__main__":
    main()
