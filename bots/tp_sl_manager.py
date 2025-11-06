#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (5 equal TPs + auto-resize + laddered trailing SL)
HARDENED EDITION (final): startup grace, DRY_RUN, adopt-only, reduce-only, Base44 tagging,
optional WS, notifier integration, sub-account routing fixed, correct maker shading.

What this does:
- 5 TP rungs, equal-R spacing (0.5R steps to 2.5R), equal size per rung.
- On size change (scale-in), re-sizes all 5 rungs so sum(qty)=position size.
- Trailing SL ladder: TP2→SL=TP1; TP3→SL=TP2; TP4→SL=TP3; TP5→SL=TP4.
- SL tightened: chooses closer of structure-stop vs ATR-stop.
- Maker-only, reduce-only; periodic HTTP sweep; optional private WS with watchdog & auto-fallback.

Safety rail additions (env-driven):
  TP_ADOPT_EXISTING=true        # adopt existing ladder; do not cancel foreign orders
  TP_CANCEL_NON_B44=false       # never cancel orders we didn't tag (unless you set true)
  TP_DRY_RUN=true               # report intended actions but place nothing
  TP_STARTUP_GRACE_SEC=20       # during grace: no cancels; only tagged reduce-only placements allowed
  TP_MANAGED_TAG=B44            # orderLinkId prefix we own and match against for cancellations
  TP_WS_DISABLE=false           # disable WS entirely; rely on HTTP sweep
  TP_PERIODIC_SWEEP_SEC=12      # sweep frequency (seconds)

Requires:
  pip install pybit requests python-dotenv pyyaml
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from decimal import Decimal, ROUND_DOWN, getcontext
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

# Notifications
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str = "info", **kwargs):
        logging.getLogger("tp5eq").info(f"[notify/{priority}] {msg}")

# Account policy + decision logging (soft imports; no hard fail)
try:
    from core.account_policy import (
        may_manage_exits,
        reconciler_can_protect_manual,
        get_account,
    )
except Exception:
    def may_manage_exits(_uid: str) -> bool: return True
    def reconciler_can_protect_manual() -> bool: return True
    def get_account(uid: str) -> dict: return {"uid": uid, "mode": "auto"}

try:
    from core.decision_log import log_event
except Exception:
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        print(f"[DECLOG/{component}/{event}] {symbol} @{account_uid} {payload or {}}")

getcontext().prec = 28

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tp5eq")

# ---------- env/root ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=REPO_ROOT / ".env")

STATE_DIR = REPO_ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

def env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, str(int(default))) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}

def env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, str(default)) or "").strip())
    except Exception:
        return default

TP_ADOPT_EXISTING = env_bool("TP_ADOPT_EXISTING", True)
TP_CANCEL_NON_B44 = env_bool("TP_CANCEL_NON_B44", False)
TP_DRY_RUN        = env_bool("TP_DRY_RUN", True)
TP_GRACE_SEC      = env_int("TP_STARTUP_GRACE_SEC", 20)
TP_TAG            = (os.getenv("TP_MANAGED_TAG", "B44") or "B44").strip()
TP_WS_DISABLE     = env_bool("TP_WS_DISABLE", False)
TP_SWEEP_SEC_ENV  = env_int("TP_PERIODIC_SWEEP_SEC", 12)

# ---------- config ----------
CFG = {
    # Equal-R TP grid (exactly 5 rungs)
    "tp_rungs": 5,
    "tp_equal_r_start": 0.5,   # first TP at 0.5R
    "tp_equal_r_step": 0.5,    # step between rungs

    # maker placement
    "maker_post_only": True,
    "spread_offset_ratio": 0.35,
    "max_maker_offset_ticks": 5,
    "fallback_maker_offset_ticks": 2,
    "price_band_retry": True,
    "price_band_max_retries": 2,

    # SL logic (tightened)
    "sl_use_closer_of_structure_or_atr": True,
    "sl_atr_mult_fallback": 0.45,
    "sl_atr_buffer": 0.08,
    "sl_kline_tf": "5",
    "sl_kline_count": 120,
    "sl_swing_lookback": 20,
    "sl_min_tick_buffer": 2,

    # market/category
    "category": "linear",

    # symbol handling
    "listen_symbols": "*",
    "symbol_blocklist": [],

    # leverage policy (optional)
    "require_min_max_leverage": False,
    "min_max_leverage": 75,

    # settle scan
    "settle_coins": ["USDT", "USDC"],

    # WS resilience
    "ws_ping_interval": 15,
    "ws_ping_timeout": 10,
    "ws_silence_watchdog_secs": 25,
    "ws_backoff_start": 2,
    "ws_backoff_max": 60,

    # periodic sweep to catch misses
    "periodic_sweep_sec": 12,

    # WS optional toggle (overridden by env)
    "ws_optional": False,
}

# Apply env overrides
CFG["ws_optional"] = TP_WS_DISABLE
CFG["periodic_sweep_sec"] = TP_SWEEP_SEC_ENV

# ---------- env / clients ----------
BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_ENV = os.getenv("BYBIT_ENV", "mainnet").lower().strip()
SUB_UIDS = [s.strip() for s in os.getenv("SUB_UIDS", "").split(",") if s.strip()]
if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env at project root.")

http = HTTP(testnet=(BYBIT_ENV == "testnet"), api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)

# --- breaker flag ---
_BREAKER_FILE = STATE_DIR / "risk_state.json"
def breaker_active() -> bool:
    try:
        if not _BREAKER_FILE.exists():
            return False
        js = json.loads(_BREAKER_FILE.read_text(encoding="utf-8"))
        return bool(js.get("breach"))
    except Exception:
        return False

# ---------- account mapping ----------
def account_key_to_uid(account_key: str) -> str:
    """Convert internal key to policy UID."""
    if account_key == "main":
        return "MAIN"
    if account_key.startswith("sub:"):
        return account_key.split(":", 1)[1]
    return account_key

# pybit expects memberId for subaccount scoping (not subUid)
def extra_for_uid(uid: Optional[str]) -> dict:
    return {} if not uid else {"memberId": uid}

# ---------- precision / symbol meta ----------
@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_order_qty: Decimal
    max_leverage: int

def get_symbol_filters(symbol: str, extra: dict) -> SymbolFilters:
    # ignore memberId on public market endpoints
    res = http.get_instruments_info(category=CFG["category"], symbol=symbol)
    lst = res.get("result", {}).get("list", [])
    if not lst:
        raise RuntimeError(f"symbol info not found for {symbol}")
    item = lst[0]
    tick = Decimal(item["priceFilter"]["tickSize"])
    step = Decimal(item["lotSizeFilter"]["qtyStep"])
    minq = Decimal(item["lotSizeFilter"]["minOrderQty"])
    lev = 0
    try:
        lev = int(Decimal(str(item.get("leverageFilter", {}).get("maxLeverage", "0"))))
    except Exception:
        lev = 0
    return SymbolFilters(tick, step, minq, lev)

def round_to_step(q: Decimal, step: Decimal) -> Decimal:
    steps = (q / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step

def round_price_to_tick(p: Decimal, tick: Decimal) -> Decimal:
    steps = (p / tick).to_integral_value(rounding=ROUND_DOWN)
    return steps * tick

# ---------- market data ----------
def get_orderbook_top(symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
    try:
        ob = http.get_orderbook(category=CFG["category"], symbol=symbol, limit=1)
        r = ob.get("result", {})
        bids = r.get("b", []) or r.get("bids") or []
        asks = r.get("a", []) or r.get("asks") or []
        if not bids or not asks:
            return None
        return Decimal(str(bids[0][0])), Decimal(str(asks[0][0]))
    except Exception as e:
        log.warning(f"orderbook error {symbol}: {e}")
        return None

def adaptive_offset_ticks(symbol: str, tick: Decimal) -> int:
    ob = get_orderbook_top(symbol)
    if not ob:
        return CFG["fallback_maker_offset_ticks"]
    bid, ask = ob
    spread = max(Decimal("0"), ask - bid)
    if spread <= 0:
        return 1
    spread_ticks = int((spread / tick).to_integral_value(rounding=ROUND_DOWN))
    base = max(1, round(spread_ticks * CFG["spread_offset_ratio"]))
    return int(min(max(base, 1), CFG["max_maker_offset_ticks"]))

# ---------- helpers ----------
def side_to_close(position_side_word: str) -> str:
    return "Sell" if position_side_word.lower().startswith("l") else "Buy"

# ---------- SL calculations ----------
def _compute_structure_stop(symbol: str, side_word: str, entry: Decimal, tick: Decimal, extra: dict) -> Optional[Decimal]:
    try:
        res = http.get_kline(category=CFG["category"], symbol=symbol, interval=CFG["sl_kline_tf"], limit=CFG["sl_kline_count"], **extra)
        arr = (res.get("result") or {}).get("list") or []
        if not arr:
            return None
        lows, highs, trs = [], [], []
        prev_close = None
        for it in arr:
            o, h, l, c = map(Decimal, [it[1], it[2], it[3], it[4]])
            lows.append(l); highs.append(h)
            if prev_close is not None:
                trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
            prev_close = c
        atr = (sum(trs[-14:]) / Decimal(14)) if len(trs) >= 14 else Decimal(0)
        atr_buf = atr * Decimal(str(CFG["sl_atr_buffer"]))
        if side_word == "long":
            stop = min(lows[-(CFG["sl_swing_lookback"] or 20):]) - atr_buf
        else:
            stop = max(highs[-(CFG["sl_swing_lookback"] or 20):]) + atr_buf
        return round_price_to_tick(Decimal(stop), tick)
    except Exception:
        return None

def _compute_fallback_atr_stop(symbol: str, side_word: str, entry: Decimal, tick: Decimal, extra: dict) -> Optional[Decimal]:
    try:
        res = http.get_kline(category=CFG["category"], symbol=symbol, interval=CFG["sl_kline_tf"], limit=CFG["sl_kline_count"], **extra)
        arr = (res.get("result") or {}).get("list") or []
        if not arr:
            return None
        prev_close = None
        trs = []
        for it in arr:
            o, h, l, c = map(Decimal, [it[1], it[2], it[3], it[4]])
            if prev_close is not None:
                trs.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
            prev_close = c
        if len(trs) < 14:
            return None
        atr = sum(trs[-14:]) / Decimal(14)
        move = atr * Decimal(str(CFG["sl_atr_mult_fallback"]))
        stop = entry - move if side_word == "long" else entry + move
        return round_price_to_tick(stop, tick)
    except Exception:
        return None

def _pick_closer_stop(entry: Decimal, a: Optional[Decimal], b: Optional[Decimal], side_word: str, tick: Decimal) -> Decimal:
    choices = [x for x in (a, b) if x is not None]
    if choices:
        return min(choices, key=lambda s: abs(entry - s))
    # fallback minimal buffer
    return round_price_to_tick(entry - tick * Decimal(CFG["sl_min_tick_buffer"]) if side_word == "long"
                               else entry + tick * Decimal(CFG["sl_min_tick_buffer"]), tick)

def ensure_stop(account_uid: str, symbol: str, side_word: str, entry: Decimal, pos_idx: int, tick: Decimal, extra: dict) -> Decimal:
    # check existing SL
    try:
        res = http.get_positions(category=CFG["category"], symbol=symbol, **extra)
        lst = res.get("result", {}).get("list", [])
        if lst and lst[0].get("stopLoss"):
            cur = Decimal(str(lst[0]["stopLoss"]))
            if cur > 0:
                return cur
    except Exception as e:
        log.debug(f"check SL failed: {e}")

    s_struct = _compute_structure_stop(symbol, side_word, entry, tick, extra)
    s_atr    = _compute_fallback_atr_stop(symbol, side_word, entry, tick, extra)
    stop = _pick_closer_stop(entry, s_struct, s_atr, side_word, tick)

    try:
        if TP_DRY_RUN:
            tg_send(f"🛑 {symbol}: DRY_RUN would set SL at {stop}", priority="info")
            log_event("tp_manager", "sl_compute_dry", symbol, account_uid, {"stop": str(stop)})
        else:
            http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(stop), **extra)
            tg_send(f"🛑 <b>{symbol}</b>: Auto-SL set at {stop}.", priority="success")
            log_event("tp_manager", "sl_set", symbol, account_uid, {"stop": str(stop)})
    except Exception as e:
        log.warning(f"set SL failed: {e}")
        log_event("tp_manager", "sl_set_error", symbol, account_uid, {"error": str(e)}, level="error")
    return Decimal(stop)

# ---------- guardrails ----------
_t0 = time.monotonic()
def in_grace() -> bool:
    return (time.monotonic() - _t0) < max(0, TP_GRACE_SEC)

def managed_link(link: Optional[str]) -> bool:
    if not link:
        return False
    try:
        return TP_TAG in str(link)
    except Exception:
        return False

def make_link(link_hint: Optional[str] = None) -> str:
    base = link_hint.strip() if link_hint else "auto"
    if not base.startswith(TP_TAG):
        base = f"{TP_TAG}-{base}"
    return base

# ---------- orders ----------
def place_limit_reduce(account_uid: str, symbol: str, side: str, price: Decimal, qty: Decimal, tick: Decimal, extra: dict) -> Optional[str]:
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: place {side} {symbol} qty={qty} @ {price}", priority="info")
        log_event("tp_manager", "tp_create_dry", symbol, account_uid, {"side": side, "qty": str(qty), "px": str(price)})
        return None
    # maker shading: move AWAY from mid so PostOnly rests
    maker_ticks = adaptive_offset_ticks(symbol, tick)
    px = price + tick * maker_ticks if side == "Sell" else price - tick * maker_ticks

    # min qty check early
    try:
        filters = get_symbol_filters(symbol, extra)
        if qty < filters.min_order_qty:
            tg_send(f"⚠️ {symbol}: skip rung qty<{filters.min_order_qty} (rounded too small)", priority="warn")
            log_event("tp_manager", "tp_skip_too_small", symbol, account_uid, {"qty": str(qty), "minQty": str(filters.min_order_qty)})
            return None
    except Exception:
        pass

    body = dict(
        category=CFG["category"], symbol=symbol, side=side, orderType="Limit",
        qty=str(qty.normalize()), price=str(px.normalize()),
        timeInForce="PostOnly" if CFG["maker_post_only"] else "GoodTillCancel",
        reduceOnly=True,
        orderLinkId=make_link()
    )
    retries = CFG["price_band_max_retries"] if CFG["price_band_retry"] else 0
    for attempt in range(retries + 1):
        try:
            r = http.place_order(**body, **extra)
            if r.get("retCode") in (0, "0"):
                oid = (r.get("result") or {}).get("orderId")
                log_event("tp_manager", "tp_create", symbol, account_uid, {"orderId": oid, "side": side, "qty": str(qty), "px": str(px)})
                return oid
            # price band/invalid price → walk one tick and retry
            if attempt < retries:
                px = px + tick if side == "Sell" else px - tick
                body["price"] = str(px)
                time.sleep(0.2)
        except Exception as e:
            log.warning(f"place_order ex: {e}")
            if attempt < retries:
                time.sleep(0.3)
    log_event("tp_manager", "tp_create_fail", symbol, account_uid, {"side": side, "qty": str(qty), "px": str(price)})
    return None

def amend_order_qty(account_uid: str, symbol: str, order_id: str, qty: Decimal, extra: dict) -> bool:
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: amend qty orderId={order_id} → {qty}", priority="info")
        log_event("tp_manager", "tp_amend_qty_dry", symbol, account_uid, {"orderId": order_id, "qty": str(qty)})
        return True
    try:
        r = http.amend_order(category=CFG["category"], orderId=order_id, qty=str(qty.normalize()), **extra)
        ok = r.get("retCode") in (0, "0")
        log_event("tp_manager", "tp_amend_qty", symbol, account_uid, {"orderId": order_id, "qty": str(qty), "ok": ok})
        return ok
    except Exception as e:
        log.warning(f"amend qty ex: {e}")
        log_event("tp_manager", "tp_amend_qty_error", symbol, account_uid, {"orderId": order_id, "error": str(e)}, level="error")
        return False

def amend_order_price(account_uid: str, symbol: str, order_id: str, price: Decimal, extra: dict) -> bool:
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: amend price orderId={order_id} → {price}", priority="info")
        log_event("tp_manager", "tp_amend_px_dry", symbol, account_uid, {"orderId": order_id, "px": str(price)})
        return True
    try:
        r = http.amend_order(category=CFG["category"], orderId=order_id, price=str(price.normalize()), **extra)
        ok = r.get("retCode") in (0, "0")
        log_event("tp_manager", "tp_amend_px", symbol, account_uid, {"orderId": order_id, "px": str(price), "ok": ok})
        return ok
    except Exception as e:
        log.warning(f"amend price ex: {e}")
        log_event("tp_manager", "tp_amend_px_error", symbol, account_uid, {"orderId": order_id, "error": str(e)}, level="error")
        return False

def cancel_order(account_uid: str, symbol: str, order_id: str, link_id: Optional[str], extra: dict) -> None:
    if in_grace():
        tg_send(f"🔒 Cancel blocked (grace): orderId={order_id}", priority="warn")
        log_event("tp_manager", "tp_cancel_block_grace", symbol, account_uid, {"orderId": order_id})
        return
    if (not managed_link(link_id)) and (not TP_CANCEL_NON_B44):
        tg_send(f"🔒 Cancel blocked (non-Base44): orderId={order_id}", priority="info")
        log_event("tp_manager", "tp_cancel_block_foreign", symbol, account_uid, {"orderId": order_id, "link": link_id})
        return
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: cancel orderId={order_id}", priority="info")
        log_event("tp_manager", "tp_cancel_dry", symbol, account_uid, {"orderId": order_id})
        return
    try:
        http.cancel_order(category=CFG["category"], orderId=order_id, **extra)
        log_event("tp_manager", "tp_cancel", symbol, account_uid, {"orderId": order_id})
    except Exception as e:
        log_event("tp_manager", "tp_cancel_error", symbol, account_uid, {"orderId": order_id, "error": str(e)}, level="error")

def fetch_open_tp_orders(symbol: str, close_side: str, extra: dict) -> List[dict]:
    try:
        r = http.get_open_orders(category=CFG["category"], symbol=symbol, **extra)
        rows = (r.get("result") or {}).get("list") or []
        out = []
        for it in rows:
            try:
                if str(it.get("reduceOnly", "")).lower() not in ("true", "1"):
                    continue
                if (it.get("side") or "") != close_side:
                    continue
                if (it.get("orderType") or "") != "Limit":
                    continue
                out.append(it)
            except Exception:
                continue
        return out
    except Exception as e:
        log.warning(f"open_orders error {symbol}: {e}")
        return []

# ---------- state ----------
class PositionState:
    def __init__(self):
        self.pos: Dict[Tuple[str, str], Dict] = {}
        self.lock = threading.Lock()
STATE = PositionState()

def account_iter():
    yield ("main", extra_for_uid(None))
    for uid in SUB_UIDS:
        yield (f"sub:{uid}", extra_for_uid(uid))

# ---------- policy ----------
@dataclass
class PolicyCheck:
    allowed: bool
    reason: Optional[str]
    filters: Optional['SymbolFilters']

def symbol_allowed_by_policy(symbol: str, extra: dict) -> PolicyCheck:
    # instrument meta + leverage sanity
    try:
        filters = get_symbol_filters(symbol, extra)
    except Exception as e:
        return PolicyCheck(False, f"no instrument meta: {e}", None)
    if CFG["require_min_max_leverage"]:
        need = int(CFG["min_max_leverage"])
        if filters.max_leverage < need:
            return PolicyCheck(False, f"maxLeverage {filters.max_leverage} < required {need}", filters)
    return PolicyCheck(True, None, filters)

# ---------- core ladder logic ----------
def _split_even(total: Decimal, step: Decimal, minq: Decimal, n: int) -> List[Decimal]:
    if n <= 0:
        return []
    ideal = total / Decimal(n)
    chunks = [round_to_step(ideal, step) for _ in range(n)]
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
    # min qty repair
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
    return chunks

def build_equal_r_targets(entry: Decimal, stop: Decimal, side_word: str, tick: Decimal) -> List[Decimal]:
    n = CFG.get("tp_rungs", 5)
    r_start = Decimal(str(CFG.get("tp_equal_r_start", 0.5)))
    r_step  = Decimal(str(CFG.get("tp_equal_r_step", 0.5)))
    r_value = abs(entry - stop)
    targets: List[Decimal] = []
    for i in range(n):
        dist_R = r_start + Decimal(i) * r_step
        offset = (dist_R * r_value) if r_value > Decimal("0") else (dist_R * tick)
        raw_px = entry + offset if side_word == "long" else entry - offset
        targets.append(round_price_to_tick(raw_px, tick))
    return targets

def place_or_sync_ladder(account_key: str, extra: dict, symbol: str, side_word: str, entry: Decimal, qty: Decimal, pos_idx: int):
    uid = account_key_to_uid(account_key)

    # Policy gate: may we manage exits for this account?
    if not may_manage_exits(uid):
        # If policy allows protection for manual accounts, we still set/maintain SL only
        if reconciler_can_protect_manual():
            try:
                filters = get_symbol_filters(symbol, extra)
                stop = ensure_stop(uid, symbol, side_word, entry, pos_idx, filters.tick_size, extra)
                log_event("tp_manager", "policy_protect_only", symbol, uid, {"stop": str(stop)})
                tg_send(f"🛡️ {account_key}:{symbol} protect-only: SL ensured; TP maintenance skipped by policy.", priority="info")
            except Exception:
                pass
        else:
            log_event("tp_manager", "policy_block_exits", symbol, uid, {"reason": "may_manage_exits=false"})
        return

    check = symbol_allowed_by_policy(symbol, extra)
    if not check.allowed:
        with STATE.lock:
            STATE.pos.setdefault((account_key, symbol), {})["tp_placed"] = True
        tg_send(f"⏸️ {account_key}:{symbol} skipped — {check.reason}", priority="warn")
        log_event("tp_manager", "symbol_block", symbol, uid, {"reason": check.reason})
        return

    if breaker_active():
        tick = check.filters.tick_size
        stop = ensure_stop(uid, symbol, side_word, entry, pos_idx, tick, extra)
        with STATE.lock:
            STATE.pos.setdefault((account_key, symbol), {})["tp_placed"] = True
        tg_send(f"⛔ {account_key}:{symbol} breaker active — SL ensured at {stop}, TPs paused.", priority="warn")
        log_event("tp_manager", "breaker_pause", symbol, uid, {"stop": str(stop)})
        return

    tick, step, minq = check.filters.tick_size, check.filters.step_size, check.filters.min_order_qty
    stop = ensure_stop(uid, symbol, side_word, entry, pos_idx, tick, extra)
    targets = build_equal_r_targets(entry, stop, side_word, tick)
    close_side = side_to_close(side_word)

    target_chunks = _split_even(qty, step, minq, CFG["tp_rungs"])

    existing = fetch_open_tp_orders(symbol, close_side, extra)

    adopt_only = in_grace() or TP_ADOPT_EXISTING

    two_ticks = max(tick, tick * 2)
    matched = [None] * CFG["tp_rungs"]
    used = set()
    for order in existing:
        try:
            px = Decimal(str(order.get("price")))
            for i, tpx in enumerate(targets):
                if i in used:
                    continue
                if abs(px - tpx) <= two_ticks:
                    matched[i] = order
                    used.add(i)
                    break
        except Exception:
            continue

    placed = 0
    rung_info = []
    for i, (tpx, tq) in enumerate(zip(targets, target_chunks)):
        ex = matched[i]
        ex_id = ex.get("orderId") if ex else None
        ex_link = ex.get("orderLinkId") if ex else None

        if tq <= 0:
            if ex_id:
                if managed_link(ex_link):
                    cancel_order(uid, symbol, ex_id, ex_link, extra)
                else:
                    tg_send(f"🤝 Adopt keep (qty=0 target): {account_key}:{symbol} rung {i+1} foreign order", priority="info")
                    log_event("tp_manager", "adopt_keep_foreign", symbol, uid, {"rung": i+1, "orderId": ex_id})
            rung_info.append({"px": tpx, "qty": Decimal("0"), "orderId": ex_id})
            continue

        if not ex_id:
            oid = place_limit_reduce(uid, symbol, close_side, tpx, tq, tick, extra)
            rung_info.append({"px": tpx, "qty": tq, "orderId": oid})
            if oid:
                placed += 1
            continue

        if ex_id and (not managed_link(ex_link)) and adopt_only:
            rung_info.append({"px": Decimal(str(ex.get("price"))), "qty": Decimal(str(ex.get("qty"))), "orderId": ex_id})
            log_event("tp_manager", "adopt_foreign", symbol, uid, {"rung": i+1, "orderId": ex_id})
            continue

        cur_px = Decimal(str(ex.get("price")))
        cur_qty = Decimal(str(ex.get("qty")))
        if abs(cur_px - tpx) > tick:
            amend_order_price(uid, symbol, ex_id, tpx, extra)
            cur_px = tpx
        if abs(cur_qty - tq) >= step:
            amend_order_qty(uid, symbol, ex_id, tq, extra)
            cur_qty = tq
        rung_info.append({"px": cur_px, "qty": cur_qty, "orderId": ex_id})

    with STATE.lock:
        STATE.pos[(account_key, symbol)] = {
            "side": side_word,
            "qty": qty,
            "entry": entry,
            "pos_idx": pos_idx,
            "tp_placed": True,
            "tp_filled": STATE.pos.get((account_key, symbol), {}).get("tp_filled", 0),
            "r_value": abs(entry - stop),
            "tick": tick,
            "rungs": rung_info,
            "targets": targets
        }

    px_str = ", ".join([str(x) for x in targets])
    mode = "DRY_RUN" if TP_DRY_RUN else ("ADOPT" if adopt_only else "SYNC")
    tg_send(f"✅ {account_key}:{symbol} ladder {mode} • qty={qty} • entry={entry} • stop={stop}\nTPs: {px_str}", priority="success")
    log_event("tp_manager", "ladder_sync", symbol, uid, {
        "mode": mode, "qty": str(qty), "entry": str(entry), "stop": str(stop),
        "targets": [str(x) for x in targets]
    })

def set_sl(account_key: str, extra: dict, symbol: str, pos_idx: int, new_sl: Decimal):
    uid = account_key_to_uid(account_key)
    try:
        if TP_DRY_RUN:
            tg_send(f"🧪 DRY_RUN: SL → {new_sl} ({account_key}:{symbol})", priority="info")
            log_event("tp_manager", "sl_adjust_dry", symbol, uid, {"new_sl": str(new_sl)})
            return
        http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(new_sl), **extra)
        tg_send(f"🛡️ {account_key}:{symbol} SL → {new_sl}", priority="success")
        log_event("tp_manager", "sl_adjust", symbol, uid, {"new_sl": str(new_sl)})
    except Exception as e:
        log.warning(f"set SL error {account_key}:{symbol}: {e}")
        log_event("tp_manager", "sl_adjust_error", symbol, uid, {"error": str(e)}, level="error")

# ---------- fills: laddered SL ----------
def handle_tp_fill(account_key: str, extra: dict, ev: dict):
    symbol = ev.get("symbol")
    with STATE.lock:
        s = STATE.pos.get((account_key, symbol))
    if not s or not s.get("tp_placed"):
        return

    with STATE.lock:
        s["tp_filled"] = s.get("tp_filled", 0) + 1
        filled_count = s["tp_filled"]
        targets = s.get("targets", [])
        pos_idx = s["pos_idx"]

    filled_px = ev.get("avgPrice") or ev.get("execPrice") or ev.get("price")
    tg_send(f"🎯 {account_key}:{symbol} TP filled {filled_count}/{CFG['tp_rungs']} @ {filled_px}", priority="info")
    log_event("tp_manager", "tp_fill", symbol, account_key_to_uid(account_key), {
        "filled": filled_count, "price": str(filled_px)
    })

    if targets and filled_count >= 2:
        prev_index = min(filled_count - 2, CFG["tp_rungs"] - 2)  # 2->0, 3->1, 4->2, 5->3
        if 0 <= prev_index < len(targets):
            ladder_price = Decimal(str(targets[prev_index]))
            set_sl(account_key, extra, symbol, pos_idx, ladder_price)

# ---------- bootstrap/sweep ----------
def load_positions_once():
    for acct_key, _extra in account_iter():
        uid = account_key_to_uid(acct_key)
        for coin in CFG["settle_coins"]:
            try:
                res = http.get_positions(category=CFG["category"], settleCoin=coin, **_extra)
                arr = res.get("result", {}).get("list", []) or []
                for p in arr:
                    symbol = p.get("symbol") or ""
                    if not symbol:
                        continue
                    if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                        continue
                    size = Decimal(p.get("size") or "0")
                    if size == 0:
                        continue
                    side_word = "long" if (p.get("side", "").lower().startswith("b")) else "short"
                    entry = Decimal(p.get("avgPrice") or "0")
                    pos_idx = int(p.get("positionIdx") or 0)
                    log_event("tp_manager", "bootstrap_pos", symbol, uid, {"qty": str(size), "entry": str(entry)})
                    place_or_sync_ladder(acct_key, _extra, symbol, side_word, entry, size, pos_idx)
            except Exception as e:
                log.error(f"load pos {acct_key} {coin} error: {e}")
                log_event("tp_manager", "bootstrap_error", "", uid, {"error": str(e)}, level="error")

def periodic_sweep_loop():
    while True:
        try:
            for acct_key, _extra in account_iter():
                uid = account_key_to_uid(acct_key)
                for coin in CFG["settle_coins"]:
                    try:
                        res = http.get_positions(category=CFG["category"], settleCoin=coin, **_extra)
                        rows = (res.get("result") or {}).get("list") or []
                        for p in rows:
                            symbol = p.get("symbol") or ""
                            size = Decimal(p.get("size") or "0")
                            if not symbol:
                                continue
                            if size <= 0:
                                with STATE.lock:
                                    if (acct_key, symbol) in STATE.pos:
                                        tg_send(f"🏁 {acct_key}:{symbol} position closed.", priority="info")
                                        log_event("tp_manager", "pos_closed", symbol, uid)
                                    STATE.pos.pop((acct_key, symbol), None)
                                continue
                            side_word = "long" if (p.get("side", "").lower().startswith("b")) else "short"
                            entry = Decimal(p.get("avgPrice") or "0")
                            pos_idx = int(p.get("positionIdx") or 0)

                            with STATE.lock:
                                s = STATE.pos.get((acct_key, symbol))
                                prev_qty = s.get("qty") if s else None
                            if s is None or prev_qty != size or not s.get("tp_placed"):
                                place_or_sync_ladder(acct_key, _extra, symbol, side_word, entry, size, pos_idx)
                    except Exception as e:
                        log.warning(f"sweep {acct_key} {coin} err: {e}")
                        log_event("tp_manager", "sweep_error", "", uid, {"error": str(e)}, level="error")
            time.sleep(CFG["periodic_sweep_sec"])
        except Exception as e:
            log.warning(f"periodic loop err: {e}")
            time.sleep(CFG["periodic_sweep_sec"])

# ---------- WS ----------
_last_msg_ts = 0.0
_ws_lock = threading.Lock()
_ws_obj: Optional[WebSocket] = None
_stop_ws = False

def _update_heartbeat():
    global _last_msg_ts
    _last_msg_ts = time.time()

def ws_private_handler(message):
    _update_heartbeat()
    try:
        if not isinstance(message, dict):
            return
        topic = message.get("topic", "")
        data = message.get("data", [])
        if not data:
            return

        if "execution" in topic or "order" in topic:
            for ev in data:
                symbol = ev.get("symbol")
                status = (ev.get("orderStatus") or ev.get("execType") or "").lower()
                reduce_only = str(ev.get("reduceOnly", "")).lower()
                if not (("filled" in status or "trade" in status) and ("true" in reduce_only or reduce_only == "")):
                    continue
                with STATE.lock:
                    accounts = [ak for (ak, sym) in STATE.pos.keys() if sym == symbol]
                for acct_key in accounts:
                    _extra = extra_for_uid(account_key_to_uid(acct_key) if acct_key.startswith("sub:") else None)
                    handle_tp_fill(acct_key, _extra, ev)

        if "position" in topic:
            for p in data:
                symbol = p.get("symbol") or ""
                size = Decimal(p.get("size") or "0")
                if not symbol:
                    continue
                side_word = "long" if (p.get("side", "").lower().startswith("b")) else "short"
                entry = Decimal(p.get("avgPrice") or "0")
                pos_idx = int(p.get("positionIdx") or 0)

                for acct_key, _extra in account_iter():
                    if size == 0:
                        with STATE.lock:
                            if (acct_key, symbol) in STATE.pos:
                                tg_send(f"🏁 {acct_key}:{symbol} position closed.", priority="info")
                                log_event("tp_manager", "pos_closed", symbol, account_key_to_uid(acct_key))
                            STATE.pos.pop((acct_key, symbol), None)
                        continue
                    place_or_sync_ladder(acct_key, _extra, symbol, side_word, entry, size, pos_idx)
    except Exception as e:
        log.error(f"ws handler error: {e}")

def _spawn_ws():
    global _ws_obj
    try:
        with _ws_lock:
            _ws_obj = WebSocket(
                testnet=(BYBIT_ENV == "testnet"),
                channel_type="private",
                api_key=BYBIT_KEY,
                api_secret=BYBIT_SECRET,
                ping_interval=CFG["ws_ping_interval"],
                ping_timeout=CFG["ws_ping_timeout"]
            )
            _update_heartbeat()
            _ws_obj.order_stream(callback=ws_private_handler)
            _ws_obj.position_stream(callback=ws_private_handler)
            _ws_obj.execution_stream(callback=ws_private_handler)
            log.info("Subscribed to private streams.")
            log_event("tp_manager", "ws_subscribed", "", "MAIN")
    except Exception as e:
        log.error(f"WS spawn failed: {e}; disabling WS and relying on HTTP sweep.")
        CFG["ws_optional"] = True
        tg_send("⚠️ TP/SL Manager: WS spawn failed; falling back to HTTP-only mode.", priority="warn")
        log_event("tp_manager", "ws_spawn_fail", "", "MAIN", {"error": str(e)}, level="error")

def _close_ws():
    global _ws_obj
    with _ws_lock:
        try:
            if _ws_obj and hasattr(_ws_obj, "exit"):
                _ws_obj.exit()
        except Exception:
            pass
        _ws_obj = None

def _ws_watchdog():
    backoff = CFG["ws_backoff_start"]
    while not _stop_ws:
        now = time.time()
        silent = now - _last_msg_ts if _last_msg_ts else 0
        if silent > CFG["ws_silence_watchdog_secs"]:
            log.error("WS silent too long; reconnecting...")
            _close_ws()
            try:
                _spawn_ws()
                backoff = CFG["ws_backoff_start"]
            except Exception as e:
                log.error(f"WS respawn failed: {e}")
                sleep_for = min(backoff, CFG["ws_backoff_max"])
                time.sleep(sleep_for)
                backoff = min(CFG["ws_backoff_max"], max(backoff * 2, CFG["ws_backoff_start"]))
                continue
        time.sleep(1.0)

# ---------- main ----------
def bootstrap():
    tg_send(f"🟢 TP/SL Manager online • dry_run={TP_DRY_RUN} grace={TP_GRACE_SEC}s adopt={TP_ADOPT_EXISTING} tag={TP_TAG} ws_optional={CFG['ws_optional']} sweep={CFG['periodic_sweep_sec']}s", priority="success")
    log_event("tp_manager", "startup", "", "MAIN", {
        "dry_run": TP_DRY_RUN, "grace_sec": TP_GRACE_SEC, "adopt": TP_ADOPT_EXISTING, "tag": TP_TAG,
        "ws_optional": CFG["ws_optional"], "sweep_sec": CFG["periodic_sweep_sec"]
    })
    log.info("TP/SL Manager started (5 equal TPs, hardened guards)")

    load_positions_once()

    if not CFG["ws_optional"]:
        _spawn_ws()
        threading.Thread(target=_ws_watchdog, name="ws-watchdog", daemon=True).start()
    else:
        log.warning("WS disabled by config/env; using HTTP sweep only.")

    threading.Thread(target=periodic_sweep_loop, name="sweep", daemon=True).start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        global _stop_ws
        _stop_ws = True
        _close_ws()
        log.info("Shutting down.")
        log_event("tp_manager", "shutdown", "", "MAIN")

if __name__ == "__main__":
    bootstrap()
