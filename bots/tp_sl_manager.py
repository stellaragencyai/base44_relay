#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (v2.1)
Equal-R ladder, adopt-once grace, breaker-aware, ownership-aware, decision-logged.

What this does (fast version)
- Keeps a single protective SL (position trading-stop) and a 5-rung reduce-only TP ladder.
- Equal-R ladder: targets at R_START, R_START+R_STEP, ... from entry vs stop.
- Adopts foreign rungs during warmup/adopt window; manages only “ours” after.
- Breaker ON: enforce reduce-only, cancel entry-type orders, keep SL; pause new TP placement.
- Emits structured events to decision_log.
- Maker-first shading so PostOnly rests; falls back only if you remove POST_ONLY yourself.

Key settings (env or core.settings)
  TPSL_ENABLED=true
  TPSL_POLL_SEC=4

  TP_RUNGS=5
  TP_EQUAL_R_START=0.5
  TP_EQUAL_R_STEP=0.5
  TP_RUNG_SIZE_PCT=20               # 5 rungs x 20% each; auto-normalized if changed

  TP_POST_ONLY=true
  TP_SPREAD_OFFSET_RATIO=0.35
  TP_MAX_MAKER_OFFSET_TICKS=5
  TP_FALLBACK_OFFSET_TICKS=2

  TP_ADOPT_EXISTING=true
  TP_CANCEL_NON_B44=false
  TP_STARTUP_GRACE_SEC=20
  STARTUP_WARMUP_SEC=20             # SL-only during warmup

  TP_SL_TF=5
  TP_SL_LOOKBACK=120
  TP_SL_SWING_WIN=20
  TP_SL_ATR_MULT_FALLBACK=0.45
  TP_SL_ATR_BUFFER=0.08
  TP_STOP_TRAIL=false               # tighten SL if structure/ATR improves

  TP_MAX_ORDERS_PER_SYMBOL=12
  TP_SYMBOL_WHITELIST=BTCUSDT,ETHUSDT
  TP_DRY_RUN=true

Ownership & tags
  TP_MANAGED_TAG=B44
  OWNERSHIP_ENFORCED=true
  MANAGE_UNTAGGED=false
  OWNERSHIP_SUB_UID=260417078
  OWNERSHIP_STRATEGY=A2
"""

from __future__ import annotations
import json
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Dict, List, Optional, Tuple

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from tools.notifier_telegram import tg
from core.guard import guard_blocking_reason

# Optional: structured event log
try:
    from core.decision_log import log_event
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass

# ---------- logging ----------
log = bind_context(get_logger("bots.tp_sl_manager"), comp="tpsl")
getcontext().prec = 28

# ---------- config ----------
TP_ADOPT_EXISTING = str(getattr(settings, "TP_ADOPT_EXISTING", "true")).lower() in ("1","true","yes","on")
TP_CANCEL_NON_B44 = str(getattr(settings, "TP_CANCEL_NON_B44", "false")).lower() in ("1","true","yes","on")
TP_DRY_RUN        = str(getattr(settings, "TP_DRY_RUN", "true")).lower() in ("1","true","yes","on")
TP_TAG            = (str(getattr(settings, "TP_MANAGED_TAG", "B44")).strip() or "B44")[:12]
POLL_SEC          = int(getattr(settings, "TPSL_POLL_SEC", 4))
GRACE_SEC         = int(getattr(settings, "TP_STARTUP_GRACE_SEC", 20))
WARMUP_SEC        = int(getattr(settings, "STARTUP_WARMUP_SEC", GRACE_SEC))

RUNGS             = max(1, int(getattr(settings, "TP_RUNGS", 5)))
R_START           = Decimal(str(getattr(settings, "TP_EQUAL_R_START", 0.5)))
R_STEP            = Decimal(str(getattr(settings, "TP_EQUAL_R_STEP", 0.5)))
RUNG_SIZE_PCT     = float(getattr(settings, "TP_RUNG_SIZE_PCT", 20.0))  # evenly split; normalized

POST_ONLY         = str(getattr(settings, "TP_POST_ONLY", "true")).lower() in ("1","true","yes","on")
SPREAD_RATIO      = float(getattr(settings, "TP_SPREAD_OFFSET_RATIO", 0.35))
MAX_OFFSET_TICKS  = max(1, int(getattr(settings, "TP_MAX_MAKER_OFFSET_TICKS", 5)))
FALLBACK_OFFSET   = max(1, int(getattr(settings, "TP_FALLBACK_OFFSET_TICKS", 2)))

SL_TF             = str(getattr(settings, "TP_SL_TF", 5))
SL_LOOKBACK       = max(30, int(getattr(settings, "TP_SL_LOOKBACK", 120)))
SL_SWING_WIN      = max(5, int(getattr(settings, "TP_SL_SWING_WIN", 20)))
SL_ATR_MULT_FB    = float(getattr(settings, "TP_SL_ATR_MULT_FALLBACK", 0.45))
SL_ATR_BUF        = float(getattr(settings, "TP_SL_ATR_BUFFER", 0.08))
STOP_TRAIL        = str(getattr(settings, "TP_STOP_TRAIL", "false")).lower() in ("1","true","yes","on")

OWNERSHIP_ENFORCED = str(getattr(settings, "OWNERSHIP_ENFORCED", "true")).lower() in ("1","true","yes","on")
MANAGE_UNTAGGED    = str(getattr(settings, "MANAGE_UNTAGGED", "false")).lower() in ("1","true","yes","on")
OWNERSHIP_SUB_UID  = str(getattr(settings, "OWNERSHIP_SUB_UID", "")).strip()
OWNERSHIP_STRAT    = str(getattr(settings, "OWNERSHIP_STRATEGY", "")).strip()

HTTP_TIMEOUT_S    = int(getattr(settings, "HTTP_TIMEOUT_S", 10))
BYBIT_PUBLIC      = (getattr(settings, "BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/"))

MAX_ORDERS_PER_SYMBOL = max(6, int(getattr(settings, "TP_MAX_ORDERS_PER_SYMBOL", 12)))
SYMBOL_WHITELIST  = [s.strip().upper() for s in str(getattr(settings, "TP_SYMBOL_WHITELIST", "") or "").split(",") if s.strip()]

TPSL_ENABLED      = str(getattr(settings, "TPSL_ENABLED", "true")).lower() in ("1","true","yes","on")

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

# ---------- order tag helpers ----------
def _session_id() -> str:
    return (settings.SESSION_ID if hasattr(settings, "SESSION_ID") else None) or \
           (time.strftime("%Y%m%dT%H%M%S", time.gmtime()))

def _build_owner_tag() -> str:
    try:
        from core.order_tag import build_tag
        return build_tag(OWNERSHIP_SUB_UID or "sub", OWNERSHIP_STRAT or "strat", _session_id())
    except Exception:
        base = TP_TAG if TP_TAG else "B44"
        sub  = OWNERSHIP_SUB_UID or "sub"
        st   = OWNERSHIP_STRAT or "strat"
        return f"{base}:{sub}:{st}:{_session_id()}"

OWNER_TAG = _build_owner_tag()

def _attach_link_id(base: str) -> str:
    try:
        from core.order_tag import attach_to_client_order_id
        return attach_to_client_order_id(base, OWNER_TAG)
    except Exception:
        base_clean = (base or "B44").replace(" ", "")[:24]
        tail = OWNER_TAG.replace(":", "-")
        return f"{base_clean}|{tail}"[:64]

def _link_is_ours(link: Optional[str]) -> bool:
    if not link:
        return False
    s = str(link)
    return (TP_TAG in s) or (OWNER_TAG.split(":")[0] in s)

# ---------- public HTTP helpers ----------
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

# ---------- SL helpers ----------
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
    trs: List[Decimal] = []
    prev_close: Optional[Decimal] = None
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

def _pick_tighter(current: Decimal, candidate: Decimal, side_word: str) -> Decimal:
    # For longs, tighter means higher; for shorts, lower
    if side_word == "long":
        return max(current, candidate)
    return min(current, candidate)

def ensure_stop(symbol: str, side_word: str, entry: Decimal, pos_idx: int, tick: Decimal) -> Decimal:
    """Ensure a position-level stopLoss. Optionally trail toward a tighter level."""
    current_sl: Optional[Decimal] = None
    try:
        ok, data, _ = by.get_positions(category="linear", symbol=symbol)
        if ok:
            for p in (data.get("result") or {}).get("list") or []:
                if int(p.get("positionIdx") or 0) == int(pos_idx):
                    cur = p.get("stopLoss")
                    if cur:
                        v = Decimal(str(cur))
                        if v > 0:
                            current_sl = round_to_tick(v, tick)
    except Exception:
        pass

    s_struct = _structure_stop(symbol, side_word, entry, tick)
    s_atr    = _atr_fallback_stop(symbol, side_word, entry, tick)

    desired = s_struct if s_struct is not None else s_atr
    if desired is None:
        # minimal buffer if all else fails
        desired = round_to_tick(entry - tick*Decimal(2) if side_word == "long" else entry + tick*Decimal(2), tick)

    if current_sl is not None:
        target = _pick_tighter(current_sl, desired, side_word) if STOP_TRAIL else current_sl
    else:
        target = desired

    if TP_DRY_RUN:
        log_event("tpsl", "sl_eval", symbol, "MAIN", {"entry": float(entry), "pos_idx": pos_idx, "current": float(current_sl or 0), "target": float(target)})
        return target

    if (current_sl is None) or (STOP_TRAIL and target != current_sl):
        body = {"category":"linear","symbol":symbol,"positionIdx":pos_idx,"stopLoss":str(target)}
        ok, _, err = by._request_private_json("/v5/position/trading-stop", body=body, method="POST")
        if not ok:
            log.warning("set SL failed %s: %s", symbol, err)
            log_event("tpsl", "sl_set_fail", symbol, "MAIN", {"pos_idx": pos_idx, "target": float(target), "err": str(err)}, level="warn")
        else:
            tg_send(f"🛡️ {symbol} SL set {target}")
            log_event("tpsl", "sl_set_ok", symbol, "MAIN", {"pos_idx": pos_idx, "target": float(target)})
    return target

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
    return _link_is_ours(link)

def make_link(base: str = "tp") -> str:
    base = base if base.startswith(TP_TAG) else f"{TP_TAG}-{base}"
    return _attach_link_id(base)[:64]

_t0 = time.monotonic()
def in_grace() -> bool:
    return (time.monotonic() - _t0) < max(0, GRACE_SEC)

def in_warmup() -> bool:
    return (time.monotonic() - _t0) < max(0, WARMUP_SEC)

def _allowed_symbol(sym: str) -> bool:
    return not SYMBOL_WHITELIST or sym.upper() in SYMBOL_WHITELIST

# ---------- ownership detection ----------
def _position_owned(symbol: str, pos_row: dict) -> bool:
    for k in ("positionTag", "comment", "lastOrderLinkId", "last_exec_link_id"):
        v = pos_row.get(k)
        if v and _link_is_ours(str(v)):
            return True
    try:
        ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
        if ok:
            for it in (data.get("result") or {}).get("list") or []:
                if str(it.get("reduceOnly","")).lower() not in ("true","1"):
                    continue
                if _link_is_ours(it.get("orderLinkId")):
                    return True
    except Exception:
        pass
    return MANAGE_UNTAGGED

# ---------- order ops ----------
def list_all_open_orders(symbol: str) -> List[dict]:
    ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
    if not ok:
        log.warning("open_orders err %s: %s", symbol, err)
        return []
    return (data.get("result") or {}).get("list") or []

def fetch_open_tp_orders(symbol: str, close_side: str) -> List[dict]:
    rows = list_all_open_orders(symbol)
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

def _enforce_order_cap(symbol: str, desired_links: List[str], ours: List[dict]) -> None:
    """Cancel any extras beyond cap that we own and that aren't desired."""
    if MAX_ORDERS_PER_SYMBOL <= 0:
        return
    owned = [o for o in ours if managed_link(o.get("orderLinkId"))]
    extras = [o for o in owned if str(o.get("orderLinkId") or "") not in set(desired_links)]
    # cancel oldest first
    for o in sorted(extras, key=lambda r: str(r.get("createdTime") or ""))[:max(0, len(owned) - MAX_ORDERS_PER_SYMBOL)]:
        cancel_order(symbol, o.get("orderId"), o.get("orderLinkId"))

def place_limit_reduce(symbol: str, side: str, price: Decimal, qty: Decimal, tick: Decimal) -> Optional[str]:
    off = adaptive_offset_ticks(symbol, tick)
    px = price + tick*off if side == "Sell" else price - tick*off
    qtxt = f"{qty.normalize()}"
    ptxt = f"{px.normalize()}"

    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: {side} {symbol} qty={qtxt} @ {ptxt}")
        log_event("tpsl", "tp_place_dry", symbol, "MAIN", {"side": side, "qty": float(qty), "px": float(px)})
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
        log_event("tpsl", "tp_place_fail", symbol, "MAIN", {"err": str(err), "qty": float(qty), "px": float(px)}, level="warn")
        return None
    oid = (data.get("result") or {}).get("orderId")
    log_event("tpsl", "tp_place_ok", symbol, "MAIN", {"orderId": oid, "qty": float(qty), "px": float(px)})
    return oid

def cancel_order(symbol: str, order_id: str, link_id: Optional[str]) -> None:
    if in_grace():
        tg_send(f"🔒 Cancel blocked by grace: {order_id}")
        return
    if (not managed_link(link_id)) and (not TP_CANCEL_NON_B44):
        tg_send(f"🔒 Keep foreign order (non-Base44): {order_id}")
        return
    if TP_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: cancel orderId={order_id}")
        log_event("tpsl", "cancel_dry", symbol, "MAIN", {"orderId": order_id})
        return
    ok, _, err = by.cancel_order(category="linear", symbol=symbol, orderId=order_id)
    if not ok:
        log.warning("cancel_order fail %s: %s", symbol, err)
        log_event("tpsl", "cancel_fail", symbol, "MAIN", {"orderId": order_id, "err": str(err)}, level="warn")
    else:
        log_event("tpsl", "cancel_ok", symbol, "MAIN", {"orderId": order_id})

# ---------- grid construction ----------
def _normalize_weights(n: int, each_pct: float) -> List[Decimal]:
    raw = [max(0.0, float(each_pct)) for _ in range(n)]
    s = sum(raw) or 1.0
    return [Decimal(x / s) for x in raw]

def split_even(total: Decimal, step: Decimal, minq: Decimal, n: int) -> List[Decimal]:
    if n <= 0 or total <= 0:
        return [Decimal("0")] * max(0, n)
    weights = _normalize_weights(n, RUNG_SIZE_PCT)
    chunks = [round_to_step(total * w, step) for w in weights]

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

    for i in range(n):
        if 0 < chunks[i] < minq:
            chunks[i] = Decimal("0")
    return chunks

def build_equal_r_targets(entry: Decimal, stop: Decimal, side_word: str, tick: Decimal) -> List[Decimal]:
    targets: List[Decimal] = []
    r_value = abs(entry - stop)
    if r_value <= 0:
        r_value = tick * Decimal(3)
    for i in range(RUNGS):
        dist_R = R_START + Decimal(i) * R_STEP
        raw_px = entry + dist_R * r_value if side_word == "long" else entry - dist_R * r_value
        targets.append(round_to_tick(raw_px, tick))
    return targets

# ---------- breaker behavior ----------
def enforce_reduce_only_and_cancel_entries(symbol: str) -> None:
    try:
        orders = list_all_open_orders(symbol)
    except Exception as e:
        log.warning("list_open_orders failed %s: %s", symbol, e)
        return

    for o in orders:
        try:
            reduce_only = str(o.get("reduceOnly", "0")).lower() in {"1", "true"}
            ord_type = str(o.get("orderType", "")).upper()
            oid  = o.get("orderId")
            qty  = Decimal(str(o.get("qty", "0") or "0"))

            if not reduce_only and ord_type in {"LIMIT", "MARKET"} and qty > 0:
                if TP_DRY_RUN:
                    tg_send(f"✋ DRY_RUN breaker: cancel entry {symbol} {o.get('side')} qty={qty}")
                    log_event("tpsl", "breaker_cancel_dry", symbol, "MAIN", {"orderId": oid})
                else:
                    ok, _, err = by.cancel_order(category="linear", orderId=oid, symbol=symbol)
                    if not ok:
                        log.warning("cancel entry failed %s: %s", symbol, err)
                        log_event("tpsl", "breaker_cancel_fail", symbol, "MAIN", {"orderId": oid, "err": str(err)}, level="warn")
                    else:
                        log_event("tpsl", "breaker_cancel_ok", symbol, "MAIN", {"orderId": oid})
                continue

            if not reduce_only:
                if TP_DRY_RUN:
                    tg_send(f"🔒 DRY_RUN breaker: set reduce-only on {symbol} order {oid}")
                    log_event("tpsl", "breaker_ro_dry", symbol, "MAIN", {"orderId": oid})
                else:
                    ok, _, err = by.amend_order(category="linear", orderId=oid, reduceOnly=True)
                    if not ok:
                        log.warning("amend reduce-only failed %s: %s", symbol, err)
                        log_event("tpsl", "breaker_ro_fail", symbol, "MAIN", {"orderId": oid, "err": str(err)}, level="warn")
                    else:
                        log_event("tpsl", "breaker_ro_ok", symbol, "MAIN", {"orderId": oid})
        except Exception as e:
            log.warning("breaker RO enforcement err %s: %s", symbol, e)

# ---------- core sync ----------
def place_or_sync_ladder(symbol: str, side_word: str, entry: Decimal, qty: Decimal, pos_idx: int) -> None:
    filters = get_symbol_filters(symbol)
    tick, step, minq = filters.tick, filters.step, filters.min_qty
    close_side = side_to_close(side_word)

    # Always ensure SL first
    stop = ensure_stop(symbol, side_word, entry, pos_idx, tick)

    blocked, why = guard_blocking_reason()
    if blocked:
        enforce_reduce_only_and_cancel_entries(symbol)
        tg_send(f"⛔ Breaker ON • {symbol} • SL at {stop} • TPs paused ({why})")
        log_event("tpsl", "paused_breaker", symbol, "MAIN", {"stop": float(stop), "reason": why}, level="warn")
        return

    if in_warmup():
        tg_send(f"🧷 Warmup • {symbol} • SL at {stop} • TPs paused")
        log_event("tpsl", "paused_warmup", symbol, "MAIN", {"stop": float(stop)})
        return

    targets = build_equal_r_targets(entry, stop, side_word, tick)
    target_chunks = split_even(qty, step, minq, RUNGS)
    existing = fetch_open_tp_orders(symbol, close_side)

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
    desired_links: List[str] = []
    placed = 0

    # enforce cap on total orders first (extras that are ours and not desired will be cancelled)
    _enforce_order_cap(symbol, [], existing)

    for i, (tpx, tq) in enumerate(zip(targets, target_chunks)):
        ex = matched[i]
        ex_id = ex.get("orderId") if ex else None
        ex_link = ex.get("orderLinkId") if ex else None

        if tq <= 0:
            if ex_id:
                cancel_order(symbol, ex_id, ex_link)
            continue

        if not ex_id:
            # still respect overall cap
            if len(existing) + placed >= MAX_ORDERS_PER_SYMBOL:
                log_event("tpsl", "tp_skip_cap", symbol, "MAIN", {"target": float(tpx), "qty": float(tq)})
                continue
            oid = place_limit_reduce(symbol, close_side, tpx, tq, tick)
            if oid:
                placed += 1
            continue

        desired_links.append(str(ex_link or ""))

        if (not managed_link(ex_link)) and adopt_only:
            # adopt foreign rung as-is during grace/adopt window
            continue

        try:
            cur_px = Decimal(str(ex.get("price")))
            cur_qty = Decimal(str(ex.get("qty")))
        except Exception:
            cur_px, cur_qty = tpx, tq

        if abs(cur_px - tpx) > tol or abs(cur_qty - tq) >= step:
            cancel_order(symbol, ex_id, ex_link)
            place_limit_reduce(symbol, close_side, tpx, tq, tick)

    # cap clean-up (after placements)
    _enforce_order_cap(symbol, desired_links, existing)

    tg_send(f"✅ {symbol} ladder sync • qty={qty} • entry={entry} • stop={stop}\nTPs: {', '.join(str(x) for x in targets)}")
    log_event("tpsl", "ladder_sync", symbol, "MAIN",
              {"qty": float(qty), "entry": float(entry), "stop": float(stop), "targets": [float(x) for x in targets],
               "rungs": RUNGS, "maker": POST_ONLY})

# ---------- sweep loop ----------
def _side_word_from_row(p: dict) -> Optional[str]:
    try:
        side_raw = (p.get("side") or "").lower()
        if side_raw.startswith("b"): return "long"
        if side_raw.startswith("s"): return "short"
    except Exception:
        pass
    try:
        sz = Decimal(p.get("size") or "0")
        if sz > 0:  return "long"
        if sz < 0:  return "short"
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

            if OWNERSHIP_ENFORCED and not _position_owned(symbol, p):
                tg_send(f"🔎 SKIP untagged {symbol} (ownership enforced)")
                continue

            place_or_sync_ladder(symbol, side_word, entry, abs(size), pos_idx)
        except Exception as e:
            log.warning("sweep row error: %s row=%s", e, p)

def main() -> None:
    if not TPSL_ENABLED:
        log.info("TP/SL Manager disabled via TPSL_ENABLED")
        return
    tg_send(
        f"🟢 TP/SL Manager online • dry={TP_DRY_RUN} grace={GRACE_SEC}s warmup={WARMUP_SEC}s "
        f"adopt={TP_ADOPT_EXISTING} tag={TP_TAG} owner={OWNER_TAG} sweep={POLL_SEC}s "
        f"maker={POST_ONLY} enforce_own={OWNERSHIP_ENFORCED} maxOrders={MAX_ORDERS_PER_SYMBOL}"
    )
    log_event("tpsl", "startup", "", "MAIN",
              {"dry": TP_DRY_RUN, "grace": GRACE_SEC, "warmup": WARMUP_SEC, "rungs": RUNGS,
               "maker": POST_ONLY, "max_orders": MAX_ORDERS_PER_SYMBOL})
    # Bootstrap immediately, then loop
    try:
        sweep_once()
    except Exception as e:
        log.warning("initial sweep error: %s", e)
    while True:
        try:
            time.sleep(max(2, POLL_SEC))
            sweep_once()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("loop error: %s", e)
            time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
