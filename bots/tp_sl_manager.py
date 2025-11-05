#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (5 equal TPs + auto-resize on scale-in + laddered trailing SL)

- 5 TP rungs, equal R spacing (default 0.5R steps up to 2.5R), equal size per rung.
- On position size change (scale-in), immediately re-sizes all 5 rungs so sum(qty)=position size.
- Shorts fixed: symmetric logic, close-side Buy, periodic sweep + WS.
- Trailing SL: TP2 → SL=TP1; TP3 → SL=TP2; TP4 → SL=TP3; TP5 → SL=TP4.
- SL tighter: chooses closer of structure-stop vs ATR-stop, smaller ATR multiple, smaller tick buffer.
- Maker-only reduce-only; WS watchdog; USDT/USDC settle scan.

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

getcontext().prec = 28

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tp5eq")

# ---------- env/root ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=REPO_ROOT / ".env")

STATE_DIR = REPO_ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- config ----------
CFG = {
    # Equal-R TP grid (exactly 5 rungs)
    "tp_rungs": 5,
    "tp_equal_r_start": 0.5,   # first TP at 0.5R
    "tp_equal_r_step": 0.5,    # step between rungs
    "tp_equal_alloc": 1.0,     # total allocation across rungs; split equally

    # maker placement
    "maker_post_only": True,
    "spread_offset_ratio": 0.35,
    "max_maker_offset_ticks": 5,
    "fallback_maker_offset_ticks": 2,
    "price_band_retry": True,
    "price_band_max_retries": 2,

    # SL logic (tightened)
    "sl_use_closer_of_structure_or_atr": True,  # pick the closer stop to entry
    "sl_atr_mult_fallback": 0.45,               # closer ATR baseline (was 1.0)
    "sl_atr_buffer": 0.08,                      # smaller structure cushion (was 0.2)
    "sl_kline_tf": "5",
    "sl_kline_count": 120,
    "sl_swing_lookback": 20,
    "sl_min_tick_buffer": 2,                    # was 3

    # BE / laddered SL trigger point (we now use rung laddering; BE no longer used)
    "be_after_tp_index": 2,
    "be_buffer_ticks": 1,

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
}

# ---------- env / clients ----------
BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_ENV = os.getenv("BYBIT_ENV", "mainnet").lower().strip()
SUB_UIDS = [s.strip() for s in os.getenv("SUB_UIDS", "").split(",") if s.strip()]
if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env at project root.")

http = HTTP(testnet=(BYBIT_ENV == "testnet"), api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

def _tg_send(payload: dict):
    if not (TG_TOKEN and TG_CHAT): return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", json=payload, timeout=6)
    except Exception as e:
        log.warning(f"telegram/error: {e}")

def send_tg(msg: str):
    if not (TG_TOKEN and TG_CHAT): return
    if len(msg) <= 3900:
        _tg_send({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"})
    else:
        s = msg
        while s:
            chunk = s[:3900]
            s = s[3900:]
            _tg_send({"chat_id": TG_CHAT, "text": chunk, "parse_mode": "HTML"})

# --- breaker flag ---
_BREAKER_FILE = STATE_DIR / "risk_state.json"
def breaker_active() -> bool:
    try:
        if not _BREAKER_FILE.exists(): return False
        js = json.loads(_BREAKER_FILE.read_text(encoding="utf-8"))
        return bool(js.get("breach"))
    except Exception:
        return False

# ---------- precision / symbol meta ----------
@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_order_qty: Decimal
    max_leverage: int

def get_symbol_filters(symbol: str, extra: dict) -> SymbolFilters:
    res = http.get_instruments_info(category=CFG["category"], symbol=symbol, **extra)
    lst = res.get("result", {}).get("list", [])
    if not lst: raise RuntimeError(f"symbol info not found for {symbol}")
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
def get_orderbook_top(symbol: str, extra: dict) -> Optional[Tuple[Decimal, Decimal]]:
    try:
        ob = http.get_orderbook(category=CFG["category"], symbol=symbol, limit=1, **extra)
        r = ob.get("result", {})
        bids = r.get("b", []) or r.get("bids") or []
        asks = r.get("a", []) or r.get("asks") or []
        if not bids or not asks: return None
        return Decimal(str(bids[0][0])), Decimal(str(asks[0][0]))
    except Exception as e:
        log.warning(f"orderbook error {symbol}: {e}")
        return None

def adaptive_offset_ticks(symbol: str, tick: Decimal, extra: dict) -> int:
    ob = get_orderbook_top(symbol, extra)
    if not ob: return CFG["fallback_maker_offset_ticks"]
    bid, ask = ob
    spread = max(Decimal("0"), ask - bid)
    if spread <= 0: return 1
    spread_ticks = int((spread / tick).to_integral_value(rounding=ROUND_DOWN))
    base = max(1, round(spread_ticks * CFG["spread_offset_ratio"]))
    return int(min(max(base, 1), CFG["max_maker_offset_ticks"]))

# ---------- helpers ----------
def side_to_close(position_side_word: str) -> str:
    # close longs with Sell, shorts with Buy
    return "Sell" if position_side_word.lower().startswith("l") else "Buy"

def _compute_structure_stop(symbol: str, side_word: str, entry: Decimal, tick: Decimal, extra: dict) -> Optional[Decimal]:
    try:
        res = http.get_kline(category=CFG["category"], symbol=symbol, interval=CFG["sl_kline_tf"], limit=CFG["sl_kline_count"], **extra)
        arr = (res.get("result") or {}).get("list") or []
        if not arr: return None
        lows, highs, trs = [], [], []
        prev_close = None
        for it in arr:
            o,h,l,c = map(Decimal, [it[1], it[2], it[3], it[4]])
            lows.append(l); highs.append(h)
            if prev_close is not None:
                trs.append(max(h-l, abs(h-prev_close), abs(l-prev_close)))
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
        if not arr: return None
        prev_close = None
        trs = []
        for it in arr:
            o,h,l,c = map(Decimal, [it[1], it[2], it[3], it[4]])
            if prev_close is not None:
                trs.append(max(h-l, abs(h-prev_close), abs(l-prev_close)))
            prev_close = c
        if len(trs) < 14: return None
        atr = sum(trs[-14:]) / Decimal(14)
        move = atr * Decimal(str(CFG["sl_atr_mult_fallback"]))
        stop = entry - move if side_word == "long" else entry + move
        return round_price_to_tick(stop, tick)
    except Exception:
        return None

def _pick_closer_stop(entry: Decimal, a: Optional[Decimal], b: Optional[Decimal], side_word: str, tick: Decimal) -> Decimal:
    """Choose the stop closer to entry; fallback to a small tick buffer if both missing."""
    choices = [x for x in (a, b) if x is not None]
    if choices:
        return min(choices, key=lambda s: abs(entry - s))
    # last resort: a couple ticks off entry
    return round_price_to_tick(entry - tick*Decimal(CFG["sl_min_tick_buffer"]) if side_word=="long"
                               else entry + tick*Decimal(CFG["sl_min_tick_buffer"]), tick)

def ensure_stop(symbol: str, side_word: str, entry: Decimal, pos_idx: int, tick: Decimal, extra: dict) -> Decimal:
    try:
        res = http.get_positions(category=CFG["category"], symbol=symbol, **extra)
        lst = res.get("result", {}).get("list", [])
        if lst and lst[0].get("stopLoss"):
            cur = Decimal(str(lst[0]["stopLoss"]))
            if cur > 0: return cur
    except Exception as e:
        log.debug(f"check SL failed: {e}")

    s_struct = _compute_structure_stop(symbol, side_word, entry, tick, extra)
    s_atr    = _compute_fallback_atr_stop(symbol, side_word, entry, tick, extra)
    stop = _pick_closer_stop(entry, s_struct, s_atr, side_word, tick)

    try:
        http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(stop), **extra)
        send_tg(f"🛑 <b>{symbol}</b>: Auto-SL set at {stop}.")
    except Exception as e:
        log.warning(f"set SL failed: {e}")
    return Decimal(stop)

def build_equal_r_targets(entry: Decimal, stop: Decimal, side_word: str, tick: Decimal) -> List[Decimal]:
    """Return exactly 5 target prices equally spaced in R; rounded to tick."""
    r_unit = abs(entry - stop)
    # guard against pathological zero-R
    if r_unit <= 0:
        r_unit = tick * Decimal(5)
    start = Decimal(str(CFG["tp_equal_r_start"]))
    step  = Decimal(str(CFG["tp_equal_r_step"]))
    levels = [start + step * i for i in range(CFG["tp_rungs"])]
    if side_word == "long":
        prices = [entry + l * r_unit for l in levels]
    else:
        prices = [entry - l * r_unit for l in levels]
    return [round_price_to_tick(p, tick) for p in prices]

# ---------- orders ----------
def place_limit_reduce(symbol: str, side: str, price: Decimal, qty: Decimal, tick: Decimal, extra: dict) -> Optional[str]:
    # For Sell (closing longs), shade below; for Buy (closing shorts), shade above
    maker_ticks = adaptive_offset_ticks(symbol, tick, extra)
    px = price - tick*maker_ticks if side=="Sell" else price + tick*maker_ticks
    body = dict(
        category=CFG["category"], symbol=symbol, side=side, orderType="Limit",
        qty=str(qty.normalize()), price=str(px.normalize()),
        timeInForce="PostOnly" if CFG["maker_post_only"] else "GoodTillCancel",
        reduceOnly=True
    )
    retries = CFG["price_band_max_retries"] if CFG["price_band_retry"] else 0
    for attempt in range(retries + 1):
        try:
            r = http.place_order(**body, **extra)
            if r.get("retCode") in (0, "0"):
                return (r.get("result") or {}).get("orderId")
            if attempt < retries:
                px = px - tick if side=="Sell" else px + tick
                body["price"] = str(px)
                time.sleep(0.2)
        except Exception as e:
            log.warning(f"place_order ex: {e}")
            if attempt < retries: time.sleep(0.3)
    return None

def amend_order_qty(order_id: str, qty: Decimal, extra: dict) -> bool:
    try:
        r = http.amend_order(category=CFG["category"], orderId=order_id, qty=str(qty.normalize()), **extra)
        return r.get("retCode") in (0, "0")
    except Exception as e:
        log.warning(f"amend qty ex: {e}")
        return False

def amend_order_price(order_id: str, price: Decimal, extra: dict) -> bool:
    try:
        r = http.amend_order(category=CFG["category"], orderId=order_id, price=str(price.normalize()), **extra)
        return r.get("retCode") in (0, "0")
    except Exception as e:
        log.warning(f"amend price ex: {e}")
        return False

def cancel_order(order_id: str, extra: dict) -> None:
    try:
        http.cancel_order(category=CFG["category"], orderId=order_id, **extra)
    except Exception:
        pass

def fetch_open_tp_orders(symbol: str, close_side: str, extra: dict) -> List[dict]:
    try:
        r = http.get_open_orders(category=CFG["category"], symbol=symbol, **extra)
        rows = (r.get("result") or {}).get("list") or []
        out = []
        for it in rows:
            try:
                if str(it.get("reduceOnly", "")).lower() != "true": continue
                if (it.get("side") or "") != close_side: continue
                if (it.get("orderType") or "") != "Limit": continue
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
        # state[(account_key, symbol)] = {..., "rungs": [ {"px": Decimal, "orderId": str, "qty": Decimal}, ... ]}
        self.pos: Dict[Tuple[str, str], Dict] = {}
        self.lock = threading.Lock()

STATE = PositionState()

def account_iter():
    yield ("main", {})
    for uid in SUB_UIDS:
        yield (f"sub:{uid}", {"subUid": uid})

# ---------- policy ----------
@dataclass
class PolicyCheck:
    allowed: bool
    reason: Optional[str]
    filters: Optional['SymbolFilters']

def symbol_allowed_by_policy(symbol: str, extra: dict) -> PolicyCheck:
    if CFG["symbol_blocklist"] and symbol.upper() in {s.upper() for s in CFG["symbol_blocklist"]}:
        return PolicyCheck(False, "blocked by symbol_blocklist", None)
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
    """Even split total into n chunks rounded to step; ensure each >= minq when possible."""
    if n <= 0: return []
    ideal = total / Decimal(n)
    chunks = [round_to_step(ideal, step) for _ in range(n)]
    # fix rounding residual
    diff = total - sum(chunks)
    # distribute residual by adding/subtracting one step
    if diff != 0:
        sgn = 1 if diff > 0 else -1
        diff_abs = abs(diff)
        while diff_abs >= step:
            for i in range(n):
                if diff_abs < step: break
                new_q = chunks[i] + (step if sgn>0 else -step)
                if new_q >= 0:
                    chunks[i] = new_q
                    diff_abs -= step
            if diff_abs < step: break
    # ensure minq where possible by stealing from others
    for i in range(n):
        if chunks[i] == 0: continue
        if chunks[i] < minq:
            need = minq - chunks[i]
            for j in range(n):
                if j==i: continue
                give = min(need, max(Decimal(0), chunks[j]-minq))
                if give > 0:
                    chunks[j] -= give
                    chunks[i] += give
                    need -= give
                if need <= 0: break
    return chunks

def place_or_sync_ladder(account_key: str, extra: dict, symbol: str, side_word: str, entry: Decimal, qty: Decimal, pos_idx: int):
    check = symbol_allowed_by_policy(symbol, extra)
    if not check.allowed:
        with STATE.lock:
            STATE.pos.setdefault((account_key, symbol), {})["tp_placed"] = True
        send_tg(f"⏸️ {account_key}:{symbol} skipped — {check.reason}")
        return

    if breaker_active():
        tick = check.filters.tick_size
        stop = ensure_stop(symbol, side_word, entry, pos_idx, tick, extra)
        with STATE.lock:
            STATE.pos.setdefault((account_key, symbol), {})["tp_placed"] = True
        send_tg(f"⛔ {account_key}:{symbol} breaker active — SL ensured at {stop}, TPs paused.")
        return

    tick, step, minq = check.filters.tick_size, check.filters.step_size, check.filters.min_order_qty
    stop = ensure_stop(symbol, side_word, entry, pos_idx, tick, extra)
    targets = build_equal_r_targets(entry, stop, side_word, tick)
    close_side = side_to_close(side_word)

    # target qty per rung
    target_chunks = _split_even(qty, step, minq, CFG["tp_rungs"])

    # fetch existing RO TPs
    existing = fetch_open_tp_orders(symbol, close_side, extra)

    # map existing by nearest target price (within 2 ticks)
    two_ticks = tick * 2
    matched = [None] * CFG["tp_rungs"]  # holds dicts
    used = set()
    for order in existing:
        try:
            px = Decimal(str(order.get("price")))
            for i, tpx in enumerate(targets):
                if i in used: continue
                if abs(px - tpx) <= two_ticks:
                    matched[i] = order
                    used.add(i)
                    break
        except Exception:
            continue

    # ensure each rung
    placed = 0
    rung_info = []
    for i, (tpx, tq) in enumerate(zip(targets, target_chunks)):
        if tq <= 0:
            # if there is an order there, cancel it
            if matched[i] and matched[i].get("orderId"):
                cancel_order(matched[i]["orderId"], extra)
            rung_info.append({"px": tpx, "qty": Decimal("0"), "orderId": None})
            continue

        if matched[i] is None or not matched[i].get("orderId"):
            oid = place_limit_reduce(symbol, close_side, tpx, tq, tick, extra)
            if oid: placed += 1
            rung_info.append({"px": tpx, "qty": tq, "orderId": oid})
        else:
            oid = matched[i]["orderId"]
            cur_px = Decimal(str(matched[i]["price"]))
            cur_qty = Decimal(str(matched[i]["qty"]))
            # amend price if drifted more than 1 tick from target
            if abs(cur_px - tpx) > tick:
                amend_order_price(oid, tpx, extra)
                cur_px = tpx
            # amend qty if off by >= one step
            if abs(cur_qty - tq) >= step:
                amend_order_qty(oid, tq, extra)
                cur_qty = tq
            rung_info.append({"px": cur_px, "qty": cur_qty, "orderId": oid})

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
    send_tg(f"✅ {account_key}:{symbol} ladder synced • qty={qty} • entry={entry} • stop={stop}\nTPs: {px_str}")

def set_sl(account_key: str, extra: dict, symbol: str, pos_idx: int, new_sl: Decimal):
    try:
        http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(new_sl), **extra)
        send_tg(f"🛡️ {account_key}:{symbol} SL → {new_sl}")
    except Exception as e:
        log.warning(f"set SL error {account_key}:{symbol}: {e}")

# ---------- fills: laddered SL ----------
def handle_tp_fill(account_key: str, extra: dict, ev: dict):
    symbol = ev.get("symbol")
    with STATE.lock:
        s = STATE.pos.get((account_key, symbol))
    if not s or not s.get("tp_placed"): return

    with STATE.lock:
        s["tp_filled"] = s.get("tp_filled", 0) + 1
        filled_count = s["tp_filled"]
        targets = s.get("targets", [])
        entry = Decimal(str(s["entry"]))
        side_word = s["side"]
        tick = s["tick"]
        pos_idx = s["pos_idx"]

    filled_px = ev.get("avgPrice") or ev.get("execPrice") or ev.get("price")
    send_tg(f"🎯 {account_key}:{symbol} TP filled {filled_count}/{CFG['tp_rungs']} @ {filled_px}")

    # Ladder SL to previous TP price:
    # TP2 -> SL = TP1; TP3 -> SL = TP2; TP4 -> SL = TP3; TP5 -> SL = TP4
    if targets and filled_count >= 2:
        prev_index = min(filled_count - 2, CFG["tp_rungs"] - 2)  # 2->0, 3->1, 4->2, 5->3
        if 0 <= prev_index < len(targets):
            ladder_price = Decimal(str(targets[prev_index]))
            set_sl(account_key, extra, symbol, pos_idx, ladder_price)

# ---------- bootstrap/sweep ----------
def load_positions_once():
    for acct_key, extra in account_iter():
        for coin in CFG["settle_coins"]:
            try:
                res = http.get_positions(category=CFG["category"], settleCoin=coin, **extra)
                arr = res.get("result", {}).get("list", []) or []
                for p in arr:
                    symbol = p.get("symbol") or ""
                    if not symbol: continue
                    if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]: continue
                    if CFG["symbol_blocklist"] and symbol.upper() in {s.upper() for s in CFG["symbol_blocklist"]}: continue
                    size = Decimal(p.get("size") or "0")
                    if size == 0: continue
                    side_word = "long" if (p.get("side","").lower().startswith("b")) else "short"
                    entry = Decimal(p.get("avgPrice") or "0")
                    pos_idx = int(p.get("positionIdx") or 0)
                    place_or_sync_ladder(acct_key, extra, symbol, side_word, entry, size, pos_idx)
            except Exception as e:
                log.error(f"load pos {acct_key} {coin} error: {e}")

def periodic_sweep_loop():
    while True:
        try:
            for acct_key, extra in account_iter():
                for coin in CFG["settle_coins"]:
                    try:
                        res = http.get_positions(category=CFG["category"], settleCoin=coin, **extra)
                        rows = (res.get("result") or {}).get("list") or []
                        for p in rows:
                            symbol = p.get("symbol") or ""
                            size = Decimal(p.get("size") or "0")
                            if size <= 0 or not symbol: continue
                            side_word = "long" if (p.get("side","").lower().startswith("b")) else "short"
                            entry = Decimal(p.get("avgPrice") or "0")
                            pos_idx = int(p.get("positionIdx") or 0)

                            with STATE.lock:
                                s = STATE.pos.get((acct_key, symbol))
                                prev_qty = s.get("qty") if s else None
                            # sync if new or size changed or ladder not yet placed
                            if s is None or prev_qty != size or not s.get("tp_placed"):
                                place_or_sync_ladder(acct_key, extra, symbol, side_word, entry, size, pos_idx)
                    except Exception as e:
                        log.warning(f"sweep {acct_key} {coin} err: {e}")
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
        if not isinstance(message, dict): return
        topic = message.get("topic", "")
        data = message.get("data", [])
        if not data: return

        # Execution/order fills
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
                    extra = {} if acct_key == "main" else {"subUid": acct_key.split(":")[1]}
                    handle_tp_fill(acct_key, extra, ev)

        # Position updates
        if "position" in topic:
            for p in data:
                symbol = p.get("symbol") or ""
                size = Decimal(p.get("size") or "0")
                if not symbol: continue
                side_word = "long" if (p.get("side","").lower().startswith("b")) else "short"
                entry = Decimal(p.get("avgPrice") or "0")
                pos_idx = int(p.get("positionIdx") or 0)

                for acct_key, extra in account_iter():
                    if size == 0:
                        with STATE.lock:
                            if (acct_key, symbol) in STATE.pos:
                                send_tg(f"🏁 {acct_key}:{symbol} position closed.")
                            STATE.pos.pop((acct_key, symbol), None)
                        continue

                    # always sync ladder on any update (covers scale-in)
                    place_or_sync_ladder(acct_key, extra, symbol, side_word, entry, size, pos_idx)

    except Exception as e:
        log.error(f"ws handler error: {e}")

def _spawn_ws():
    global _ws_obj
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
    send_tg("🟢 TP/SL Manager online (5 equal TPs; auto-resize on scale-in; tight SL).")
    log.info("TP/SL Manager started (5 equal TPs)")

    load_positions_once()

    _spawn_ws()
    threading.Thread(target=_ws_watchdog, name="ws-watchdog", daemon=True).start()
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

if __name__ == "__main__":
    bootstrap()
