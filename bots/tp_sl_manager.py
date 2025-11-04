#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (final + breaker-aware + 75x-ready)

Keeps your original features:
- Auto Stop Loss (structure-first, ATR fallback, last-resort tick buffer)
- 5 TP Fixed-R with regime-aware grids and allocations 15/20/25/30/35
- Spread-aware maker offset, reduce-only, post-only
- BE after TP2, then LADDERED SL: TP3->SL to TP2, TP4->SL to TP3, TP5->SL to TP4
- Optional multi-subaccount monitoring via SUB_UIDS
- .env loaded explicitly from project root to avoid path issues

Enhancements:
- Breaker-aware: if `.state/risk_state.json` says breach=true, we ENSURE SL but DO NOT place new TPs.
- Optional leverage policy: act only on symbols whose maxLeverage >= MIN_MAX_LEVERAGE (default off).
- Optional symbol blocklist.

Requires:
  pip install pybit python-dotenv requests pyyaml
.env in repo root:
  BYBIT_API_KEY=xxxx
  BYBIT_API_SECRET=xxxx
  BYBIT_ENV=mainnet          # or testnet
  TELEGRAM_BOT_TOKEN=1234:abc   # optional
  TELEGRAM_CHAT_ID=123456       # optional
  SUB_UIDS=111111,222222        # optional CSV for subs you manually trade

Optional policy to act ONLY on 75x instruments:
  REQUIRE_MIN_MAX_LEVERAGE=true
  MIN_MAX_LEVERAGE=75
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tp5")

# ---------- force-load .env from project root ----------
# bots/tp_sl_manager.py -> parents[1] is repo root
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

# ---------- config ----------
CFG = {
    # allocations and default R grid
    "tp_allocations": [0.15, 0.20, 0.25, 0.30, 0.35],
    "tp_levels_fixed_r_default": [0.5, 1.0, 1.5, 2.0, 3.0],

    # regime detection
    "regime_enabled": True,
    "regime_candle_tf": "5",
    "regime_candle_count": 120,
    "atr_period": 14,
    "adx_period": 14,
    "chop_atr_pct_max": 0.25,   # percent
    "chop_adx_max": 18.0,
    "trend_atr_pct_min": 0.60,  # percent
    "trend_adx_min": 25.0,
    "r_grid_chop":   [0.4, 0.8, 1.2, 1.6, 2.4],
    "r_grid_normal": [0.5, 1.0, 1.5, 2.0, 3.0],
    "r_grid_trend":  [0.7, 1.2, 1.8, 2.6, 3.8],

    # maker placement
    "reduce_only": True,
    "maker_post_only": True,
    "spread_offset_ratio": 0.35,
    "max_maker_offset_ticks": 5,
    "fallback_maker_offset_ticks": 2,

    "price_band_retry": True,
    "price_band_max_retries": 2,

    # qty rules
    "min_notional_carry_over": True,
    "fallback_single_tp_when_tiny": True,
    "single_tp_level_fixed_r": 1.0,

    # SL logic (auto)
    "sl_atr_mult_fallback": 1.0,
    "sl_atr_buffer": 0.2,
    "sl_kline_tf": "5",
    "sl_kline_count": 120,
    "sl_swing_lookback": 20,
    "sl_min_tick_buffer": 3,

    # BE and trailing (trailing kept but laddered SL will dominate)
    "be_after_tp_index": 2,
    "be_buffer_ticks": 1,
    "enable_trailing_after_be": True,
    "trail_check_interval_sec": 1.5,
    "trail_step_r": 0.5,
    "trail_start_atr_mult": 0.7,
    "trail_tighten_per_step": 0.1,
    "trail_tighten_max_atr_mult": 1.4,
    "trail_extra_be_tick_per_step": 1,
    "trail_max_be_buffer_ticks": 6,

    # market/category
    "category": "linear",

    # symbol handling
    "listen_symbols": "*",           # "*" = all symbols the account trades
    "symbol_blocklist": [],          # optional explicit excludes (["XYZUSDT", ...])

    # NEW: leverage policy
    "require_min_max_leverage": False,  # when True, only manage symbols whose maxLeverage >= min_max_leverage
    "min_max_leverage": 75,
}

# ---------- env / clients ----------
BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_ENV = os.getenv("BYBIT_ENV", "mainnet").lower().strip()
SUB_UIDS = [s.strip() for s in os.getenv("SUB_UIDS", "").split(",") if s.strip()]

# Allow env overrides for new leverage policy
if os.getenv("REQUIRE_MIN_MAX_LEVERAGE"):
    CFG["require_min_max_leverage"] = os.getenv("REQUIRE_MIN_MAX_LEVERAGE", "false").strip().lower() in ("1","true","yes","on")
if os.getenv("MIN_MAX_LEVERAGE"):
    try:
        CFG["min_max_leverage"] = int(os.getenv("MIN_MAX_LEVERAGE"))
    except Exception:
        pass

if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env at project root.")

if BYBIT_ENV == "testnet":
    http = HTTP(testnet=True, api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)
    ws_private = WebSocket(testnet=True, channel_type="private")
else:
    http = HTTP(testnet=False, api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)
    ws_private = WebSocket(testnet=False, channel_type="private")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

def send_tg(msg: str):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=6
        )
    except Exception as e:
        log.warning(f"telegram/error: {e}")

# --- breaker flag (shared with risk_daemon) ---
_BREAKER_FILE = Path(__file__).resolve().parents[1] / ".state" / "risk_state.json"

def breaker_active() -> bool:
    try:
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
def get_orderbook_top(symbol: str, extra: dict) -> Optional[Tuple[Decimal, Decimal]]:
    try:
        ob = http.get_orderbook(category=CFG["category"], symbol=symbol, limit=1, **extra)
        r = ob.get("result", {})
        bids = r.get("b", []) or r.get("bids") or []
        asks = r.get("a", []) or r.get("asks") or []
        if not bids or not asks:
            return None
        return Decimal(str(bids[0][0])), Decimal(str(asks[0][0]))
    except Exception as e:
        log.warning(f"orderbook error {symbol}: {e}")
        return None

def get_mark_price(symbol: str, extra: dict) -> Optional[Decimal]:
    try:
        res = http.get_tickers(category=CFG["category"], symbol=symbol, **extra)
        lst = res.get("result", {}).get("list", [])
        if not lst:
            return None
        return Decimal(lst[0]["markPrice"])
    except Exception as e:
        log.warning(f"mark price error {symbol}: {e}")
        return None

def get_klines(symbol: str, interval: str, limit: int, extra: dict) -> List[Dict]:
    try:
        res = http.get_kline(category=CFG["category"], symbol=symbol, interval=interval, limit=limit, **extra)
        lst = res.get("result", {}).get("list", [])
        out = []
        for it in lst:
            out.append({"t": int(it[0]), "o": float(it[1]), "h": float(it[2]), "l": float(it[3]), "c": float(it[4])})
        out.sort(key=lambda x: x["t"])
        return out
    except Exception as e:
        log.warning(f"kline error {symbol}: {e}")
        return []

def compute_atr(candles: List[Dict], period: int) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    prev_close = candles[0]["c"]
    for i in range(1, len(candles)):
        h, l, c_prev = candles[i]["h"], candles[i]["l"], prev_close
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)
        prev_close = candles[i]["c"]
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

def compute_adx(candles: List[Dict], period: int) -> Optional[float]:
    if len(candles) < period + 2:
        return None
    plus_dm, minus_dm, tr_list = [], [], []
    for i in range(1, len(candles)):
        up = candles[i]["h"] - candles[i-1]["h"]
        dn = candles[i-1]["l"] - candles[i]["l"]
        plus_dm.append(max(up, 0) if up > dn else 0)
        minus_dm.append(max(dn, 0) if dn > up else 0)
        tr_list.append(max(candles[i]["h"] - candles[i]["l"],
                           abs(candles[i]["h"] - candles[i-1]["c"]),
                           abs(candles[i]["l"] - candles[i-1]["c"])))
    if len(tr_list) < period: return None
    tr_n = sum(tr_list[-period:])
    if tr_n == 0: return None
    plus_di = 100 * (sum(plus_dm[-period:]) / tr_n)
    minus_di = 100 * (sum(minus_dm[-period:]) / tr_n)
    dx = 100 * abs(plus_di - minus_di) / max(plus_di + minus_di, 1e-9)
    return dx

# ---------- helpers ----------
def side_to_close(position_side: str) -> str:
    return "Sell" if position_side.lower() == "long" else "Buy"

def adaptive_offset_ticks(symbol: str, tick: Decimal, extra: dict) -> int:
    ob = get_orderbook_top(symbol, extra)
    if not ob:
        return CFG["fallback_maker_offset_ticks"]
    bid, ask = ob
    spread = max(Decimal("0"), ask - bid)
    if spread <= 0:
        return 1
    spread_ticks = int((spread / tick).to_integral_value(rounding=ROUND_DOWN))
    base = max(1, round(spread_ticks * CFG["spread_offset_ratio"]))
    return int(min(max(base, 1), CFG["max_maker_offset_ticks"]))

def compute_fixed_r_targets(entry: Decimal, stop: Decimal, side: str, r_levels: List[float]) -> List[Decimal]:
    r = abs(entry - stop)
    if side.lower() == "buy":
        return [entry + Decimal(str(m)) * r for m in r_levels]
    else:
        return [entry - Decimal(str(m)) * r for m in r_levels]

def allocate_sizes(total: Decimal, weights: List[float], step: Decimal, minq: Decimal) -> Tuple[List[Decimal], Decimal]:
    sizes, carry = [], Decimal("0")
    for w in weights:
        raw = total * Decimal(str(w)) + carry
        rounded = round_to_step(raw, step)
        if rounded < minq:
            carry += raw
            sizes.append(Decimal("0"))
        else:
            sizes.append(rounded)
            carry = raw - rounded
    rem = Decimal("0")
    if carry > 0:
        add = round_to_step(carry, step)
        if add > 0:
            sizes[-1] += add
            rem = carry - add
        else:
            rem = carry
    return sizes, rem

# ---------- stop logic ----------
def compute_structure_stop(symbol: str, side: str, entry: Decimal, tick: Decimal, extra: dict) -> Optional[Decimal]:
    candles = get_klines(symbol, CFG["sl_kline_tf"], CFG["sl_kline_count"], extra)
    if not candles:
        return None
    look = max(5, int(CFG["sl_swing_lookback"]))
    window = candles[-look:]
    lows = [c["l"] for c in window]; highs = [c["h"] for c in window]
    atr = compute_atr(candles, CFG["atr_period"]) or 0.0
    atr_buf = Decimal(str(atr)) * Decimal(str(CFG["sl_atr_buffer"]))
    if side == "long":
        stop = Decimal(str(min(lows))) - atr_buf
    else:
        stop = Decimal(str(max(highs))) + atr_buf
    return round_price_to_tick(stop, tick)

def compute_fallback_atr_stop(symbol: str, side: str, entry: Decimal, tick: Decimal, extra: dict) -> Optional[Decimal]:
    candles = get_klines(symbol, CFG["sl_kline_tf"], CFG["sl_kline_count"], extra)
    if not candles:
        return None
    atr = compute_atr(candles, CFG["atr_period"])
    if atr is None:
        return None
    move = Decimal(str(atr)) * Decimal(str(CFG["sl_atr_mult_fallback"]))
    stop = entry - move if side == "long" else entry + move
    return round_price_to_tick(stop, tick)

def last_resort_stop(side: str, entry: Decimal, tick: Decimal) -> Decimal:
    return round_price_to_tick(entry - tick * Decimal(str(CFG["sl_min_tick_buffer"])) if side == "long"
                               else entry + tick * Decimal(str(CFG["sl_min_tick_buffer"])), tick)

def ensure_stop(symbol: str, side: str, entry: Decimal, pos_idx: int, tick: Decimal, extra: dict) -> Decimal:
    try:
        res = http.get_positions(category=CFG["category"], symbol=symbol, **extra)
        lst = res.get("result", {}).get("list", [])
        if lst and lst[0].get("stopLoss"):
            cur = Decimal(str(lst[0]["stopLoss"]))
            if cur > 0:
                return cur
    except Exception as e:
        log.debug(f"check SL failed: {e}")

    stop = compute_structure_stop(symbol, side, entry, tick, extra)
    if stop is None:
        stop = compute_fallback_atr_stop(symbol, side, entry, tick, extra)
    if stop is None:
        stop = last_resort_stop(side, entry, tick)

    try:
        http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(stop), **extra)
        send_tg(f"🛑 <b>{symbol}</b>: Auto-SL set at {stop}.")
    except Exception as e:
        log.warning(f"set SL failed: {e}")
    return stop

# ---------- placement ----------
def place_tp(http_cli: HTTP, symbol: str, close_side: str, px: Decimal, qty: Decimal, tick: Decimal, extra: dict):
    px = round_price_to_tick(px, tick)
    maker_ticks = adaptive_offset_ticks(symbol, tick, extra)
    px = px - tick * maker_ticks if close_side == "Sell" else px + tick * maker_ticks
    body = dict(
        category=CFG["category"], symbol=symbol, side=close_side, orderType="Limit",
        qty=str(qty.normalize()), price=str(px.normalize()),
        timeInForce="PostOnly" if CFG["maker_post_only"] else "GoodTillCancel",
        reduceOnly=True
    )
    retries = CFG["price_band_max_retries"] if CFG["price_band_retry"] else 0
    for attempt in range(retries + 1):
        try:
            r = http_cli.place_order(**body, **extra)
            if r.get("retCode") == 0:
                return True
            if attempt < retries:
                px = px - tick if close_side == "Sell" else px + tick
                body["price"] = str(px)
                time.sleep(0.2)
        except Exception as e:
            log.warning(f"place_order ex: {e}")
            if attempt < retries: time.sleep(0.3)
    return False

# ---------- state ----------
class PositionState:
    def __init__(self):
        # key: (account_key, symbol)
        self.pos: Dict[Tuple[str, str], Dict] = {}
        self.lock = threading.Lock()

STATE = PositionState()

def account_iter():
    yield ("main", {})
    for uid in SUB_UIDS:
        yield (f"sub:{uid}", {"subUid": uid})

# ---------- leverage policy ----------
def symbol_allowed_by_policy(symbol: str, extra: dict) -> Tuple[bool, Optional[str], Optional[SymbolFilters]]:
    """Return (allowed, reason_if_blocked, filters)"""
    if CFG["symbol_blocklist"] and symbol.upper() in {s.upper() for s in CFG["symbol_blocklist"]}:
        return (False, "blocked by symbol_blocklist", None)
    try:
        filters = get_symbol_filters(symbol, extra)
    except Exception as e:
        return (False, f"no instrument meta: {e}", None)

    if CFG["require_min_max_leverage"]:
        need = int(CFG["min_max_leverage"])
        if filters.max_leverage < need:
            return (False, f"maxLeverage {filters.max_leverage} < required {need}", filters)
    return (True, None, filters)

# ---------- regime & targets ----------
def regime_and_targets(symbol: str, entry: Decimal, stop: Decimal, side_open: str, extra: dict) -> Tuple[str, List[float], List[Decimal], Optional[float]]:
    candles = get_klines(symbol, CFG["regime_candle_tf"], CFG["regime_candle_count"], extra) if CFG["regime_enabled"] else []
    atr = compute_atr(candles, CFG["atr_period"]) if candles else None
    adx = compute_adx(candles, CFG["adx_period"]) if candles else None
    if atr is not None and adx is not None and candles:
        last_close = candles[-1]["c"]
        atr_pct = (atr / max(last_close, 1e-9)) * 100.0
        if atr_pct <= CFG["chop_atr_pct_max"] and adx <= CFG["chop_adx_max"]:
            rgrid, regime = CFG["r_grid_chop"], "chop"
        elif atr_pct >= CFG["trend_atr_pct_min"] or adx >= CFG["trend_adx_min"]:
            rgrid, regime = CFG["r_grid_trend"], "trend"
        else:
            rgrid, regime = CFG["r_grid_normal"], "normal"
    else:
        rgrid, regime = CFG["tp_levels_fixed_r_default"], "normal"
    tps = compute_fixed_r_targets(entry, stop, "Buy" if side_open == "long" else "Sell", rgrid)
    return regime, rgrid, tps, atr

# ---------- core placement ----------
def place_5_tps_for(account_key: str, extra: dict, symbol: str, side: str, entry: Decimal, qty: Decimal, pos_idx: int):
    allowed, reason, filters = symbol_allowed_by_policy(symbol, extra)
    if not allowed:
        log.info(f"{account_key}:{symbol} skipped by policy: {reason}")
        send_tg(f"⏸️ {account_key}:{symbol} skipped — {reason}")
        with STATE.lock:
            # still track pos so we don't spam; mark as 'placed' to suppress loops
            STATE.pos[(account_key, symbol)]["tp_placed"] = True
        return

    # Breaker guard: ensure SL, do not place TPs
    if breaker_active():
        tick = filters.tick_size
        stop = ensure_stop(symbol, side, entry, pos_idx, tick, extra)
        with STATE.lock:
            STATE.pos[(account_key, symbol)]["tp_placed"] = True
        send_tg(f"⛔ {account_key}:{symbol} breaker active — SL ensured at {stop}, TPs paused.")
        return

    tick, step, minq = filters.tick_size, filters.step_size, filters.min_order_qty

    stop = ensure_stop(symbol, side, entry, pos_idx, tick, extra)
    regime, rgrid, tp_prices, atr_val = regime_and_targets(symbol, entry, stop, side, extra)

    sizes, _ = allocate_sizes(qty, CFG["tp_allocations"], step, minq)
    if sum(sizes) == 0:
        if CFG["fallback_single_tp_when_tiny"]:
            single = compute_fixed_r_targets(entry, stop, "Buy" if side == "long" else "Sell", [CFG["single_tp_level_fixed_r"]])[0]
            try:
                http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), takeProfit=str(single), **extra)
                send_tg(f"🪙 {account_key}:{symbol}: tiny position, single TP at {single} (1.0R).")
            except Exception as e:
                log.warning(f"fallback single-TP failed: {e}")
        with STATE.lock:
            STATE.pos[(account_key, symbol)]["tp_placed"] = True
            STATE.pos[(account_key, symbol)]["tp_prices"] = tp_prices
        return

    close_side = side_to_close(side)
    placed = 0
    for px, q in zip(tp_prices, sizes):
        if q < minq:
            continue
        if place_tp(http, symbol, close_side, px, q, tick, extra):
            placed += 1

    with STATE.lock:
        STATE.pos[(account_key, symbol)]["tp_placed"] = True
        STATE.pos[(account_key, symbol)]["tp_prices"] = tp_prices
        STATE.pos[(account_key, symbol)]["tick"] = tick
        STATE.pos[(account_key, symbol)]["atr_val"] = atr_val
        STATE.pos[(account_key, symbol)]["r_value"] = abs(entry - stop)
        STATE.pos[(account_key, symbol)]["be_moved"] = False
        STATE.pos[(account_key, symbol)]["entry"] = entry
        STATE.pos[(account_key, symbol)]["side"] = side
        STATE.pos[(account_key, symbol)]["pos_idx"] = pos_idx

    alloc_str = ", ".join([f"{int(w*100)}%" for w in CFG["tp_allocations"]])
    px_str = ", ".join([str(round_price_to_tick(Decimal(p), tick)) for p in tp_prices])
    extra_note = f" | maxLev≥{CFG['min_max_leverage']}" if CFG["require_min_max_leverage"] else ""
    send_tg(f"✅ {account_key}:{symbol} placed {placed}/5 TPs{extra_note}\nRegime:{regime}\nEntry:{entry} Stop:{stop}\nAlloc:{alloc_str}\nTPs:{px_str}")

def set_sl(account_key: str, extra: dict, symbol: str, pos_idx: int, new_sl: Decimal):
    try:
        http.set_trading_stop(category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(new_sl), **extra)
        send_tg(f"🛡️ {account_key}:{symbol} SL → {new_sl}")
    except Exception as e:
        log.warning(f"set SL error {account_key}:{symbol}: {e}")

# ---------- fills: BE + laddered SL ----------
def handle_tp_fill(account_key: str, extra: dict, ev: dict):
    symbol = ev.get("symbol")
    with STATE.lock:
        s = STATE.pos.get((account_key, symbol))
    if not s or not s.get("tp_placed"):
        return

    with STATE.lock:
        s["tp_filled"] = s.get("tp_filled", 0) + 1
        filled_count = s["tp_filled"]
        tp_prices = s.get("tp_prices", [])
        entry = Decimal(str(s["entry"]))
        side = s["side"]
        tick = s["tick"]
        pos_idx = s["pos_idx"]

    filled_px = ev.get("avgPrice") or ev.get("execPrice") or ev.get("price")
    send_tg(f"🎯 {account_key}:{symbol} TP filled {filled_count}/5 @ {filled_px}")

    # Move to BE after TP2 (once)
    if filled_count >= CFG["be_after_tp_index"]:
        with STATE.lock:
            if not s.get("be_moved"):
                be_price = entry + tick * CFG["be_buffer_ticks"] if side == "long" else entry - tick * CFG["be_buffer_ticks"]
                s["be_moved"] = True
                set_sl(account_key, extra, symbol, pos_idx, be_price)

    # Laddered SL: TP3 -> SL to TP2; TP4 -> SL to TP3; TP5 -> SL to TP4
    if filled_count >= 3 and tp_prices:
        prev_index = min(filled_count - 1, 4) - 1  # TP3->index 1, TP4->2, TP5->3
        if prev_index >= 0:
            ladder_price = Decimal(str(tp_prices[prev_index]))
            set_sl(account_key, extra, symbol, pos_idx, ladder_price)

# ---------- bootstrap ----------
def load_positions_once():
    for acct_key, extra in account_iter():
        try:
            res = http.get_positions(category=CFG["category"], **extra)
            arr = res.get("result", {}).get("list", [])
            for p in arr:
                symbol = p["symbol"]
                if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                    continue
                if CFG["symbol_blocklist"] and symbol.upper() in {s.upper() for s in CFG["symbol_blocklist"]}:
                    continue
                size = Decimal(p.get("size") or "0")
                if size == 0:
                    continue
                side = "long" if p.get("side","").lower() == "buy" else "short"
                entry = Decimal(p.get("avgPrice") or "0")
                pos_idx = int(p.get("positionIdx") or 0)
                with STATE.lock:
                    STATE.pos[(acct_key, symbol)] = {
                        "side": side, "qty": size, "entry": entry, "pos_idx": pos_idx,
                        "tp_placed": False, "tp_filled": 0
                    }
        except Exception as e:
            log.error(f"load pos {acct_key} error: {e}")

def bootstrap_existing():
    for acct_key, extra in account_iter():
        with STATE.lock:
            keys = [k for k in STATE.pos.keys() if k[0] == acct_key]
        for _, symbol in keys:
            allowed, reason, filters = symbol_allowed_by_policy(symbol, extra)
            if not allowed:
                log.info(f"{acct_key}:{symbol} skipped at bootstrap by policy: {reason}")
                with STATE.lock:
                    STATE.pos[(acct_key, symbol)]["tp_placed"] = True
                continue
            if breaker_active():
                tick = filters.tick_size
                ensure_stop(symbol, STATE.pos[(acct_key, symbol)]["side"], Decimal(STATE.pos[(acct_key, symbol)]["entry"]), STATE.pos[(acct_key, symbol)]["pos_idx"], tick, extra)
                with STATE.lock:
                    STATE.pos[(acct_key, symbol)]["tp_placed"] = True
                send_tg(f"⛔ {acct_key}:{symbol} breaker active — SL ensured, TPs paused.")
                continue
            tick = filters.tick_size
            ensure_stop(symbol, STATE.pos[(acct_key, symbol)]["side"], Decimal(STATE.pos[(acct_key, symbol)]["entry"]), STATE.pos[(acct_key, symbol)]["pos_idx"], tick, extra)
            place_5_tps_for(acct_key, extra, symbol, STATE.pos[(acct_key, symbol)]["side"], Decimal(STATE.pos[(acct_key, symbol)]["entry"]), STATE.pos[(acct_key, symbol)]["qty"], STATE.pos[(acct_key, symbol)]["pos_idx"])

# ---------- WS handler ----------
def ws_private_handler(message):
    try:
        if not isinstance(message, dict):
            return
        topic = message.get("topic", "")
        data = message.get("data", [])
        if not data:
            return

        # Execution/order fills
        if "execution" in topic or "order" in topic:
            for ev in data:
                symbol = ev.get("symbol")
                status = (ev.get("orderStatus") or ev.get("execType") or "").lower()
                reduce_only = str(ev.get("reduceOnly", "")).lower()
                if not (("filled" in status or "trade" in status) and ("true" in reduce_only or reduce_only == "")):
                    continue
                # Deliver to all accounts tracking this symbol
                with STATE.lock:
                    accounts = [ak for (ak, sym) in STATE.pos.keys() if sym == symbol]
                for acct_key in accounts:
                    extra = {} if acct_key == "main" else {"subUid": acct_key.split(":")[1]}
                    handle_tp_fill(acct_key, extra, ev)

        # Position updates
        if "position" in topic:
            for p in data:
                symbol = p.get("symbol")
                size = Decimal(p.get("size") or "0")
                side = "long" if (p.get("side","").lower() == "buy") else "short"
                entry = Decimal(p.get("avgPrice") or "0")
                pos_idx = int(p.get("positionIdx") or 0)

                for acct_key, extra in account_iter():
                    if size == 0:
                        with STATE.lock:
                            if (acct_key, symbol) in STATE.pos:
                                send_tg(f"🏁 {acct_key}:{symbol} position closed.")
                            STATE.pos.pop((acct_key, symbol), None)
                        continue

                    with STATE.lock:
                        new_pos = (acct_key, symbol) not in STATE.pos or STATE.pos[(acct_key, symbol)].get("qty", Decimal("0")) == 0
                        STATE.pos[(acct_key, symbol)] = {
                            "side": side, "qty": size, "entry": entry, "pos_idx": pos_idx,
                            "tp_placed": STATE.pos.get((acct_key, symbol), {}).get("tp_placed", False),
                            "tp_filled": STATE.pos.get((acct_key, symbol), {}).get("tp_filled", 0),
                            "tp_prices": STATE.pos.get((acct_key, symbol), {}).get("tp_prices", []),
                        }
                    if new_pos or not STATE.pos[(acct_key, symbol)]["tp_placed"]:
                        allowed, reason, filters = symbol_allowed_by_policy(symbol, extra)
                        if not allowed:
                            log.info(f"{acct_key}:{symbol} skipped by policy on new/updated pos: {reason}")
                            with STATE.lock:
                                STATE.pos[(acct_key, symbol)]["tp_placed"] = True
                            continue
                        if breaker_active():
                            tick = filters.tick_size
                            ensure_stop(symbol, side, entry, pos_idx, tick, extra)
                            with STATE.lock:
                                STATE.pos[(acct_key, symbol)]["tp_placed"] = True
                            send_tg(f"⛔ {acct_key}:{symbol} breaker active — SL ensured, TPs paused.")
                            continue
                        tick = filters.tick_size
                        ensure_stop(symbol, side, entry, pos_idx, tick, extra)
                        place_5_tps_for(acct_key, extra, symbol, side, entry, size, pos_idx)

    except Exception as e:
        log.error(f"ws handler error: {e}")

# ---------- main ----------
def bootstrap():
    lev_note = f"(policy: maxLev≥{CFG['min_max_leverage']})" if CFG["require_min_max_leverage"] else "(policy: all symbols)"
    send_tg(f"🟢 TP/SL Manager online {lev_note}.")
    log.info(f"TP/SL Manager started {lev_note}")
    load_positions_once()
    bootstrap_existing()
    try:
        ws_private.order_stream(callback=ws_private_handler)
        ws_private.position_stream(callback=ws_private_handler)
        ws_private.execution_stream(callback=ws_private_handler)
        log.info("Subscribed to private streams.")
    except Exception as e:
        log.error(f"WS subscribe error: {e}")
    while True:
        time.sleep(5)

if __name__ == "__main__":
    try:
        bootstrap()
    except KeyboardInterrupt:
        log.info("Shutting down.")
