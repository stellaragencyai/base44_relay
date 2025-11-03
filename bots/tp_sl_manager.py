#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (5-TP Fixed-R, regime-aware, spread-aware, step-ladder trailing)

Enhancements implemented:
1) Regime-aware R grid:
   - Uses recent 5m candles to compute ATR% and ADX.
   - Chooses R targets:
       * CHOP (low-vol):   [0.4, 0.8, 1.2, 1.6, 2.4] R
       * NORMAL:           [0.5, 1.0, 1.5, 2.0, 3.0] R
       * TREND (high-vol): [0.7, 1.2, 1.8, 2.6, 3.8] R
   - Thresholds configurable.

2) Spread-aware maker offset:
   - Offset ticks = clamp( max(1 tick, round(spread_ticks * spread_offset_ratio)), 1, max_maker_offset_ticks )
   - Prevents posting too far from the book when spread widens, and reduces accidental taker when spread shrinks.

3) Step-ladder trailing after breakeven:
   - After TP2 fill, set SL to BE + buffer ticks.
   - As price advances every +0.5R from entry, tighten trail by +0.1×ATR per step and increase BE buffer by +1 tick (ceilinged).
   - A lightweight per-position trailing loop updates stopLoss via HTTP.

4) Per-symbol overrides:
   - Per-coin configuration allows custom R grids, TP weights, maker offsets, etc.
   - If not provided, global defaults apply.

Requirements:
    pip install pybit python-dotenv requests

.env (in repo root):
    BYBIT_API_KEY=xxxx
    BYBIT_API_SECRET=xxxx
    BYBIT_ENV=mainnet      # or testnet
    TELEGRAM_BOT_TOKEN=1234:abc   # optional
    TELEGRAM_CHAT_ID=123456       # optional

Run:
    python bots/tp_sl_manager.py
"""

import os
import time
import math
import json
import logging
import threading
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

# ------------- Logging -------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tp5")

# ------------- Config -------------
CFG = {
    # Global base mode and allocations
    "tp_mode": "fixed_r",
    "tp_allocations": [0.15, 0.20, 0.25, 0.30, 0.35],  # sum to 1.0

    # Default R grids (used if regime detection disabled or fails)
    "tp_levels_fixed_r_default": [0.5, 1.0, 1.5, 2.0, 3.0],

    # Regime detection (5m candles)
    "regime_enabled": True,
    "regime_candle_tf": "5",          # minutes (Bybit v5 kline interval "5")
    "regime_candle_count": 120,       # recent bars to fetch
    "atr_period": 14,
    "adx_period": 14,
    "atr_pct_period": 14,             # ATR% computed vs recent mid price
    "chop_atr_pct_max": 0.25,         # e.g., 0.25% of price
    "chop_adx_max": 18.0,
    "trend_atr_pct_min": 0.60,        # e.g., 0.60% of price
    "trend_adx_min": 25.0,

    # Regime R grids
    "r_grid_chop":   [0.4, 0.8, 1.2, 1.6, 2.4],
    "r_grid_normal": [0.5, 1.0, 1.5, 2.0, 3.0],
    "r_grid_trend":  [0.7, 1.2, 1.8, 2.6, 3.8],

    # Reduce-only maker placement
    "reduce_only": True,
    "maker_post_only": True,

    # Spread-aware offset
    "spread_offset_ratio": 0.35,      # fraction of current spread in ticks
    "max_maker_offset_ticks": 5,      # cap
    "fallback_maker_offset_ticks": 2, # if book unavailable

    # Price band retry
    "price_band_retry": True,
    "price_band_max_retries": 2,

    # Rounding and min qty handling
    "round_qty": "down",
    "min_notional_carry_over": True,
    "fallback_single_tp_when_tiny": True,
    "single_tp_level_fixed_r": 1.0,

    # Stop logic
    "sl_mode": "structure_then_atr",
    "sl_atr_mult_fallback": 1.0,
    "sl_atr_buffer": 0.2,

    # Breakeven and trailing
    "be_after_tp_index": 2,
    "be_buffer_ticks": 1,
    "enable_trailing_after_be": True,

    # Step-ladder trailing config
    "trail_check_interval_sec": 1.5,     # poll mark price
    "trail_step_r": 0.5,                 # tighten every +0.5R of progress
    "trail_start_atr_mult": 0.7,         # initial trail
    "trail_tighten_per_step": 0.1,       # add per step
    "trail_tighten_max_atr_mult": 1.4,   # ceiling
    "trail_extra_be_tick_per_step": 1,   # add to BE buffer each step
    "trail_max_be_buffer_ticks": 6,      # ceiling

    # Risk/leverage (informational; entries elsewhere)
    "risk_per_trade_pct": 0.10,
    "max_leverage": 75,

    # Notifications
    "notify_telegram": True,
    "notify_on_place": True,
    "notify_on_fill": True,
    "notify_on_close": True,

    "timezone": "Atlantic/Canary",
    "category": "linear",
    "listen_symbols": "*",   # or list like ["PUMPFUNUSDT","HBARUSDT"]

    # Per-symbol overrides (optional)
    # Example:
    # "per_symbol": {
    #   "PUMPFUNUSDT": {
    #     "tp_allocations": [0.10,0.15,0.25,0.25,0.25],
    #     "r_grid_chop": [0.5,0.9,1.3,1.7,2.5],
    #     "max_maker_offset_ticks": 7
    #   }
    # }
    "per_symbol": {}
}

# ------------- Env / Clients -------------
load_dotenv()
BYBIT_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_ENV = os.getenv("BYBIT_ENV", "mainnet").lower().strip()

if BYBIT_ENV == "testnet":
    http = HTTP(testnet=True, api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)
    ws_private = WebSocket(testnet=True, channel_type="private")
    ws_public = WebSocket(testnet=True, channel_type="linear")
else:
    http = HTTP(testnet=False, api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)
    ws_private = WebSocket(testnet=False, channel_type="private")
    ws_public = WebSocket(testnet=False, channel_type="linear")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

# ------------- Telegram -------------
def send_tg(msg: str):
    if not (CFG["notify_telegram"] and TG_TOKEN and TG_CHAT):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=6
        )
    except Exception as e:
        log.warning(f"telegram/error: {e}")

# ------------- Symbols / Precision -------------
@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_order_qty: Decimal

def get_symbol_filters(symbol: str) -> SymbolFilters:
    info = http.get_instruments_info(category=CFG["category"], symbol=symbol)
    lst = info.get("result", {}).get("list", [])
    if not lst:
        raise RuntimeError(f"symbol info not found for {symbol}")
    item = lst[0]
    tick = Decimal(item["priceFilter"]["tickSize"])
    step = Decimal(item["lotSizeFilter"]["qtyStep"])
    minq = Decimal(item["lotSizeFilter"]["minOrderQty"])
    return SymbolFilters(tick, step, minq)

def round_to_step(qty: Decimal, step: Decimal) -> Decimal:
    steps = (qty / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step

def round_price_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    steps = (price / tick).to_integral_value(rounding=ROUND_DOWN)
    return steps * tick

# ------------- Market data helpers -------------
def get_orderbook_top(symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
    try:
        ob = http.get_orderbook(category=CFG["category"], symbol=symbol, limit=1)
        result = ob.get("result", {})
        bids = result.get("b", []) or result.get("bids") or []
        asks = result.get("a", []) or result.get("asks") or []
        if not bids or not asks:
            return None
        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0]))
        return best_bid, best_ask
    except Exception as e:
        log.warning(f"orderbook error {symbol}: {e}")
        return None

def get_mark_price(symbol: str) -> Optional[Decimal]:
    try:
        res = http.get_tickers(category=CFG["category"], symbol=symbol)
        lst = res.get("result", {}).get("list", [])
        if not lst:
            return None
        mp = Decimal(lst[0]["markPrice"])
        return mp
    except Exception as e:
        log.warning(f"mark price error {symbol}: {e}")
        return None

def get_klines(symbol: str, interval: str, limit: int) -> List[Dict]:
    # Bybit v5: HTTP.get_kline(category, symbol, interval, limit)
    try:
        res = http.get_kline(category=CFG["category"], symbol=symbol, interval=interval, limit=limit)
        lst = res.get("result", {}).get("list", [])
        # Expected format: list of [startTime, open, high, low, close, volume, turnover]
        # Normalize to dicts
        candles = []
        for it in lst:
            candles.append({
                "t": int(it[0]),
                "o": float(it[1]),
                "h": float(it[2]),
                "l": float(it[3]),
                "c": float(it[4]),
                "v": float(it[5])
            })
        candles.sort(key=lambda x: x["t"])
        return candles
    except Exception as e:
        log.warning(f"kline error {symbol}: {e}")
        return []

def compute_atr(candles: List[Dict], period: int) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    prev_close = candles[0]["c"]
    for i in range(1, len(candles)):
        h = candles[i]["h"]; l = candles[i]["l"]; c_prev = prev_close
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)
        prev_close = candles[i]["c"]
    if len(trs) < period:
        return None
    # Wilder smoothing (simple SMA acceptable for simplicity)
    atr = sum(trs[-period:]) / period
    return atr

def compute_adx(candles: List[Dict], period: int) -> Optional[float]:
    # Simplified ADX computation
    if len(candles) < period + 2:
        return None
    plus_dm = []
    minus_dm = []
    tr_list = []
    for i in range(1, len(candles)):
        up_move = candles[i]["h"] - candles[i-1]["h"]
        down_move = candles[i-1]["l"] - candles[i]["l"]
        plus_dm.append(max(up_move, 0) if up_move > down_move else 0)
        minus_dm.append(max(down_move, 0) if down_move > up_move else 0)
        tr = max(candles[i]["h"] - candles[i]["l"],
                 abs(candles[i]["h"] - candles[i-1]["c"]),
                 abs(candles[i]["l"] - candles[i-1]["c"]))
        tr_list.append(tr)
    if len(tr_list) < period:
        return None
    tr_n = sum(tr_list[-period:])
    if tr_n == 0:
        return None
    plus_di = 100 * (sum(plus_dm[-period:]) / tr_n)
    minus_di = 100 * (sum(minus_dm[-period:]) / tr_n)
    dx = 100 * abs(plus_di - minus_di) / max(plus_di + minus_di, 1e-9)
    # Smooth a bit using last few values
    return dx

def decide_regime(symbol: str, entry_price: Decimal) -> Tuple[str, List[float], Optional[float]]:
    if not CFG["regime_enabled"]:
        return "normal", CFG["tp_levels_fixed_r_default"], None
    candles = get_klines(symbol, CFG["regime_candle_tf"], CFG["regime_candle_count"])
    if not candles:
        return "normal", CFG["tp_levels_fixed_r_default"], None

    atr = compute_atr(candles, CFG["atr_period"])
    adx = compute_adx(candles, CFG["adx_period"])
    if atr is None or adx is None:
        return "normal", CFG["tp_levels_fixed_r_default"], atr

    # ATR% vs price (use last close as proxy)
    last_close = candles[-1]["c"]
    atr_pct = (atr / max(last_close, 1e-9)) * 100.0  # percent

    if atr_pct <= CFG["chop_atr_pct_max"] and adx <= CFG["chop_adx_max"]:
        return "chop", CFG["r_grid_chop"], atr
    if atr_pct >= CFG["trend_atr_pct_min"] or adx >= CFG["trend_adx_min"]:
        return "trend", CFG["r_grid_trend"], atr
    return "normal", CFG["r_grid_normal"], atr

# ------------- R/TP math -------------
def compute_fixed_r_targets(entry: Decimal, stop: Decimal, side: str, r_levels: List[float]) -> List[Decimal]:
    # side: "Buy" for long entry, "Sell" for short entry (closing side is opposite)
    r = (entry - stop) if side.lower() == "sell" else (stop - entry)
    r = abs(r)
    targets = []
    if side.lower() == "buy":
        for m in r_levels:
            targets.append(entry + Decimal(str(m)) * r)
    else:
        for m in r_levels:
            targets.append(entry - Decimal(str(m)) * r)
    return targets

def allocate_tp_sizes(total_qty: Decimal,
                      allocations: List[float],
                      step: Decimal,
                      min_qty: Decimal,
                      carry_over: bool = True
                      ) -> Tuple[List[Decimal], Decimal]:
    sizes = []
    carry = Decimal("0")
    for w in allocations:
        raw = total_qty * Decimal(str(w))
        if carry_over:
            raw += carry
            carry = Decimal("0")
        rounded = round_to_step(raw, step)
        if rounded < min_qty:
            carry += raw
            sizes.append(Decimal("0"))
        else:
            sizes.append(rounded)
            carry += (raw - rounded)
    remainder = Decimal("0")
    if carry > 0:
        last_add = round_to_step(carry, step)
        if last_add > 0:
            sizes[-1] = sizes[-1] + last_add
            remainder = carry - last_add
        else:
            remainder = carry
    return sizes, remainder

def side_to_closer_side(position_side: str) -> str:
    return "Sell" if position_side.lower() == "long" else "Buy"

# ------------- Maker price helper (spread-aware) -------------
def adaptive_maker_offset_ticks(symbol: str, tick: Decimal) -> int:
    # If orderbook available, compute spread in ticks and adapt
    book = get_orderbook_top(symbol)
    if not book:
        return CFG["fallback_maker_offset_ticks"]
    bid, ask = book
    spread = max(Decimal("0"), ask - bid)
    if spread <= 0:
        return 1
    spread_ticks = int((spread / tick).to_integral_value(rounding=ROUND_DOWN))
    base = max(1, round(spread_ticks * CFG["spread_offset_ratio"]))
    return int(min(max(base, 1), CFG["max_maker_offset_ticks"]))

def apply_maker_offset(price: Decimal, close_side: str, tick: Decimal, offset_ticks: int) -> Decimal:
    if close_side.lower() == "sell":
        return price - tick * offset_ticks
    else:
        return price + tick * offset_ticks

# ------------- HTTP wrappers -------------
def place_reduce_only_tp(http_cli: HTTP,
                         symbol: str,
                         close_side: str,
                         price: Decimal,
                         qty: Decimal,
                         tick: Decimal,
                         maker_offset_ticks: int,
                         retries: int) -> Optional[Dict]:
    px = round_price_to_tick(price, tick)
    px = apply_maker_offset(px, close_side, tick, maker_offset_ticks)
    body = dict(
        category=CFG["category"],
        symbol=symbol,
        side=close_side,
        orderType="Limit",
        qty=str(qty.normalize()),
        price=str(px.normalize()),
        timeInForce="PostOnly" if CFG["maker_post_only"] else "GoodTillCancel",
        reduceOnly=True
    )
    for attempt in range(retries + 1):
        try:
            res = http_cli.place_order(**body)
            if res.get("retCode") == 0:
                return res
            if attempt < retries:
                # Nudge one tick toward the book center and retry
                px = px - tick if close_side == "Sell" else px + tick
                body["price"] = str(px)
                time.sleep(0.2)
            else:
                return res
        except Exception as e:
            log.warning(f"place_order exception: {e}")
            if attempt < retries:
                time.sleep(0.4)
            else:
                return None
    return None

def set_position_tp_via_trading_stop(symbol: str, pos_idx: int, take_profit_price: Decimal):
    try:
        return http.set_trading_stop(
            category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), takeProfit=str(take_profit_price)
        )
    except Exception as e:
        log.error(f"set_trading_stop TP fallback failed: {e}")
        return None

def set_stop_loss(symbol: str, pos_idx: int, stop_loss_price: Decimal):
    try:
        return http.set_trading_stop(
            category=CFG["category"], symbol=symbol, positionIdx=str(pos_idx), stopLoss=str(stop_loss_price)
        )
    except Exception as e:
        log.warning(f"set_stop_loss failed: {e}")
        return None

# ------------- State -------------
class PositionState:
    def __init__(self):
        # symbol -> state
        self.pos: Dict[str, Dict] = {}
        self.lock = threading.Lock()

STATE = PositionState()

# ------------- Trailing manager -------------
def trailing_loop(symbol: str):
    """
    Runs after BE is set. Tightens trail every +step_r of favorable move.
    trail_atr_mult starts at trail_start_atr_mult and tightens by trail_tighten_per_step up to max.
    Also increases BE buffer ticks per step, up to a max.
    """
    try:
        with STATE.lock:
            if symbol not in STATE.pos:
                return
            s = STATE.pos[symbol]
            if not s.get("trail_active"):
                return
            entry = Decimal(str(s["entry"]))
            side = s["side"]
            tick = s["tick"]
            pos_idx = s["pos_idx"]
            r_value = s["r_value"]  # Decimal absolute R (entry-stop)
            atr_val = s.get("atr_val")  # float or None

            # Initialize step ladder parameters
            step_r = Decimal(str(CFG["trail_step_r"]))
            steps_taken = s.get("trail_steps_taken", 0)
            be_buffer_ticks = s.get("be_buffer_ticks", CFG["be_buffer_ticks"])
            trail_mult = s.get("trail_mult", CFG["trail_start_atr_mult"])
            trail_mult = min(trail_mult, CFG["trail_tighten_max_atr_mult"])

        while True:
            time.sleep(CFG["trail_check_interval_sec"])
            with STATE.lock:
                if symbol not in STATE.pos:
                    return
                s = STATE.pos[symbol]
                if not s.get("trail_active"):
                    return
                entry = Decimal(str(s["entry"]))
                side = s["side"]
                tick = s["tick"]
                pos_idx = s["pos_idx"]
                r_value = s["r_value"]
                atr_val = s.get("atr_val")

            mark = get_mark_price(symbol)
            if mark is None:
                continue
            mark = Decimal(str(mark))

            # Favorable progress in R
            if r_value > 0:
                if side == "long":
                    progress_r = (mark - entry) / r_value
                else:
                    progress_r = (entry - mark) / r_value
            else:
                progress_r = Decimal("0")

            # If we crossed a new step, tighten trail and increase BE buffer
            new_steps = int(progress_r // step_r)
            if new_steps > steps_taken:
                steps_diff = new_steps - steps_taken
                steps_taken = new_steps
                # Tighten trail
                trail_mult = min(trail_mult + CFG["trail_tighten_per_step"] * steps_diff,
                                 CFG["trail_tighten_max_atr_mult"])
                # Increase BE buffer ticks
                be_buffer_ticks = min(be_buffer_ticks + CFG["trail_extra_be_tick_per_step"] * steps_diff,
                                      CFG["trail_max_be_buffer_ticks"])
                with STATE.lock:
                    s["trail_steps_taken"] = steps_taken
                    s["trail_mult"] = trail_mult
                    s["be_buffer_ticks"] = be_buffer_ticks

            # Compute target SL as max(BE+buffer, trailing level)
            be_price = entry + tick * be_buffer_ticks if side == "long" else entry - tick * be_buffer_ticks

            if atr_val is not None:
                trail_abs = Decimal(str(atr_val)) * Decimal(str(trail_mult))
                if side == "long":
                    trail_price = mark - trail_abs
                else:
                    trail_price = mark + trail_abs
            else:
                # Fallback trailing by ticks ~ 6 ticks
                trail_ticks_abs = Decimal("6")
                if side == "long":
                    trail_price = mark - tick * trail_ticks_abs
                else:
                    trail_price = mark + tick * trail_ticks_abs

            new_sl = max(be_price, trail_price) if side == "long" else min(be_price, trail_price)

            # Update stopLoss if it improved in our favor
            try:
                pos_res = http.get_positions(category=CFG["category"], symbol=symbol)
                lst = pos_res.get("result", {}).get("list", [])
                cur_sl = None
                if lst and lst[0].get("stopLoss"):
                    cur_sl = Decimal(str(lst[0]["stopLoss"]))
                if cur_sl is None:
                    set_stop_loss(symbol, pos_idx, new_sl)
                else:
                    if (side == "long" and new_sl > cur_sl) or (side == "short" and new_sl < cur_sl):
                        set_stop_loss(symbol, pos_idx, new_sl)
            except Exception as e:
                log.debug(f"trail update error {symbol}: {e}")

    except Exception as e:
        log.error(f"trailing loop crashed for {symbol}: {e}")

# ------------- Bootstrap / Placement -------------
def load_positions_once():
    try:
        res = http.get_positions(category=CFG["category"])
        arr = res.get("result", {}).get("list", [])
        for p in arr:
            symbol = p["symbol"]
            if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                continue
            size = Decimal(p.get("size") or "0")
            if size == 0:
                continue
            side = "long" if p.get("side", "").lower() == "buy" else "short"
            entry = Decimal(p.get("avgPrice") or "0")
            pos_idx = int(p.get("positionIdx") or 0)
            with STATE.lock:
                STATE.pos[symbol] = {
                    "side": side,
                    "qty": size,
                    "entry": entry,
                    "pos_idx": pos_idx,
                    "tp_placed": False,
                    "tp_filled": 0
                }
    except Exception as e:
        log.error(f"load_positions_once error: {e}")

def get_symbol_cfg(symbol: str) -> Dict:
    # Merge per-symbol override over global CFG (shallow)
    override = (CFG.get("per_symbol") or {}).get(symbol, {})
    merged = dict(CFG)
    merged.update(override)
    return merged

def place_5_tps_for(symbol: str):
    with STATE.lock:
        s = STATE.pos.get(symbol)
    if not s:
        return
    side = s["side"]
    entry = Decimal(s["entry"])
    qty = Decimal(s["qty"])
    pos_idx = s["pos_idx"]

    filters = get_symbol_filters(symbol)
    tick, step, minq = filters.tick_size, filters.step_size, filters.min_order_qty
    cfg_sym = get_symbol_cfg(symbol)

    # Fetch stopLoss to compute R
    stop = None
    try:
        pos_res = http.get_positions(category=CFG["category"], symbol=symbol)
        lst = pos_res.get("result", {}).get("list", [])
        if lst:
            raw_sl = lst[0].get("stopLoss")
            if raw_sl and Decimal(str(raw_sl)) > 0:
                stop = Decimal(str(raw_sl))
    except Exception as e:
        log.warning(f"could not fetch stopLoss: {e}")
    if not stop:
        log.warning(f"{symbol}: stopLoss not set; cannot compute R targets. Skipping TP placement.")
        return

    # Regime detection for R grid and ATR
    regime_name, r_grid, atr_val = decide_regime(symbol, entry)
    if not r_grid:
        r_grid = cfg_sym["tp_levels_fixed_r_default"]

    # Compute R absolute value
    r_abs = abs((entry - stop) if side == "short" else (entry - stop))  # both give absolute after abs()
    r_abs = abs(entry - stop)

    # Compute targets and allocate sizes
    r_targets = compute_fixed_r_targets(entry, stop, "Buy" if side == "long" else "Sell", r_grid)
    allocs = cfg_sym["tp_allocations"]
    sizes, remainder = allocate_tp_sizes(qty, allocs, step, minq, carry_over=cfg_sym["min_notional_carry_over"])

    # Fallback if all too small
    if sum(sizes) == 0:
        if cfg_sym["fallback_single_tp_when_tiny"]:
            single_target = compute_fixed_r_targets(entry, stop, "Buy" if side == "long" else "Sell",
                                                    [cfg_sym["single_tp_level_fixed_r"]])[0]
            set_position_tp_via_trading_stop(symbol, pos_idx, single_target)
            send_tg(f"🪙 {symbol}: tiny position, set single position-TP at {single_target} (1.0R).")
        else:
            send_tg(f"🪙 {symbol}: tiny position and fallback disabled; no TP placed.")
        with STATE.lock:
            STATE.pos[symbol]["tp_placed"] = True
        return

    placed = 0
    close_side = side_to_closer_side(side)
    # Spread-aware maker offset
    with STATE.lock:
        STATE.pos[symbol]["tick"] = tick
        STATE.pos[symbol]["r_value"] = Decimal(str(r_abs))
        STATE.pos[symbol]["atr_val"] = atr_val

    for px, q in zip(r_targets, sizes):
        if q < minq:
            continue
        maker_ticks = adaptive_maker_offset_ticks(symbol, tick)
        # Respect per-symbol cap if provided
        cap = cfg_sym.get("max_maker_offset_ticks", CFG["max_maker_offset_ticks"])
        maker_ticks = min(maker_ticks, cap)
        res = place_reduce_only_tp(
            http_cli=http,
            symbol=symbol,
            close_side=close_side,
            price=px,
            qty=q,
            tick=tick,
            maker_offset_ticks=maker_ticks,
            retries=CFG["price_band_max_retries"] if CFG["price_band_retry"] else 0
        )
        if res and res.get("retCode") == 0:
            placed += 1

    with STATE.lock:
        STATE.pos[symbol]["tp_placed"] = True
        STATE.pos[symbol]["trail_active"] = False
        STATE.pos[symbol]["trail_steps_taken"] = 0
        STATE.pos[symbol]["trail_mult"] = CFG["trail_start_atr_mult"]
        STATE.pos[symbol]["be_buffer_ticks"] = CFG["be_buffer_ticks"]

    if CFG["notify_on_place"]:
        alloc_str = ", ".join([f"{int(a*100)}%" for a in allocs])
        tgt_str = ", ".join([str(round_price_to_tick(Decimal(t), tick)) for t in r_targets])
        send_tg(
            f"✅ Placed TP orders for <b>{symbol}</b>\n"
            f"Regime: <b>{regime_name.upper()}</b>\n"
            f"Side: {side.upper()} | Close: {close_side}\n"
            f"Entry: {entry} | Stop: {stop}\n"
            f"R grid: {r_grid}\n"
            f"Alloc: {alloc_str}\n"
            f"TP px: {tgt_str}\n"
            f"Placed: {placed}/5 (minQty may skip some)"
        )

# ------------- WS Handlers -------------
def ws_private_handler(message):
    try:
        if not isinstance(message, dict):
            return
        topic = message.get("topic", "")
        data = message.get("data", [])
        if not data:
            return

        # Order/execution events -> TP fills
        if "order" in topic or "execution" in topic:
            for ev in data:
                symbol = ev.get("symbol")
                if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                    continue
                status = (ev.get("orderStatus") or ev.get("execType") or "").lower()
                reduce_only = str(ev.get("reduceOnly", "")).lower()
                if ("filled" in status or "trade" in status) and ("true" in reduce_only or reduce_only == ""):
                    with STATE.lock:
                        s = STATE.pos.get(symbol)
                    if s and s.get("tp_placed"):
                        with STATE.lock:
                            s["tp_filled"] += 1
                            filled_count = s["tp_filled"]
                        filled_px = ev.get("avgPrice") or ev.get("execPrice") or ev.get("price")
                        filled_qty = ev.get("qty") or ev.get("execQty")
                        if CFG["notify_on_fill"]:
                            send_tg(f"🎯 TP filled {filled_count}/5 on <b>{symbol}</b> @ {filled_px} | qty {filled_qty}")

                        # After TP2 => set BE and enable trailing loop
                        if filled_count >= CFG["be_after_tp_index"]:
                            try:
                                with STATE.lock:
                                    pos_idx = s["pos_idx"]
                                    entry = Decimal(str(s["entry"]))
                                    tick = s["tick"]
                                    side = s["side"]
                                    be_buf = s.get("be_buffer_ticks", CFG["be_buffer_ticks"])
                                be_price = entry + tick * be_buf if side == "long" else entry - tick * be_buf
                                set_stop_loss(symbol, pos_idx, be_price)
                                send_tg(f"🛡️ <b>{symbol}</b>: SL moved to BE + {be_buf} tick(s). Trailing engaged.")
                                with STATE.lock:
                                    STATE.pos[symbol]["trail_active"] = True
                                # Launch trailing thread if not running
                                thr = threading.Thread(target=trailing_loop, args=(symbol,), daemon=True)
                                thr.start()
                            except Exception as e:
                                log.warning(f"BE/trail set error: {e}")

        # Position updates -> detect open/close
        if "position" in topic:
            for p in data:
                symbol = p.get("symbol")
                if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                    continue
                size = Decimal(p.get("size") or "0")
                if size == 0:
                    with STATE.lock:
                        existed = symbol in STATE.pos
                    if existed and CFG["notify_on_close"]:
                        send_tg(f"🏁 <b>{symbol}</b>: Position closed.")
                    with STATE.lock:
                        STATE.pos.pop(symbol, None)
                else:
                    side = "long" if (p.get("side","").lower() == "buy") else "short"
                    entry = Decimal(p.get("avgPrice") or "0")
                    pos_idx = int(p.get("positionIdx") or 0)
                    with STATE.lock:
                        new_pos = symbol not in STATE.pos
                        STATE.pos[symbol] = {
                            "side": side, "qty": size, "entry": entry, "pos_idx": pos_idx,
                            "tp_placed": STATE.pos.get(symbol, {}).get("tp_placed", False),
                            "tp_filled": STATE.pos.get(symbol, {}).get("tp_filled", 0),
                            "tick": STATE.pos.get(symbol, {}).get("tick"),
                            "r_value": STATE.pos.get(symbol, {}).get("r_value"),
                            "atr_val": STATE.pos.get(symbol, {}).get("atr_val"),
                            "trail_active": STATE.pos.get(symbol, {}).get("trail_active", False),
                            "trail_steps_taken": STATE.pos.get(symbol, {}).get("trail_steps_taken", 0),
                            "trail_mult": STATE.pos.get(symbol, {}).get("trail_mult", CFG["trail_start_atr_mult"]),
                            "be_buffer_ticks": STATE.pos.get(symbol, {}).get("be_buffer_ticks", CFG["be_buffer_ticks"])
                        }
                    if new_pos or not STATE.pos[symbol]["tp_placed"]:
                        place_5_tps_for(symbol)
    except Exception as e:
        log.error(f"ws_private_handler error: {e}")

# ------------- Main -------------
def bootstrap():
    send_tg("🟢 TP/SL Manager online (5-TP, regime-aware, spread-aware, step-ladder trailing).")
    load_positions_once()

    # Place TPs for any existing open positions at startup
    with STATE.lock:
        symbols = list(STATE.pos.keys())
    for sym in symbols:
        place_5_tps_for(sym)

    # Subscribe to private streams
    try:
        ws_private.order_stream(callback=ws_private_handler)
        ws_private.position_stream(callback=ws_private_handler)
        ws_private.execution_stream(callback=ws_private_handler)
        log.info("Subscribed to private order/position/execution streams.")
    except Exception as e:
        log.error(f"WS subscribe error: {e}")

    # Keep alive
    while True:
        time.sleep(5)

if __name__ == "__main__":
    if not (BYBIT_KEY and BYBIT_SECRET):
        log.error("Missing BYBIT_API_KEY/BYBIT_API_SECRET in environment.")
        raise SystemExit(1)
    try:
        bootstrap()
    except KeyboardInterrupt:
        log.info("Shutting down.")
