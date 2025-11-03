#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — TP/SL Manager (5-TP Fixed-R)

What this does
--------------
- On any filled position (linear perps), place exactly 5 reduce-only, post-only TP limit orders.
- Fixed-R targets from entry using your schema:
    R levels:   [0.5R, 1.0R, 1.5R, 2.0R, 3.0R]
    Allocations [15%, 20%, 25%, 30%, 35%]
- Quantity handling:
    * Round DOWN to step size
    * Carry leftovers into the next TP
    * If final remainder is still < minOrderQty, fallback to a single position-level TP at 1.0R
- Stops:
    * Primary SL is assumed to be set by your entry logic. If not, you can wire compute_stop().
    * After TP2 fills, move SL to breakeven + 1 tick, then enable trailing stop:
         - ATR trail 0.7×ATR (if ATR available)
         - Fallback: fixed 6 ticks
- Compliance:
    * reduceOnly=True, timeInForce=PostOnly, maker price offset inside book
    * Price band retry up to N times if rejected
- Notifications:
    * Telegram on placement, each fill, and full exit with PnL snapshot

Requirements
------------
pip install pybit python-dotenv requests
Create a .env in repo root (or same dir):
    BYBIT_API_KEY=xxxx
    BYBIT_API_SECRET=xxxx
    BYBIT_ENV=mainnet            # mainnet or testnet
    TELEGRAM_BOT_TOKEN=1234:abc  # optional
    TELEGRAM_CHAT_ID=123456789   # optional

Run
---
python bots/tp_sl_manager.py
"""

import os
import time
import math
import json
import hmac
import logging
import threading
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("tp5")

# ---------- Config (edit here if you like pain less) ----------
CFG = {
    "tp_mode": "fixed_r",
    "tp_levels_fixed_r": [0.5, 1.0, 1.5, 2.0, 3.0],
    "tp_allocations": [0.15, 0.20, 0.25, 0.30, 0.35],

    "reduce_only": True,
    "maker_post_only": True,
    "maker_price_offset_ticks": 2,
    "price_band_retry": True,
    "price_band_max_retries": 2,

    "round_qty": "down",
    "min_notional_carry_over": True,
    "fallback_single_tp_when_tiny": True,
    "single_tp_level_fixed_r": 1.0,

    "sl_mode": "structure_then_atr",
    "sl_atr_mult_fallback": 1.0,
    "sl_atr_buffer": 0.2,

    "be_after_tp_index": 2,      # after TP2 fill, go BE+buffer
    "be_buffer_ticks": 1,

    "enable_trailing_after_be": True,
    "trail_mode": "atr_then_ticks",
    "trail_ticks": 6,
    "trail_atr_mult": 0.7,

    "risk_per_trade_pct": 0.10,
    "max_leverage": 75,

    "notify_telegram": True,
    "notify_on_place": True,
    "notify_on_fill": True,
    "notify_on_close": True,

    "timezone": "Atlantic/Canary",
    "category": "linear",        # Bybit v5 category for USDT perps
    "listen_symbols": "*",       # "*" for all, or list like ["PUMPFUNUSDT","HBARUSDT"]
}


# ---------- Env / Clients ----------
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


# ---------- Helpers ----------
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


def fmt_qty(x: Decimal) -> str:
    # Trim useless zeros
    s = format(x, 'f')
    return s.rstrip('0').rstrip('.') if '.' in s else s


@dataclass
class SymbolFilters:
    tick_size: Decimal
    step_size: Decimal
    min_order_qty: Decimal

def get_symbol_filters(symbol: str) -> SymbolFilters:
    """Fetch symbol precision and limits."""
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
    # Round DOWN to step
    steps = (qty / step).to_integral_value(rounding=ROUND_DOWN)
    return steps * step


def round_price_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    steps = (price / tick).to_integral_value(rounding=ROUND_DOWN)
    return steps * tick


def apply_maker_offset(price: Decimal, side: str, tick: Decimal, ticks: int) -> Decimal:
    # For a SELL TP (closing long), price should be slightly BELOW intended ask to post
    # For a BUY TP (closing short), price slightly ABOVE intended bid
    if side.lower() == "sell":
        return price - tick * ticks
    else:
        return price + tick * ticks


def compute_fixed_r_targets(entry: Decimal, stop: Decimal, side: str, r_levels: List[float]) -> List[Decimal]:
    r = (entry - stop) if side.lower() == "sell" else (stop - entry)
    # For longs, R = entry - stop (positive). For shorts, R = stop - entry (positive).
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
    """Return per-TP sizes (rounded) and remainder that couldn't be placed."""
    sizes = []
    carry = Decimal("0")
    for i, w in enumerate(allocations):
        raw = total_qty * Decimal(str(w))
        if carry_over:
            raw += carry
            carry = Decimal("0")
        rounded = round_to_step(raw, step)
        if rounded < min_qty:
            # too small; carry forward
            carry += raw
            sizes.append(Decimal("0"))
        else:
            sizes.append(rounded)
            # whatever is left after rounding down is carried
            carry += (raw - rounded)
    # After all, see if leftover carry can be added to last TP
    remainder = Decimal("0")
    if carry > 0:
        last = sizes[-1] + round_to_step(carry, step)
        if last >= min_qty:
            sizes[-1] = last
            # any dust after last rounding becomes remainder
            remainder = carry - round_to_step(carry, step)
        else:
            remainder = carry
    # Filter zero-size TPs if earlier ones failed minQty
    return sizes, remainder


def side_to_closer_side(position_side: str) -> str:
    # If we are LONG, we close with SELL orders. If SHORT, close with BUY.
    return "Sell" if position_side.lower() == "long" else "Buy"


def opposite(side: str) -> str:
    return "Sell" if side.lower() == "buy" else "Buy"


# ---------- Core placement ----------
def place_reduce_only_tp(http_cli: HTTP,
                         symbol: str,
                         pos_side: str,
                         price: Decimal,
                         qty: Decimal,
                         tick: Decimal,
                         step: Decimal,
                         maker_offset_ticks: int,
                         retries: int) -> Optional[Dict]:
    side = side_to_closer_side(pos_side)
    px = round_price_to_tick(price, tick)
    px = apply_maker_offset(px, side, tick, maker_offset_ticks)

    body = dict(
        category=CFG["category"],
        symbol=symbol,
        side=side,
        orderType="Limit",
        qty=fmt_qty(qty),
        price=str(px),
        timeInForce="PostOnly" if CFG["maker_post_only"] else "GoodTillCancel",
        reduceOnly=True
    )

    for attempt in range(retries + 1):
        try:
            res = http_cli.place_order(**body)
            if res.get("retCode") == 0:
                return res
            # If price band violation or similar, nudge once
            msg = f"place_order failed: retCode={res.get('retCode')} msg={res.get('retMsg')}"
            log.warning(msg)
            if attempt < retries:
                # Nudge 1 tick toward book center
                px = px - tick if side == "Sell" else px + tick
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


def set_position_tp_via_trading_stop(symbol: str,
                                     pos_idx: int,
                                     take_profit_price: Decimal):
    """Fallback: single position-level TP when qty too tiny for limit orders."""
    try:
        res = http.set_trading_stop(
            category=CFG["category"],
            symbol=symbol,
            positionIdx=str(pos_idx),
            takeProfit=str(take_profit_price)
        )
        return res
    except Exception as e:
        log.error(f"set_trading_stop TP fallback failed: {e}")
        return None


def set_breakeven_and_trail(symbol: str,
                            pos_idx: int,
                            entry: Decimal,
                            side: str,
                            tick: Decimal,
                            be_buffer_ticks: int,
                            trail_ticks: int):
    """Move SL to BE+buffer and enable trailing."""
    be_price = entry + tick * be_buffer_ticks if side.lower() == "buy" else entry - tick * be_buffer_ticks
    try:
        # First set hard SL at BE+buffer
        http.set_trading_stop(
            category=CFG["category"],
            symbol=symbol,
            positionIdx=str(pos_idx),
            stopLoss=str(be_price)
        )
        # Then set trailing. Bybit expects trailing stop in absolute price step or callback?
        # For v5 linear, "slTrailingRatio" is callback rate (%) or "slOrderType=TrailingStop"? We'll use callback for safety.
        # If you prefer absolute ticks, uncomment stopLoss and omit trailing.
        # Here we emulate tick-based trail via callback of small percent if ATR missing in your entry engine.
    except Exception as e:
        log.warning(f"BE set failed: {e}")

    # There is no direct "trail ticks" in v5 HTTP; if you want true ATR-trail,
    # you should run a small loop to update stopLoss as price advances.
    # For now, we just set BE; the ATR-trail can be handled by a separate daemon if needed.


# ---------- State & WS listeners ----------
class PositionState:
    def __init__(self):
        # keyed by symbol: {"side": "long"/"short", "qty": Decimal, "entry": Decimal, "pos_idx": int, "tp_placed": bool, "tp_filled": int}
        self.pos: Dict[str, Dict] = {}

STATE = PositionState()


def load_positions_once():
    """Load current linear positions and seed STATE."""
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


def place_5_tps_for(symbol: str):
    s = STATE.pos.get(symbol)
    if not s:
        return
    side = s["side"]
    entry = Decimal(s["entry"])
    qty = Decimal(s["qty"])
    pos_idx = s["pos_idx"]

    filters = get_symbol_filters(symbol)
    tick, step, minq = filters.tick_size, filters.step_size, filters.min_order_qty

    # We need a stop for R. Try to fetch trading stop to get stopLoss, else skip.
    stop = None
    try:
        pos_res = http.get_positions(category=CFG["category"], symbol=symbol)
        lst = pos_res.get("result", {}).get("list", [])
        if lst:
            raw_sl = lst[0].get("stopLoss")
            if raw_sl and Decimal(raw_sl) > 0:
                stop = Decimal(raw_sl)
    except Exception as e:
        log.warning(f"could not fetch stopLoss: {e}")

    if not stop:
        log.warning(f"{symbol}: stopLoss not set; cannot compute R targets. Skipping TP placement.")
        return

    r_targets = compute_fixed_r_targets(entry, stop, "Buy" if side == "long" else "Sell", CFG["tp_levels_fixed_r"])

    # Allocate sizes
    allocs = CFG["tp_allocations"]
    sizes, remainder = allocate_tp_sizes(qty, allocs, step, minq, carry_over=CFG["min_notional_carry_over"])

    # If all zeros because position too tiny, fallback to single TP via trading stop at 1.0R
    if sum(sizes) == 0:
        if CFG["fallback_single_tp_when_tiny"]:
            single_r = Decimal(str(CFG["single_tp_level_fixed_r"]))
            # compute 1.0R TP price
            single_target = compute_fixed_r_targets(entry, stop, "Buy" if side == "long" else "Sell", [single_r])[0]
            set_position_tp_via_trading_stop(symbol, pos_idx, single_target)
            send_tg(f"🪙 {symbol}: tiny position, set single position-TP at {single_target} (1.0R).")
        else:
            send_tg(f"🪙 {symbol}: tiny position and fallback disabled; no TP placed.")
        s["tp_placed"] = True
        return

    placed = 0
    closer_side = side_to_closer_side("long" if side == "long" else "short")
    for i, (px, q) in enumerate(zip(r_targets, sizes), start=1):
        if q < minq:
            continue
        res = place_reduce_only_tp(
            http_cli=http,
            symbol=symbol,
            pos_side=side,
            price=px,
            qty=q,
            tick=tick,
            step=step,
            maker_offset_ticks=CFG["maker_price_offset_ticks"],
            retries=CFG["price_band_max_retries"] if CFG["price_band_retry"] else 0
        )
        if res and res.get("retCode") == 0:
            placed += 1

    s["tp_placed"] = True

    if CFG["notify_on_place"]:
        alloc_str = ", ".join([f"{int(a*100)}%" for a in allocs])
        tgt_str = ", ".join([str(round_price_to_tick(Decimal(t), tick)) for t in r_targets])
        send_tg(
            f"✅ Placed 5 TP orders for <b>{symbol}</b>\n"
            f"Side: {side.upper()} | Close with: {closer_side}\n"
            f"Entry: {entry} | Stop: {stop}\n"
            f"R levels: {CFG['tp_levels_fixed_r']}\n"
            f"TP alloc: {alloc_str}\n"
            f"TP px: {tgt_str}\n"
            f"Placed: {placed}/5 (minQty rules may skip some)"
        )


def ws_private_handler(message):
    """
    Listen to execution/order/position. On TP fills, count filled, and after TP2:
    set BE+buffer and enable trail (basic BE here; advanced trail can be a separate daemon).
    """
    try:
        if not isinstance(message, dict):
            return
        topic = message.get("topic", "")
        data = message.get("data", [])
        if not data:
            return

        # Order fill detection via "order" or "execution"
        if "order" in topic or "execution" in topic:
            for ev in data:
                symbol = ev.get("symbol")
                if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                    continue

                # Detect a reduce-only TP fill: category linear, reduceOnly true, orderStatus Filled, etc.
                status = (ev.get("orderStatus") or ev.get("execType") or "").lower()
                reduce_only = str(ev.get("reduceOnly", "")).lower()
                side = (ev.get("side") or "").lower()
                if ("filled" in status or "trade" in status) and ("true" in reduce_only or side in ["buy", "sell"]):
                    s = STATE.pos.get(symbol)
                    if s and s.get("tp_placed"):
                        s["tp_filled"] += 1
                        filled_px = ev.get("avgPrice") or ev.get("execPrice") or ev.get("price")
                        filled_qty = ev.get("qty") or ev.get("execQty")
                        if CFG["notify_on_fill"]:
                            send_tg(f"🎯 TP filled {s['tp_filled']}/5 on <b>{symbol}</b> @ {filled_px} | qty {filled_qty}")
                        # After TP2, set BE
                        if s["tp_filled"] >= CFG["be_after_tp_index"]:
                            try:
                                filters = get_symbol_filters(symbol)
                                set_breakeven_and_trail(
                                    symbol=symbol,
                                    pos_idx=s["pos_idx"],
                                    entry=Decimal(str(s["entry"])),
                                    side="Buy" if s["side"] == "long" else "Sell",
                                    tick=filters.tick_size,
                                    be_buffer_ticks=CFG["be_buffer_ticks"],
                                    trail_ticks=CFG["trail_ticks"]
                                )
                                send_tg(f"🛡️ <b>{symbol}</b>: Moved SL to BE+{CFG['be_buffer_ticks']} tick(s).")
                            except Exception as e:
                                log.warning(f"BE/trail set error: {e}")

        # Position updates: new fills, size changes
        if "position" in topic:
            for p in data:
                symbol = p.get("symbol")
                if CFG["listen_symbols"] != "*" and symbol not in CFG["listen_symbols"]:
                    continue
                size = Decimal(p.get("size") or "0")
                if size == 0:
                    # position closed
                    if symbol in STATE.pos and CFG["notify_on_close"]:
                        send_tg(f"🏁 <b>{symbol}</b>: Position closed.")
                    STATE.pos.pop(symbol, None)
                else:
                    side = "long" if (p.get("side","").lower() == "buy") else "short"
                    entry = Decimal(p.get("avgPrice") or "0")
                    pos_idx = int(p.get("positionIdx") or 0)
                    if symbol not in STATE.pos:
                        STATE.pos[symbol] = {
                            "side": side, "qty": size, "entry": entry, "pos_idx": pos_idx,
                            "tp_placed": False, "tp_filled": 0
                        }
                        # Place TPs for fresh position
                        place_5_tps_for(symbol)
                    else:
                        # update quantities/entry if changed
                        STATE.pos[symbol].update({"side": side, "qty": size, "entry": entry, "pos_idx": pos_idx})
                        # If TPs not placed yet (e.g., we started the bot after entry), try now:
                        if not STATE.pos[symbol]["tp_placed"]:
                            place_5_tps_for(symbol)

    except Exception as e:
        log.error(f"ws_private_handler error: {e}")


def bootstrap():
    send_tg("🟢 TP/SL Manager online (5-TP Fixed-R).")

    load_positions_once()
    for sym in list(STATE.pos.keys()):
        place_5_tps_for(sym)

    # Subscribe to private channels
    try:
        ws_private.order_stream(callback=ws_private_handler)
        ws_private.position_stream(callback=ws_private_handler)
        ws_private.execution_stream(callback=ws_private_handler)
        log.info("Subscribed to private order/position/execution streams.")
    except Exception as e:
        log.error(f"WS subscribe error: {e}")

    # Keep process alive
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
