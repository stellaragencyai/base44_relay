#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/order_sanitizer.py — price/qty legality guard using instruments cache

Purpose
- Round price to tick and qty to step
- Enforce min order qty
- Optionally clamp/max-qty and reject zero-ish sizes
- Single place to keep Bybit "what is legal" logic so bots don't duplicate it

Public API
    sanitize(symbol: str, side: str, price: float|None, qty: float,
             *, allow_market: bool = True,
             min_qty_override: float|None = None,
             max_qty_override: float|None = None) -> tuple[bool, dict, str]

Returns:
    ok: True if order is valid after sanitization
    out: {
        "symbol": str, "side": "Buy|Sell",
        "price_in": float|None, "qty_in": float,
        "price": float|None, "qty": float,
        "tick": float, "step": float, "min_qty": float,
        "reason": str (why it changed), "market": bool
    }
    reason: empty if ok, otherwise short block reason

CLI:
    python -m core.order_sanitizer --symbol BTCUSDT --side Buy --price 70250.12 --qty 0.003
    python -m core.order_sanitizer --symbol ETHUSDT --side Sell --qty 50 --market

Notes
- If price is None (market), only qty is sanitized (min/step).
- If qty rounds below min_qty, we block instead of silently bumping, because
  “surprise fills” are how accounts cry.
"""

from __future__ import annotations
import math
import argparse
from typing import Optional, Tuple, Dict

from .instruments import load_or_fetch, round_price as _round_price, round_qty as _round_qty

def _side_word(side: str) -> str:
    s = (side or "").strip().lower()
    if s in ("b","buy","long"): return "Buy"
    if s in ("s","sell","short"): return "Sell"
    return "Buy"

def _pull_meta(symbol: str) -> tuple[float, float, float]:
    meta = load_or_fetch([symbol]).get(symbol, {}) or {}
    tick = float(meta.get("tickSize", 0.01) or 0.01)
    step = float(meta.get("lotStep", 0.001) or 0.001)
    minq = float(meta.get("minQty", 0.0) or 0.0)
    # hard sanity
    tick = tick if tick > 0 else 0.01
    step = step if step > 0 else 0.001
    minq = max(0.0, minq)
    return tick, step, minq

def sanitize(symbol: str,
             side: str,
             price: Optional[float],
             qty: float,
             *,
             allow_market: bool = True,
             min_qty_override: Optional[float] = None,
             max_qty_override: Optional[float] = None) -> Tuple[bool, Dict, str]:
    symbol = (symbol or "").upper().strip()
    side2 = _side_word(side)
    tick, step, minq = _pull_meta(symbol)

    if min_qty_override is not None:
        minq = max(0.0, float(min_qty_override))
    if max_qty_override is not None:
        qty = min(float(qty), float(max_qty_override))

    qty_in  = float(qty)
    price_in = None if price is None else float(price)

    # Round qty
    qty_rounded = _round_qty(qty_in, step, minq)
    if qty_rounded <= 0:
        reason = f"qty {qty_in} rounds below min {minq}"
        return False, {
            "symbol": symbol, "side": side2,
            "price_in": price_in, "qty_in": qty_in,
            "price": price_in, "qty": 0.0,
            "tick": tick, "step": step, "min_qty": minq,
            "reason": reason, "market": price_in is None
        }, reason

    # Price handling
    if price_in is None:
        if not allow_market:
            return False, {
                "symbol": symbol, "side": side2,
                "price_in": None, "qty_in": qty_in,
                "price": None, "qty": qty_rounded,
                "tick": tick, "step": step, "min_qty": minq,
                "reason": "market not allowed", "market": True
            }, "market not allowed"
        out = {
            "symbol": symbol, "side": side2,
            "price_in": None, "qty_in": qty_in,
            "price": None, "qty": qty_rounded,
            "tick": tick, "step": step, "min_qty": minq,
            "reason": "qty_sanitized_only" if qty_rounded != qty_in else "",
            "market": True
        }
        return True, out, ""

    # Limit price rounding
    price_rounded = _round_price(price_in, tick)
    # If rounding collapses price to zero somehow, block
    if not math.isfinite(price_rounded) or price_rounded <= 0:
        reason = f"bad price after rounding tick={tick}"
        return False, {
            "symbol": symbol, "side": side2,
            "price_in": price_in, "qty_in": qty_in,
            "price": 0.0, "qty": qty_rounded,
            "tick": tick, "step": step, "min_qty": minq,
            "reason": reason, "market": False
        }, reason

    changed = []
    if abs(price_rounded - price_in) > 0:
        changed.append("price")
    if abs(qty_rounded - qty_in) > 0:
        changed.append("qty")

    return True, {
        "symbol": symbol, "side": side2,
        "price_in": price_in, "qty_in": qty_in,
        "price": price_rounded, "qty": qty_rounded,
        "tick": tick, "step": step, "min_qty": minq,
        "reason": "rounded:" + ",".join(changed) if changed else "",
        "market": False
    }, ""

# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser(description="Order sanitizer")
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--side", required=True, help="Buy/Sell/Long/Short")
    ap.add_argument("--qty", required=True, type=float)
    ap.add_argument("--price", type=float, default=None, help="omit for market")
    ap.add_argument("--no-market", action="store_true", help="disallow market")
    ap.add_argument("--min-qty", type=float, default=None)
    ap.add_argument("--max-qty", type=float, default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    ok, out, reason = sanitize(
        args.symbol, args.side, args.price, args.qty,
        allow_market=not args.no_market,
        min_qty_override=args.min_qty,
        max_qty_override=args.max_qty
    )
    if args.json:
        import json as _json
        print(_json.dumps({"ok": ok, "out": out, "reason": reason}, indent=2))
    else:
        if ok:
            mode = "MKT" if out["market"] else f"@{out['price']}"
            chg = f" ({out['reason']})" if out["reason"] else ""
            print(f"[OK] {out['symbol']} {out['side']} qty={out['qty']} {mode}{chg}")
        else:
            print(f"[BLOCK] {out['symbol']} {out['side']} qty_in={out['qty_in']} price_in={out['price_in']} • {reason}")

if __name__ == "__main__":
    main()
