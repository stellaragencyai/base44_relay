# bots/tp_sl_manager.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

# ── make repo root importable even when run directly ───────────────────────────
import sys, os, json, time, math, threading, statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

_repo = Path(__file__).resolve().parents[1]
if str(_repo) not in sys.path:
    sys.path.insert(0, str(_repo))

# ── env & shared utils ─────────────────────────────────────────────────────────
from core.env_bootstrap import *  # loads config/.env automatically

# Prefer richer notifier if present
try:
    from core import notifier_bot as _nb
    tg_send = _nb.tg_send  # type: ignore
except Exception:
    from core.base44_client import tg_send  # type: ignore

from core.base44_client import bybit_proxy

# ───────────────────────────────────────────────────────────────────────────────
# Config (override in config/.env)
# ───────────────────────────────────────────────────────────────────────────────
POLL_SEC                = int(os.getenv("TP_POLL_SEC", "10"))
STARTUP_GRACE_SEC       = int(os.getenv("TP_STARTUP_GRACE_SEC", "10"))

TP_RUNGS                = int(os.getenv("TP_RUNGS", "50"))
TP_SPAN_PCT             = float(os.getenv("TP_SPAN_PCT", "8.0"))    # total % span for uniform grid
TP_CENTER_BIAS_PCT      = float(os.getenv("TP_CENTER_BIAS_PCT", "0.0")) # 0=uniform, >0 skew outward

# New: ATR-based spacing controls
TP_USE_ATR              = os.getenv("TP_USE_ATR", "true").lower() in ("1","true","yes","y","on")
ATR_LEN                 = int(os.getenv("ATR_LEN", "14"))
ATR_INTERVAL            = os.getenv("ATR_INTERVAL", "1")  # Bybit kline interval ("1","3","5","15","30","60","240","D","W","M")
ATR_MULT                = float(os.getenv("ATR_MULT", "3.0"))  # total span ≈ ATR*ATR_MULT
ATR_FALLBACK_TO_UNIFORM = os.getenv("ATR_FALLBACK_TO_UNIFORM", "true").lower() in ("1","true","yes","y","on")

TP_ADOPT_EXISTING       = os.getenv("TP_ADOPT_EXISTING", "false").lower() in ("1","true","yes","y","on")
TP_CANCEL_NON_B44       = os.getenv("TP_CANCEL_NON_B44", "false").lower() in ("1","true","yes","y","on")
TP_DRY_RUN              = os.getenv("TP_DRY_RUN", "false").lower() in ("1","true","yes","y","on")
TP_MANAGED_TAG          = os.getenv("TP_MANAGED_TAG", "B44")  # orderLinkId prefix we own

TP_SYMBOL_WHITELIST     = [s.strip().upper() for s in (os.getenv("TP_SYMBOL_WHITELIST","").split(",")) if s.strip()]

# Stop-loss (optional)
SL_USE_TRADING_STOP     = os.getenv("SL_USE_TRADING_STOP","true").lower() in ("1","true","yes","y","on")
SL_PCT                  = float(os.getenv("SL_PCT","-2.5"))  # negative for loss; e.g., -2.5 = 2.5% stop
SL_ONLY_IF_NONE         = os.getenv("SL_ONLY_IF_NONE","true").lower() in ("1","true","yes","y","on")

# Member / account scope
MEMBER_ID               = (os.getenv("TP_MEMBER_ID","") or os.getenv("MAIN_SUB_UID","")).strip()  # empty → main acct

# Risk breaker
STATE_DIR               = Path(os.getenv("RISK_STATE_DIR", str(_repo / ".state")))
BREAKER_PATH            = STATE_DIR / "risk_state.json"

CATEGORY                = "linear"

# ───────────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────────
def _now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")

def _read_breaker() -> bool:
    try:
        if not BREAKER_PATH.exists():
            return False
        data = json.loads(BREAKER_PATH.read_text(encoding="utf-8"))
        return bool(data.get("breach"))
    except Exception:
        return False

def _proxy(method: str, path: str, *, params=None, body=None) -> dict:
    payload = bybit_proxy(method, path, params=params or {}, body=body or {})
    return payload if isinstance(payload, dict) else {}

def _quantize(val: float, step: float, mode: str = "down") -> float:
    if step <= 0:
        return val
    n = val / step
    if mode == "up":
        n = math.ceil(n - 1e-12)
    else:
        n = math.floor(n + 1e-12)
    return max(0.0, round(n * step, 12))

def _fmt(v: float, step: float) -> str:
    if step <= 0:
        return f"{v}"
    s = f"{step:.12f}".rstrip("0")
    dec = 0
    if "." in s:
        dec = len(s.split(".")[1])
    return f"{v:.{dec}f}"

def _load_instrument(symbol: str) -> dict:
    r = _proxy("GET", "/v5/market/instruments-info", params={"category": CATEGORY, "symbol": symbol})
    items = ((r.get("result") or {}).get("list")) or []
    return items[0] if items else {}

def _filters(symbol: str):
    info = _load_instrument(symbol)
    price_step = float((info.get("priceFilter") or {}).get("tickSize") or 0.0001)
    lot_step   = float((info.get("lotSizeFilter") or {}).get("qtyStep") or 0.001)
    min_qty    = float((info.get("lotSizeFilter") or {}).get("minOrderQty") or 0.0)
    return price_step, lot_step, min_qty

def _positions() -> list[dict]:
    params = {"category": CATEGORY}
    if MEMBER_ID:
        params["memberId"] = MEMBER_ID
    r = _proxy("GET", "/v5/position/list", params=params)
    if r.get("retCode") not in (0, "0"):
        print(f"[{_now()}] positions err retCode={r.get('retCode')} msg={r.get('retMsg')}")
        return []
    return ((r.get("result") or {}).get("list")) or []

def _open_orders(symbol: str) -> list[dict]:
    params = {"category": CATEGORY, "symbol": symbol, "openOnly": 0}
    if MEMBER_ID:
        params["memberId"] = MEMBER_ID
    r = _proxy("GET", "/v5/order/realtime", params=params)
    if r.get("retCode") not in (0, "0"):
        print(f"[{_now()}] order realtime err {symbol} retCode={r.get('retCode')} msg={r.get('retMsg')}")
        return []
    return ((r.get("result") or {}).get("list")) or []

def _cancel_order(order_id: str, symbol: str) -> dict:
    body = {"category": CATEGORY, "symbol": symbol, "orderId": order_id}
    if MEMBER_ID: body["memberId"] = MEMBER_ID
    if TP_DRY_RUN:
        return {"retCode": 0, "retMsg": "DRY"}
    return _proxy("POST", "/v5/order/cancel", body=body)

def _create_tp(symbol: str, side: str, qty: str, price: str, position_idx: int, link_id: str) -> dict:
    body = {
        "category": CATEGORY,
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": qty,
        "price": price,
        "timeInForce": "PostOnly",
        "reduceOnly": True,
        "orderLinkId": link_id,
        "positionIdx": position_idx,
    }
    if MEMBER_ID: body["memberId"] = MEMBER_ID
    if TP_DRY_RUN:
        return {"retCode": 0, "retMsg": "DRY"}
    return _proxy("POST", "/v5/order/create", body=body)

def _set_stop(symbol: str, position_idx: int, sl_price: str | None) -> dict:
    if not SL_USE_TRADING_STOP:
        return {"retCode": 0, "retMsg": "skipped"}
    body = {"category": CATEGORY, "symbol": symbol, "positionIdx": position_idx}
    if MEMBER_ID: body["memberId"] = MEMBER_ID
    if sl_price:
        body["stopLoss"] = sl_price
    if TP_DRY_RUN:
        return {"retCode": 0, "retMsg": "DRY"}
    return _proxy("POST", "/v5/position/trading-stop", body=body)

# ───────────────────────────────────────────────────────────────────────────────
# ATR utilities
# ───────────────────────────────────────────────────────────────────────────────
def _klines(symbol: str, interval: str, limit: int = 200) -> list[dict]:
    # Bybit v5 kline endpoint
    params = {"category": CATEGORY, "symbol": symbol, "interval": interval, "limit": str(limit)}
    r = _proxy("GET", "/v5/market/kline", params=params)
    if r.get("retCode") not in (0, "0"):
        return []
    # Bybit returns list of arrays in "list" (ts, open, high, low, close, volume, turnover)
    items = ((r.get("result") or {}).get("list")) or []
    out = []
    for row in items:
        try:
            # row is likely a list of strings; map to dict
            ts, o, h, l, c = int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])
            out.append({"t": ts, "o": o, "h": h, "l": l, "c": c})
        except Exception:
            continue
    # ensure chronological
    out.sort(key=lambda x: x["t"])
    return out

def _atr_wilder(symbol: str, length: int, interval: str) -> float | None:
    data = _klines(symbol, interval, limit=max(2*length+5, 60))
    if len(data) < length + 2:
        return None
    trs: List[float] = []
    prev_close = data[0]["c"]
    for i in range(1, len(data)):
        h = data[i]["h"]; l = data[i]["l"]; c_prev = prev_close
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)
        prev_close = data[i]["c"]
    if len(trs) < length:
        return None
    # Wilder smoothing: start with simple average, then recursive
    atr = sum(trs[:length]) / length
    alpha = 1.0 / length
    for tr in trs[length:]:
        atr = (atr * (length - 1) + tr) / length
    return atr

# ───────────────────────────────────────────────────────────────────────────────
# Ladder builders
# ───────────────────────────────────────────────────────────────────────────────
def _build_uniform_ladder(symbol: str, pos_side: str, size: float, entry: float,
                          price_step: float, lot_step: float, min_qty: float) -> List[Tuple[str,str,str]]:
    if size <= 0 or price_step <= 0 or lot_step <= 0:
        return []
    # qty per rung (uniform; shrink rung count if min order blocks)
    rungs = TP_RUNGS
    qty_per = _quantize(size / rungs, lot_step, "down")
    if qty_per < max(min_qty, lot_step):
        max_rungs = max(1, int(_quantize(size / max(min_qty, lot_step), 1.0, "down")))
        rungs = min(TP_RUNGS, max_rungs)
        qty_per = _quantize(size / rungs, lot_step, "down")

    span = max(0.1, TP_SPAN_PCT) / 100.0
    prices = []
    for i in range(1, rungs + 1):
        t = i / rungs
        if TP_CENTER_BIAS_PCT > 0:
            t = t ** (1.0 - min(0.95, TP_CENTER_BIAS_PCT/100.0))
        raw = entry * (1.0 + t * span) if pos_side == "Buy" else entry * (1.0 - t * span)
        p = _quantize(raw, price_step, "down" if pos_side=="Buy" else "up")
        prices.append(p)

    order_side = "Sell" if pos_side == "Buy" else "Buy"
    return [(_fmt(qty_per, lot_step), _fmt(p, price_step), order_side) for p in prices]

def _build_atr_ladder(symbol: str, pos_side: str, size: float, entry: float,
                      price_step: float, lot_step: float, min_qty: float) -> List[Tuple[str,str,str]]:
    atr = _atr_wilder(symbol, ATR_LEN, ATR_INTERVAL)
    if atr is None or atr <= 0:
        if ATR_FALLBACK_TO_UNIFORM:
            return _build_uniform_ladder(symbol, pos_side, size, entry, price_step, lot_step, min_qty)
        return []

    # Span = ATR * ATR_MULT from entry → linearly divided into rungs (biased if requested)
    total_span = atr * max(0.5, ATR_MULT)  # safety floor
    rungs = TP_RUNGS
    qty_per = _quantize(size / rungs, lot_step, "down")
    if qty_per < max(min_qty, lot_step):
        max_rungs = max(1, int(_quantize(size / max(min_qty, lot_step), 1.0, "down")))
        rungs = min(TP_RUNGS, max_rungs)
        qty_per = _quantize(size / rungs, lot_step, "down")

    prices = []
    for i in range(1, rungs + 1):
        t = i / rungs
        if TP_CENTER_BIAS_PCT > 0:
            t = t ** (1.0 - min(0.95, TP_CENTER_BIAS_PCT/100.0))
        if pos_side == "Buy":
            raw = entry + t * total_span
            p = _quantize(raw, price_step, "down")
        else:
            raw = entry - t * total_span
            p = _quantize(raw, price_step, "up")
        prices.append(p)

    order_side = "Sell" if pos_side == "Buy" else "Buy"
    return [(_fmt(qty_per, lot_step), _fmt(p, price_step), order_side) for p in prices]

def build_ladder(symbol: str, pos_side: str, size: float, entry_price: float):
    price_step, lot_step, min_qty = _filters(symbol)
    if TP_USE_ATR:
        ladder = _build_atr_ladder(symbol, pos_side, size, entry_price, price_step, lot_step, min_qty)
    else:
        ladder = _build_uniform_ladder(symbol, pos_side, size, entry_price, price_step, lot_step, min_qty)
    return ladder, (price_step, lot_step, min_qty)

# ───────────────────────────────────────────────────────────────────────────────
# Core loop
# ───────────────────────────────────────────────────────────────────────────────
def _avg_price(ladder: List[Tuple[str,str,str]]) -> float:
    if not ladder:
        return 0.0
    ps = [float(p) for (_q, p, _s) in ladder]
    return sum(ps) / len(ps)

def _rr_multiple(side: str, entry: float, avg_tp: float, sl_price: float | None) -> float | None:
    if not sl_price or sl_price <= 0 or entry <= 0:
        return None
    if side == "Buy":
        risk = max(1e-12, entry - sl_price)
        reward = max(0.0, avg_tp - entry)
    else:
        risk = max(1e-12, sl_price - entry)
        reward = max(0.0, entry - avg_tp)
    if risk <= 0: 
        return None
    return reward / risk

def _telegram_summary(symbol: str, pos_side: str, pos_size: float, entry: float,
                      ladder: List[Tuple[str,str,str]], placed: int, grid_type: str,
                      price_step: float, sl_set_text: str, member_label: str):
    if not ladder:
        return
    prices = [float(p) for (_q, p, _s) in ladder]
    avg_tp = statistics.fmean(prices)
    first = prices[0]
    last = prices[-1]
    span_pct = (abs(last - entry) / entry) * 100.0 if entry > 0 else 0.0

    rr = None
    # attempt to read back SL from text like "SL set SYMBOL at 1.2345 (...)" or pass None
    # Here we skip parsing; rr is computed only if SL price was computed in this loop
    # (we'll pass None unless SL was set right here)

    msg = (
        f"🎯 **TP Ladder Placed**\n"
        f"• Pair: {symbol}\n"
        f"• Side: {('🟢 Long' if pos_side=='Buy' else '🔴 Short')}\n"
        f"• Size: {pos_size}\n"
        f"• Entry: {entry}\n"
        f"• Grid: {grid_type} | Rungs: {placed}/{len(ladder)} | Span: {span_pct:.2f}%\n"
        f"• Prices: first {first}, avg {avg_tp:.6f}, last {last}\n"
        f"• Tag: {TP_MANAGED_TAG} • Member: {member_label}\n"
        f"{sl_set_text}"
    )
    tg_send(msg)

def manage_symbol_position(p: dict):
    symbol = (p.get("symbol") or "").upper()
    if not symbol:
        return

    if TP_SYMBOL_WHITELIST and symbol not in TP_SYMBOL_WHITELIST:
        return

    pos_size = float(p.get("size") or 0)
    if pos_size <= 0:
        return

    pos_side = p.get("side")  # "Buy" or "Sell"
    pos_idx  = int(p.get("positionIdx") or 0)
    entry    = float(p.get("avgPrice") or p.get("entryPrice") or 0) or float(p.get("markPrice") or 0)

    # Read open orders for symbol
    oo = _open_orders(symbol)

    ours, others = [], []
    for o in oo:
        if not bool(o.get("reduceOnly")):
            continue
        if o.get("side") == ("Sell" if pos_side == "Buy" else "Buy"):
            if (o.get("orderLinkId") or "").startswith(f"{TP_MANAGED_TAG}:"):
                ours.append(o)
            else:
                others.append(o)

    # Cancel others if configured
    if TP_CANCEL_NON_B44 and others:
        for o in others:
            _cancel_order(o.get("orderId"), symbol)
        if others:
            tg_send(f"🧹 Cancelled {len(others)} non-managed TPs on {symbol}")

    # If adopt existing and already plenty, bail
    if TP_ADOPT_EXISTING and len(ours) >= TP_RUNGS * 0.8:
        return

    # Build ladder (ATR or uniform)
    ladder, (price_step, lot_step, min_qty) = build_ladder(symbol, pos_side, pos_size, entry)
    if not ladder:
        print(f"[{_now()}] {symbol} ladder empty (size too small / filters / ATR fail)")
        return

    existing_prices = set()
    for o in ours:
        try:
            existing_prices.add(str(o.get("price")))
        except Exception:
            pass

    placed = 0
    ts_key = str(int(time.time()*1000))
    for i, (qty, price, side) in enumerate(ladder, start=1):
        if price in existing_prices:
            continue
        link_id = f"{TP_MANAGED_TAG}:{ts_key}:L{i:03d}"
        r = _create_tp(symbol, side, qty, price, pos_idx, link_id)
        if r.get("retCode") in (0,"0"):
            placed += 1
        time.sleep(0.02)

    # Stop-loss via trading-stop (optional) and prepare SL blurb
    sl_blurb = ""
    sl_price_for_rr: float | None = None
    if SL_USE_TRADING_STOP:
        try:
            if SL_ONLY_IF_NONE and (p.get("stopLoss") or ""):
                pass  # already set
            else:
                if SL_PCT < 0:
                    if pos_side == "Buy":
                        sl_raw = entry * (1.0 + SL_PCT/100.0)
                        sl_q = _fmt(_quantize(sl_raw, price_step, "down"), price_step)
                    else:
                        sl_raw = entry * (1.0 - SL_PCT/100.0)
                        sl_q = _fmt(_quantize(sl_raw, price_step, "up"), price_step)
                    sl_price_for_rr = float(sl_q)
                    r = _set_stop(symbol, pos_idx, sl_q)
                    if r.get("retCode") in (0,"0"):
                        sl_blurb = f"• SL: set at {sl_q} ({SL_PCT:.2f}%)\n"
                    else:
                        sl_blurb = f"• SL: set failed (code {r.get('retCode')})\n"
        except Exception as e:
            sl_blurb = f"• SL: error {e}\n"

    # Rich Telegram summary (with RR if SL available)
    avg_tp = _avg_price(ladder)
    rr = _rr_multiple(pos_side, entry, avg_tp, sl_price_for_rr)
    rr_blurb = f"• R:R ≈ {rr:.2f} (avg TP vs SL)\n" if rr is not None else ""
    grid_type = "ATR" if TP_USE_ATR else "Uniform"
    member_label = MEMBER_ID or "MAIN"
    header = f"🎯 **TP Ladder Placed**"

    msg = (
        f"{header}\n"
        f"• Pair: {symbol}\n"
        f"• Side: {('🟢 Long' if pos_side=='Buy' else '🔴 Short')}\n"
        f"• Size: {pos_size}\n"
        f"• Entry: {entry}\n"
        f"• Grid: {grid_type} | Rungs: {placed}/{len(ladder)}\n"
        f"• Avg TP: {avg_tp}\n"
        f"{rr_blurb}"
        f"• Tag: {TP_MANAGED_TAG} • Member: {member_label}\n"
        f"{sl_blurb}"
    )
    if placed > 0:
        tg_send(msg)

def main():
    print(
        f"TP/SL Manager online • poll {POLL_SEC}s • rungs {TP_RUNGS} • dry={TP_DRY_RUN} • "
        f"grid={'ATR' if TP_USE_ATR else 'Uniform'}(len={ATR_LEN},mult={ATR_MULT}) • member={MEMBER_ID or 'MAIN'}"
    )
    boot = time.time()
    while True:
        try:
            if (time.time() - boot) < STARTUP_GRACE_SEC:
                time.sleep(1)
                continue

            if _read_breaker():
                time.sleep(POLL_SEC)
                continue

            pos = _positions()
            for p in pos:
                try:
                    if float(p.get("size") or 0) > 0:
                        manage_symbol_position(p)
                except Exception as e:
                    print(f"[{_now()}] manage error: {e}")

            time.sleep(POLL_SEC)
        except KeyboardInterrupt:
            print("TP/SL Manager stopped by user.")
            break
        except Exception as e:
            print(f"[{_now()}] loop error: {e}")
            time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
