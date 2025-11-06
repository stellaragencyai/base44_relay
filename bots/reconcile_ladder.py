#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Ladder Reconciler
Purpose:
- Inspect live positions
- Ensure per-position:
  • Exactly N reduce-only TP limit orders exist (our tag/prefix only)
  • A stop-loss protection order exists (reduce-only), if SAFE_MODE=1
  • Prices/qtys honor exchange filters
- Optionally adopt existing TPs (leave them in place) or cancel stray ones
- Zero authority to flatten positions or open new ones. It only manages exits.

Key env (.env):
  RELAY_URL
  RELAY_TOKEN

  # scope
  RECON_SYMBOL_WHITELIST=              # optional CSV; blank = all
  RECON_INCLUDE_SHORTS=true
  RECON_INCLUDE_LONGS=true

  # behavior
  RECON_ENABLED=true
  RECON_DRY_RUN=true
  RECON_SAFE_MODE=true                 # place/repair SL
  RECON_POLL_SEC=8
  RECON_TAG_PREFIX=B44
  RECON_ADOPT_EXISTING=true            # keep non-B44 TPs as-is
  RECON_CANCEL_STRAYS=false            # if true, cancels non-B44 on our symbol (use with care)

  # ladder spec
  RECON_RUNG_COUNT=50
  RECON_QTY_MODE=equal                 # equal | linear | frontload
  RECON_QTY_MIN_FRACTION=0.004         # min fraction of pos per rung if tiny sizes
  RECON_POST_ONLY=true
  RECON_PRICE_TOL_BPS=6                # if an order price deviates > tol, we reprice

  # grid mode
  RECON_GRID_MODE=ATR                  # ATR | FIXED
  RECON_ATR_LEN=14
  RECON_ATR_MULT=3.0                   # spacing step = ATR% * (mult / 10) preview-ish
  RECON_FIXED_STEP_BPS=35              # 0.35% between rungs for FIXED

  # SL
  RECON_SL_OFFSET_BPS=180              # 1.80% beyond invalidation fallback
  RECON_SL_TRIGGER='MARKPRICE'         # or LASTPRICE

Notes:
- Uses Bybit v5 via relay /bybit/proxy
- ReduceOnly True everywhere
- Only touches orders starting with orderLinkId prefix RECON_TAG_PREFIX unless RECON_CANCEL_STRAYS=true
- Never cancels filled/partially filled TPs; only working orders
"""

import os, time, math, requests
from typing import Dict, List, Tuple

def env_csv(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

def as_bool(v: str, default: bool=False) -> bool:
    if v is None or v == "": return default
    return str(v).lower() in ("1","true","yes","on")

def _h(v: str, default: int) -> int:
    try: return int(v)
    except: return default

def _f(v: str, default: float) -> float:
    try: return float(v)
    except: return default

def _relay_headers():
    tok = os.getenv("RELAY_TOKEN","")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def _relay_url(path: str) -> str:
    base = os.getenv("RELAY_URL","http://127.0.0.1:8080").rstrip("/")
    return base + (path if path.startswith("/") else "/" + path)

def bybit_proxy(target: str, params: Dict, method: str="GET"):
    url = _relay_url("/bybit/proxy")
    payload = {"target": target, "method": method, "params": params}
    r = requests.post(url, headers=_relay_headers(), json=payload, timeout=12)
    r.raise_for_status()
    js = r.json()
    if js.get("retCode") not in (0, "0", None) and "result" not in js:
        raise RuntimeError(f"Bybit proxy error: {js}")
    return js

def instruments_info(symbol: str) -> Dict:
    js = bybit_proxy("/v5/market/instruments-info", {"category":"linear","symbol":symbol}, "GET")
    lst = ((js.get("result") or {}).get("list") or [])
    return lst[0] if lst else {}

def fetch_positions() -> List[Dict]:
    js = bybit_proxy("/v5/position/list", {"category":"linear"}, "GET")
    lst = ((js.get("result") or {}).get("list") or [])
    # Normalize: keep only positions with size != 0
    out = []
    for p in lst:
        try:
            size = float(p.get("size") or 0.0)
        except:
            size = 0.0
        if size and abs(size) > 0:
            out.append(p)
    return out

def fetch_open_orders(symbol: str) -> List[Dict]:
    js = bybit_proxy("/v5/order/realtime", {"category":"linear","symbol":symbol}, "GET")
    return ((js.get("result") or {}).get("list") or [])

def cancel_order(symbol: str, order_id: str=None, link_id: str=None):
    params = {"category":"linear","symbol":symbol}
    if order_id: params["orderId"] = order_id
    if link_id:  params["orderLinkId"] = link_id
    return bybit_proxy("/v5/order/cancel", params, "POST")

def create_limit(symbol: str, side: str, qty: float, px: float, post_only: bool, link_id: str, reduce_only: bool=True):
    params = {
        "category":"linear","symbol":symbol,"side":side,
        "orderType":"Limit","qty":str(qty),
        "price": f"{px:.8f}","timeInForce":"PostOnly" if post_only else "GoodTillCancel",
        "reduceOnly": reduce_only,"orderLinkId": link_id
    }
    return bybit_proxy("/v5/order/create", params, "POST")

def create_sl(symbol: str, side: str, qty: float, trigger_price: float, trigger_by: str="LASTPRICE"):
    params = {
        "category":"linear","symbol":symbol,
        "side":"Sell" if side=="Buy" else "Buy",
        "orderType":"Market","qty":str(qty),
        "reduceOnly": True,
        "triggerPrice": f"{trigger_price:.8f}",
        "triggerBy": trigger_by.upper(),
        "tpslMode":"Partial"
    }
    return bybit_proxy("/v5/order/create", params, "POST")

def lot_filters(symbol: str):
    info = instruments_info(symbol)
    lot = (info.get("lotSizeFilter") or {})
    pricef = (info.get("priceFilter") or {})
    min_qty = float(lot.get("minOrderQty", "0.001"))
    step_qty = float(lot.get("qtyStep", "0.001"))
    tick = float(pricef.get("tickSize", "0.0001"))
    return min_qty, step_qty, tick

def round_step(x: float, step: float, up: bool=False) -> float:
    if step <= 0: return x
    if up: return math.ceil(x/step)*step
    return math.floor(x/step)*step

def atr_pct_from_kline(symbol: str, atr_len: int=14) -> float:
    # quick n dirty ATR% from 5m recent candles for spacing; you can swap TF if needed
    js = bybit_proxy("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"5","limit":"200"}, "GET")
    rows = ((js.get("result") or {}).get("list") or [])
    rows = sorted(rows, key=lambda r: int(r[0]))
    if len(rows) < atr_len + 2:
        return 0.8  # fallback sane-ish
    highs = [float(r[2]) for r in rows]
    lows  = [float(r[3]) for r in rows]
    closes= [float(r[4]) for r in rows]
    # TR
    TR=[]; pc=None
    for i in range(len(rows)):
        if pc is None: tr = highs[i]-lows[i]
        else: tr = max(highs[i]-lows[i], abs(highs[i]-pc), abs(lows[i]-pc))
        TR.append(max(tr,0.0)); pc=closes[i]
    # Wilder smooth
    period = atr_len
    out=[]; run=0.0; count=0
    for i,v in enumerate(TR):
        count += 1
        if count <= period: run += v; out.append(None); continue
        if count == period+1: avg = run / period
        else: avg = (out[-1]*(period-1) + v) / period
        out.append(avg)
    atr_val = out[-1] if out[-1] is not None else (sum(TR[-period:])/period)
    last = closes[-1]
    atr_pct = (atr_val / last) * 100.0 if last > 0 else 1.0
    return max(0.05, min(10.0, atr_pct))

def build_ladder_prices(side: str, last: float, count: int, mode: str, atr_len: int, atr_mult: float, fixed_bps: int) -> List[float]:
    prices = []
    if mode == "ATR":
        atrp = atr_pct_from_kline(symbol, atr_len)  # uses outer var; patched below
        step = (atrp * (atr_mult / 10.0)) / 100.0  # convert % to fraction
    else:
        step = (fixed_bps / 10000.0)
    # Long: up ladder; Short: down ladder
    for k in range(1, count + 1):
        if side == "Buy":
            prices.append(last * (1 + step * k))
        else:
            prices.append(last * (1 - step * k))
    return prices

def qty_ramp(mode: str, total: float, count: int) -> List[float]:
    if count <= 0 or total <= 0: return []
    if mode == "equal":
        return [total / count] * count
    if mode == "linear":
        # small near, larger later
        s = sum(range(1, count+1))
        return [(i / s) * total for i in range(1, count+1)]
    if mode == "frontload":
        # larger near, smaller later
        s = sum(range(1, count+1))
        base = [(i / s) * total for i in range(count, 0, -1)]
        return base
    return [total / count] * count

def ensure_ladder_for_position(pos: Dict, cfg: Dict):
    symbol = pos.get("symbol")
    side   = "Buy" if pos.get("side","") == "Buy" or float(pos.get("size",0)) > 0 else "Sell"
    size   = abs(float(pos.get("size") or 0.0))
    last   = float(pos.get("markPrice") or pos.get("avgPrice") or 0.0)

    if size <= 0 or last <= 0:
        return

    # Skip direction if excluded
    if side == "Buy" and not cfg["include_longs"]:  return
    if side == "Sell" and not cfg["include_shorts"]: return

    # Filters
    min_qty, step_qty, tick = lot_filters(symbol)

    # Fetch existing open orders for symbol
    open_orders = fetch_open_orders(symbol)
    ours = []
    others = []
    prefix = cfg["tag_prefix"]
    for o in open_orders:
        if o.get("reduceOnly") != "1" and str(o.get("reduceOnly")).lower() != "true":
            continue  # only manage reduceOnly
        link = o.get("orderLinkId","") or ""
        if link.startswith(prefix):
            ours.append(o)
        else:
            others.append(o)

    # Adopt/Cancel logic for non-B44
    if others and cfg["cancel_strays"]:
        for o in others:
            if cfg["dry"]: 
                print(f"[recon] DRY cancel stray {symbol} {o.get('orderId')}")
            else:
                try: cancel_order(symbol, order_id=o.get("orderId"))
                except Exception as e: print(f"[recon] cancel stray err {symbol}: {e}")
    # else: leave them alone (adopt)

    # Target rung build
    count = cfg["rung_count"]
    qtys  = qty_ramp(cfg["qty_mode"], size, count)
    # Ensure minimal rung size
    min_frac = max(0.0, cfg["qty_min_fraction"])
    qtys = [max(q, size*min_frac) for q in qtys]
    # Round to step and enforce min_qty
    qtys = [max(min_qty, round_step(q, step_qty)) for q in qtys]

    # Determine spacing
    mode = cfg["grid_mode"]
    atr_len = cfg["atr_len"]; atr_mult = cfg["atr_mult"]; fixed_bps = cfg["fixed_step_bps"]

    # compute ladder prices
    # patch closure variable 'symbol' for build_ladder_prices' ATR call
    def prices_for_symbol(sym: str, side: str, last: float):
        nonlocal mode, atr_len, atr_mult, fixed_bps
        if mode == "ATR":
            atrp = atr_pct_from_kline(sym, atr_len)
            step = (atrp * (atr_mult / 10.0)) / 100.0
        else:
            step = (fixed_bps / 10000.0)
        ps=[]
        for k in range(1, count+1):
            if side == "Buy": ps.append(last*(1 + step*k))
            else: ps.append(last*(1 - step*k))
        return ps

    targets = prices_for_symbol(symbol, side, last)

    # Map existing ours by rung index if parsable, else by price
    existing_map = {}
    for o in ours:
        link = o.get("orderLinkId","") or ""
        rung_idx = None
        if link.startswith(prefix):
            try:
                # link format: PREFIX:SYMBOL:IDX
                rung_idx = int(link.split(":")[-1])
            except:
                rung_idx = None
        price = float(o.get("price") or 0.0)
        existing_map[rung_idx if rung_idx is not None else price] = o

    # Ensure orders
    tol = cfg["price_tol_bps"] / 10000.0
    created = 0; updated = 0
    for i in range(count):
        q = qtys[i]
        p = targets[i]
        if q < min_qty: 
            continue
        # snap price to tick
        p = round_step(p, tick, up=False if side=="Buy" else True)

        link_id = f"{prefix}:{symbol}:{i+1}"
        found = existing_map.get(i+1)
        if found:
            # if price deviates beyond tolerance, cancel and recreate
            curp = float(found.get("price") or 0.0)
            if curp <= 0:
                continue
            dev = abs(curp - p) / curp
            if dev > tol:
                if cfg["dry"]:
                    print(f"[recon] DRY reprice {symbol} rung {i+1} {curp:.8g} -> {p:.8g}")
                else:
                    try: cancel_order(symbol, order_id=found.get("orderId"))
                    except Exception as e: print(f"[recon] cancel err {symbol} r{i+1}: {e}")
                    try:
                        create_limit(symbol, "Sell" if side=="Buy" else "Buy", q, p, cfg["post_only"], link_id)
                        updated += 1
                    except Exception as e:
                        print(f"[recon] create err {symbol} r{i+1}: {e}")
        else:
            # create missing rung
            if cfg["dry"]:
                print(f"[recon] DRY create {symbol} rung {i+1} @ {p:.8g} qty {q:.8g}")
            else:
                try:
                    create_limit(symbol, "Sell" if side=="Buy" else "Buy", q, p, cfg["post_only"], link_id)
                    created += 1
                except Exception as e:
                    print(f"[recon] create err {symbol} r{i+1}: {e}")

    # Stop-loss protection
    if cfg["safe_mode"]:
        try:
            # crude SL at a percent offset from avgPrice; improves later with invalidation map
            avg = float(pos.get("avgPrice") or last)
            off = cfg["sl_offset_bps"]/10000.0
            sl_px = (avg * (1 - off)) if side=="Buy" else (avg * (1 + off))
            # Check if any reduceOnly conditional exists roughly at or beyond our trigger
            ex_sl = [o for o in open_orders if o.get("orderType")=="Market" and o.get("reduceOnly") in ("1","true")]
            if not ex_sl:
                q = round_step(size, step_qty)
                if q >= min_qty:
                    if cfg["dry"]:
                        print(f"[recon] DRY place SL {symbol} @ {sl_px:.8g} qty {q:.8g}")
                    else:
                        try: create_sl(symbol, side, q, sl_px, cfg["sl_trigger"])
                        except Exception as e: print(f"[recon] SL err {symbol}: {e}")
        except Exception as e:
            print(f"[recon] SL compute err {symbol}: {e}")

    if created or updated:
        print(f"[recon] {symbol} done • created={created} updated={updated}")

def main():
    cfg = {
        "enabled": as_bool(os.getenv("RECON_ENABLED","true")),
        "dry": as_bool(os.getenv("RECON_DRY_RUN","true")),
        "safe_mode": as_bool(os.getenv("RECON_SAFE_MODE","true")),
        "poll_sec": _h(os.getenv("RECON_POLL_SEC","8"), 8),
        "tag_prefix": os.getenv("RECON_TAG_PREFIX","B44"),
        "adopt_existing": as_bool(os.getenv("RECON_ADOPT_EXISTING","true")),
        "cancel_strays": as_bool(os.getenv("RECON_CANCEL_STRAYS","false")),
        "rung_count": _h(os.getenv("RECON_RUNG_COUNT","50"), 50),
        "qty_mode": os.getenv("RECON_QTY_MODE","equal"),
        "qty_min_fraction": _f(os.getenv("RECON_QTY_MIN_FRACTION","0.004"), 0.004),
        "post_only": as_bool(os.getenv("RECON_POST_ONLY","true")),
        "price_tol_bps": _h(os.getenv("RECON_PRICE_TOL_BPS","6"), 6),
        "grid_mode": os.getenv("RECON_GRID_MODE","ATR").upper(),
        "atr_len": _h(os.getenv("RECON_ATR_LEN","14"), 14),
        "atr_mult": _f(os.getenv("RECON_ATR_MULT","3.0"), 3.0),
        "fixed_step_bps": _h(os.getenv("RECON_FIXED_STEP_BPS","35"), 35),
        "sl_offset_bps": _h(os.getenv("RECON_SL_OFFSET_BPS","180"), 180),
        "sl_trigger": os.getenv("RECON_SL_TRIGGER","MARKPRICE").upper(),
        "include_longs": as_bool(os.getenv("RECON_INCLUDE_LONGS","true")),
        "include_shorts": as_bool(os.getenv("RECON_INCLUDE_SHORTS","true")),
        "sym_whitelist": env_csv("RECON_SYMBOL_WHITELIST"),
    }

    if not cfg["enabled"]:
        print("[recon] disabled by RECON_ENABLED=false"); return

    print(f"[recon] online • dry={cfg['dry']} • rungs={cfg['rung_count']} • grid={cfg['grid_mode']} • tol={cfg['price_tol_bps']}bps")

    while True:
        try:
            positions = fetch_positions()
            if not positions:
                # nothing to do
                time.sleep(cfg["poll_sec"]); continue

            for p in positions:
                sym = p.get("symbol","")
                if cfg["sym_whitelist"] and sym.upper() not in cfg["sym_whitelist"]:
                    continue
                side = "Buy" if float(p.get("size",0)) > 0 else "Sell"
                if side == "Buy" and not cfg["include_longs"]:  continue
                if side == "Sell" and not cfg["include_shorts"]: continue

                ensure_ladder_for_position(p, cfg)

        except Exception as e:
            print(f"[recon] loop error: {e}")

        time.sleep(cfg["poll_sec"])

if __name__ == "__main__":
    main()
