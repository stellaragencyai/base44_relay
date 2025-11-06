#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Ladder Reconciler (automation-ready, relay-aware, breaker-safe)

Purpose:
- Inspect live positions and reconcile exits without opening/flattening positions.
- For each active position:
  • Ensure exactly N reduce-only TP limit orders (ours, tagged with RECON_TAG_PREFIX).
  • Optionally adopt non-B44 TPs or cancel them per config.
  • Ensure a reduce-only stop-loss protection order (SAFE_MODE=1).
  • Obey exchange filters (tick, qty step, min qty) using core.instruments.
- Emits decision logs and Telegram notifications with rate limiting.
- Skips writes when global breaker is active.

Key env (.env):
  RELAY_URL, RELAY_TOKEN                    # relay access (fallback if core.relay_client missing)
  RECON_ENABLED=true
  RECON_DRY_RUN=true
  RECON_SAFE_MODE=true
  RECON_POLL_SEC=8
  RECON_TAG_PREFIX=B44
  RECON_ADOPT_EXISTING=true
  RECON_CANCEL_STRAYS=false
  RECON_RUNG_COUNT=50
  RECON_QTY_MODE=equal                      # equal | linear | frontload
  RECON_QTY_MIN_FRACTION=0.004
  RECON_POST_ONLY=true
  RECON_PRICE_TOL_BPS=6
  RECON_GRID_MODE=ATR                       # ATR | FIXED
  RECON_ATR_LEN=14
  RECON_ATR_MULT=3.0
  RECON_FIXED_STEP_BPS=35
  RECON_SL_OFFSET_BPS=180
  RECON_SL_TRIGGER=MARKPRICE                # or LASTPRICE
  RECON_INCLUDE_LONGS=true
  RECON_INCLUDE_SHORTS=true
  RECON_SYMBOL_WHITELIST=BTCUSDT,ETHUSDT   # optional CSV; blank = all

  # Optional scoping / category:
  RECON_CATEGORY=linear
  RECON_SETTLE_COIN=USDT
  RECON_SUB_UID=                             # if set, operate on this sub account

  # Notifier / logging knobs:
  RECON_HEARTBEAT_MIN=10
  RECON_ALERT_COOLDOWN_SEC=120
"""

from __future__ import annotations
import os, time, math, json, requests
from typing import Dict, List, Tuple, Optional

# ---- core env + optional integrations -----------------------------------------
try:
    # loads config/.env and provides LOGS_DIR, etc.
    from core.env_bootstrap import *  # noqa: F401,F403
except Exception:
    pass

# notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

# decision log
try:
    from core.decision_log import log_event
except Exception:
    def log_event(*_, **__): pass  # no-op if not available

# breaker
try:
    from core.breaker import is_active as breaker_active
except Exception:
    def breaker_active() -> bool: return False

# instruments helpers
try:
    from core.instruments import load_or_fetch as inst_load_or_fetch, round_price as inst_round_price, round_qty as inst_round_qty
except Exception:
    inst_load_or_fetch = None
    def inst_round_price(p: float, tick: float) -> float:
        try:
            if tick <= 0: return float(p)
            steps = math.floor(float(p) / tick + 1e-12)
            return steps * tick
        except Exception:
            return float(p)
    def inst_round_qty(q: float, step: float, min_qty: float) -> float:
        try:
            if step <= 0: out = float(q)
            else: out = math.floor(float(q) / step + 1e-12) * step
            return out if out >= max(min_qty, 0.0) else 0.0
        except Exception:
            return 0.0

# prefer core.relay_client.proxy; fallback to direct POST /bybit/proxy
try:
    from core.relay_client import proxy as relay_proxy
except Exception:
    relay_proxy = None

# ---- env helpers ---------------------------------------------------------------
def _csv(name: str) -> List[str]:
    raw = os.getenv(name, "") or ""
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

def _b(name: str, default: bool=False) -> bool:
    v = os.getenv(name, "")
    if v is None or v == "": return default
    return str(v).lower() in {"1","true","yes","on"}

def _i(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

def _f(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except Exception: return default

# ---- config --------------------------------------------------------------------
CFG = {
    "enabled":           _b("RECON_ENABLED", True),
    "dry":               _b("RECON_DRY_RUN", True),
    "safe_mode":         _b("RECON_SAFE_MODE", True),
    "poll_sec":          _i("RECON_POLL_SEC", 8),
    "tag_prefix":        os.getenv("RECON_TAG_PREFIX","B44"),
    "adopt_existing":    _b("RECON_ADOPT_EXISTING", True),
    "cancel_strays":     _b("RECON_CANCEL_STRAYS", False),

    "rung_count":        max(1, _i("RECON_RUNG_COUNT", 50)),
    "qty_mode":          os.getenv("RECON_QTY_MODE","equal").lower().strip(),
    "qty_min_fraction":  max(0.0, _f("RECON_QTY_MIN_FRACTION", 0.004)),
    "post_only":         _b("RECON_POST_ONLY", True),
    "price_tol_bps":     max(0, _i("RECON_PRICE_TOL_BPS", 6)),

    "grid_mode":         (os.getenv("RECON_GRID_MODE","ATR") or "ATR").upper(),
    "atr_len":           max(2, _i("RECON_ATR_LEN", 14)),
    "atr_mult":          max(0.1, _f("RECON_ATR_MULT", 3.0)),
    "fixed_step_bps":    max(1, _i("RECON_FIXED_STEP_BPS", 35)),

    "sl_offset_bps":     max(1, _i("RECON_SL_OFFSET_BPS", 180)),
    "sl_trigger":        (os.getenv("RECON_SL_TRIGGER","MARKPRICE") or "MARKPRICE").upper(),

    "include_longs":     _b("RECON_INCLUDE_LONGS", True),
    "include_shorts":    _b("RECON_INCLUDE_SHORTS", True),
    "sym_whitelist":     _csv("RECON_SYMBOL_WHITELIST"),

    "category":          (os.getenv("RECON_CATEGORY","linear") or "linear"),
    "settle_coin":       (os.getenv("RECON_SETTLE_COIN","USDT") or "USDT").upper(),
    "sub_uid":           (os.getenv("RECON_SUB_UID","") or "").strip(),

    "hb_min":            max(0, _i("RECON_HEARTBEAT_MIN", 10)),
    "alert_cooldown":    max(30, _i("RECON_ALERT_COOLDOWN_SEC", 120)),
}

RELAY_URL  = (os.getenv("RELAY_URL","http://127.0.0.1:8080") or "http://127.0.0.1:8080").rstrip("/")
RELAY_TOKEN= os.getenv("RELAY_TOKEN","") or os.getenv("RELAY_SECRET","")

def _relay_headers() -> Dict[str,str]:
    h = {"Content-Type":"application/json"}
    if RELAY_TOKEN:
        h["Authorization"] = f"Bearer {RELAY_TOKEN}"
        h["x-relay-token"] = RELAY_TOKEN
    return h

# ---- relay proxy wrapper --------------------------------------------------------
def _bybit_proxy(target: str, params: Dict, method: str="GET") -> dict:
    """
    Calls relay /bybit/proxy. Prefer core.relay_client.proxy; else do HTTP POST.
    Supports optional subUid scoping.
    """
    p = dict(params or {})
    if CFG["sub_uid"]:
        p.setdefault("subUid", CFG["sub_uid"])
    if relay_proxy:
        return relay_proxy(method, target, params=p)
    url = f"{RELAY_URL}/bybit/proxy"
    payload = {"target": target, "method": method, "params": p}
    r = requests.post(url, headers=_relay_headers(), json=payload, timeout=15)
    r.raise_for_status()
    js = r.json()
    # Some relays nest payloads; normalize a bit
    if isinstance(js, dict) and "primary" in js and "body" in js.get("primary", {}):
        js = js["primary"]["body"]
    return js

# ---- exchange info helpers -----------------------------------------------------
def _inst_info(symbols: List[str]) -> Dict[str, dict]:
    if inst_load_or_fetch:
        try:
            return inst_load_or_fetch(symbols)
        except Exception:
            pass
    # Fallback direct hit if instruments module unavailable
    out = {}
    for s in symbols:
        try:
            body = _bybit_proxy("/v5/market/instruments-info", {"category": CFG["category"], "symbol": s}, "GET")
            lst = ((body.get("result") or {}).get("list") or [])
            it = lst[0] if lst else {}
            pf = it.get("priceFilter", {}) or {}
            lf = it.get("lotSizeFilter", {}) or {}
            out[s] = {
                "tickSize": float(pf.get("tickSize", 0.01)),
                "lotStep":  float(lf.get("qtyStep", 0.001)),
                "minQty":   float(lf.get("minOrderQty", 0.001)),
            }
        except Exception:
            continue
    return out

def _round_price_for_side(px: float, tick: float, side: str) -> float:
    """
    For longs we prefer floor to avoid taker; for shorts we prefer ceil away from market.
    """
    if tick <= 0:
        return float(px)
    if side == "Buy":  # TP is Sell, above market; use floor to nearest tick on target price
        return inst_round_price(px, tick)
    else:              # TP is Buy, below market; still use floor which moves toward zero; adjust:
        # Ensure rounding does not push price the wrong way for shorts
        floored = inst_round_price(px, tick)
        if floored > px:  # rare due to floor; guard anyway
            return floored - tick
        return floored

# ---- API wrappers --------------------------------------------------------------
def _fetch_positions() -> List[dict]:
    params = {"category": CFG["category"]}
    if CFG["category"].lower() == "linear":
        params["settleCoin"] = CFG["settle_coin"]
    body = _bybit_proxy("/v5/position/list", params, "GET")
    lst = ((body.get("result") or {}).get("list") or [])
    out = []
    for p in lst:
        try:
            sz = float(p.get("size") or 0)
            if abs(sz) > 0:
                out.append(p)
        except Exception:
            continue
    return out

def _fetch_open_orders(symbol: str) -> List[dict]:
    params = {"category": CFG["category"], "symbol": symbol}
    body = _bybit_proxy("/v5/order/realtime", params, "GET")
    return ((body.get("result") or {}).get("list") or [])

def _cancel_order(symbol: str, order_id: Optional[str]=None, link_id: Optional[str]=None) -> dict:
    params = {"category": CFG["category"], "symbol": symbol}
    if order_id: params["orderId"] = order_id
    if link_id:  params["orderLinkId"] = link_id
    return _bybit_proxy("/v5/order/cancel", params, "POST")

def _create_limit(symbol: str, side: str, qty: float, px: float, post_only: bool, link_id: str, reduce_only: bool=True) -> dict:
    params = {
        "category": CFG["category"],
        "symbol": symbol,
        "side": side,
        "orderType": "Limit",
        "qty": f"{qty:.8f}",
        "price": f"{px:.12f}",
        "timeInForce": "PostOnly" if post_only else "GoodTillCancel",
        "reduceOnly": True if reduce_only else False,
        "orderLinkId": link_id[:36],  # Bybit limit ~36 for linkId
    }
    if CFG["category"].lower() == "linear":
        params["tpslMode"] = "Partial"
    return _bybit_proxy("/v5/order/create", params, "POST")

def _create_sl(symbol: str, side: str, qty: float, trigger_price: float, trigger_by: str="LASTPRICE") -> dict:
    params = {
        "category": CFG["category"],
        "symbol": symbol,
        "side": "Sell" if side == "Buy" else "Buy",
        "orderType": "Market",
        "qty": f"{qty:.8f}",
        "reduceOnly": True,
        "triggerPrice": f"{trigger_price:.12f}",
        "triggerBy": trigger_by.upper(),
        "tpslMode": "Partial",
    }
    return _bybit_proxy("/v5/order/create", params, "POST")

# ---- ATR% spacing --------------------------------------------------------------
def _atr_pct_5m(symbol: str, atr_len: int) -> float:
    body = _bybit_proxy("/v5/market/kline", {"category": CFG["category"], "symbol": symbol, "interval": "5", "limit": "200"}, "GET")
    rows = ((body.get("result") or {}).get("list") or [])
    try:
        rows = sorted(rows, key=lambda r: int(r[0]))
    except Exception:
        return 0.8
    if len(rows) < atr_len + 2:
        return 0.8
    highs = [float(r[2]) for r in rows]
    lows  = [float(r[3]) for r in rows]
    closes= [float(r[4]) for r in rows]
    TR=[]; pc=None
    for i in range(len(rows)):
        if pc is None: tr = highs[i]-lows[i]
        else: tr = max(highs[i]-lows[i], abs(highs[i]-pc), abs(lows[i]-pc))
        TR.append(max(tr,0.0)); pc=closes[i]
    # Wilder smoothing
    period = atr_len
    run = sum(TR[:period])
    if len(TR) <= period:
        atr = run / max(period,1)
    else:
        atr = run / period
        for v in TR[period:]:
            atr = (atr*(period-1) + v)/period
    last = closes[-1] if closes else 0.0
    atr_pct = (atr / last) * 100.0 if last > 0 else 1.0
    return max(0.05, min(10.0, atr_pct))

def _ladder_prices(symbol: str, side: str, last: float, count: int) -> List[float]:
    if CFG["grid_mode"] == "ATR":
        atrp = _atr_pct_5m(symbol, CFG["atr_len"])
        step = (atrp * (CFG["atr_mult"] / 10.0)) / 100.0   # convert % to fraction
    else:
        step = (CFG["fixed_step_bps"] / 10000.0)
    prices = []
    for k in range(1, count + 1):
        if side == "Buy":
            prices.append(last * (1 + step * k))
        else:
            prices.append(last * (1 - step * k))
    return prices

def _qty_ramp(mode: str, total: float, count: int) -> List[float]:
    if count <= 0 or total <= 0: return []
    if mode == "equal":
        return [total / count] * count
    if mode == "linear":
        s = sum(range(1, count+1))
        return [(i / s) * total for i in range(1, count+1)]
    if mode == "frontload":
        s = sum(range(1, count+1))
        return [((count - i + 1) / s) * total for i in range(1, count+1)]
    return [total / count] * count

# ---- core reconcile ------------------------------------------------------------
_last_hb = 0.0
_last_alert = 0.0

def _reduce_only(o: dict) -> bool:
    v = o.get("reduceOnly")
    if isinstance(v, bool): return v
    s = str(v).lower()
    return s in {"1","true","yes","on"}

def _is_conditional(o: dict) -> bool:
    # Bybit sends triggerPrice for conditionals; orderType Market for SL/TP market
    return "triggerPrice" in o or o.get("orderType") in {"Stop","Market"} and o.get("stopOrderType")

def _ensure_for_position(pos: dict, filters: dict):
    symbol = pos.get("symbol","")
    side   = "Buy" if float(pos.get("size",0)) > 0 else "Sell"
    size   = abs(float(pos.get("size") or 0.0))
    last   = float(pos.get("markPrice") or pos.get("avgPrice") or 0.0)

    if size <= 0 or last <= 0:
        return

    if side == "Buy" and not CFG["include_longs"]:  return
    if side == "Sell" and not CFG["include_shorts"]: return

    # per-symbol filters
    tick = float(filters.get("tickSize", 0.01))
    step = float(filters.get("lotStep", 0.001))
    min_qty = float(filters.get("minQty", 0.001))

    open_orders = _fetch_open_orders(symbol)

    # Split ours vs others (reduce-only limit TPs only)
    prefix = CFG["tag_prefix"]
    ours, others, all_tp = [], [], []
    for o in open_orders:
        if not _reduce_only(o):  # manage exits only
            continue
        if (o.get("orderType") or "").lower() != "limit":
            # keep for SL detection below
            continue
        all_tp.append(o)
        link = (o.get("orderLinkId") or "")
        if link.startswith(prefix):
            ours.append(o)
        else:
            others.append(o)

    # Adoption/cancellation logic
    if CFG["cancel_strays"] and others and not CFG["dry"]:
        for o in others:
            try:
                _cancel_order(symbol, order_id=o.get("orderId"))
                log_event("reconciler", "cancel_stray", symbol, CFG["sub_uid"], {"orderId": o.get("orderId")})
            except Exception as e:
                tg_send(f"⚠️ Reconciler cancel stray err {symbol}: {e}", priority="warn")

    # If adopting and there are already enough reduce-only TP limits total, do nothing
    total_ro_tps = len(all_tp)
    target_cnt = CFG["rung_count"]
    if CFG["adopt_existing"] and total_ro_tps >= target_cnt:
        # Still ensure SL if safe mode
        if CFG["safe_mode"]:
            _ensure_sl(symbol, side, size, last, step, min_qty, open_orders)
        return

    # Build target rungs
    qtys = _qty_ramp(CFG["qty_mode"], size, target_cnt)
    # minimum fraction guard + rounding
    min_frac = CFG["qty_min_fraction"]
    qtys = [max(q, size * min_frac) for q in qtys]
    qtys = [inst_round_qty(q, step, min_qty) for q in qtys]

    targets = _ladder_prices(symbol, side, last, target_cnt)
    tol = CFG["price_tol_bps"] / 10000.0

    # Map our existing by rung index from orderLinkId suffix if present
    existing_by_rung: Dict[int, dict] = {}
    for o in ours:
        link = (o.get("orderLinkId") or "")
        idx = None
        try:
            idx = int(link.split(":")[-1])
        except Exception:
            idx = None
        if idx:
            existing_by_rung[idx] = o

    created = 0
    updated = 0

    # Respect breaker
    if breaker_active():
        tg_send(f"🛑 Reconciler skip writes (breaker ON)", priority="warn")
        # still send heartbeat below; no writes
    else:
        for i in range(target_cnt):
            q = qtys[i]
            p = targets[i]
            if q < min_qty:
                continue
            p = _round_price_for_side(p, tick, side)
            link_id = f"{prefix}:{symbol}:{i+1}"[:36]
            found = existing_by_rung.get(i+1)
            if found:
                try:
                    curp = float(found.get("price") or 0.0)
                except Exception:
                    curp = 0.0
                if curp > 0:
                    dev = abs(curp - p) / curp
                    if dev > tol:
                        if CFG["dry"]:
                            print(f"[recon] DRY reprice {symbol} rung {i+1} {curp:.8g} -> {p:.8g}")
                        else:
                            try:
                                _cancel_order(symbol, order_id=found.get("orderId"))
                            except Exception as e:
                                tg_send(f"⚠️ Reconciler cancel err {symbol} r{i+1}: {e}", priority="warn")
                            try:
                                _create_limit(symbol, "Sell" if side == "Buy" else "Buy", q, p, CFG["post_only"], link_id)
                                updated += 1
                                log_event("reconciler", "tp_reprice", symbol, CFG["sub_uid"], {"rung": i+1, "from": curp, "to": p, "qty": q})
                            except Exception as e:
                                tg_send(f"❌ Reconciler create err {symbol} r{i+1}: {e}", priority="error")
                # else weird; ignore
            else:
                if CFG["dry"]:
                    print(f"[recon] DRY create {symbol} rung {i+1} @ {p:.8g} qty {q:.8g}")
                else:
                    try:
                        _create_limit(symbol, "Sell" if side == "Buy" else "Buy", q, p, CFG["post_only"], link_id)
                        created += 1
                        log_event("reconciler", "tp_create", symbol, CFG["sub_uid"], {"rung": i+1, "price": p, "qty": q})
                    except Exception as e:
                        tg_send(f"❌ Reconciler create err {symbol} r{i+1}: {e}", priority="error")

    # SL protection
    if CFG["safe_mode"]:
        _ensure_sl(symbol, side, size, last, step, min_qty, open_orders)

    if created or updated:
        print(f"[recon] {symbol} done • created={created} updated={updated}")

def _ensure_sl(symbol: str, side: str, size: float, last: float, step: float, min_qty: float, open_orders: List[dict]):
    # if any reduce-only conditional exists, consider SL present
    existing_sl = [o for o in open_orders if _reduce_only(o) and _is_conditional(o)]
    if existing_sl:
        return
    avg = last
    try:
        # positions API has avgPrice in caller; but we receive 'last' already; best-effort
        pass
    except Exception:
        pass
    off = CFG["sl_offset_bps"]/10000.0
    sl_px = (avg * (1 - off)) if side == "Buy" else (avg * (1 + off))
    q = inst_round_qty(size, step, min_qty)
    if q < min_qty or q <= 0:
        return
    if breaker_active():
        print(f"[recon] breaker ON; skip SL placement for {symbol}")
        return
    if CFG["dry"]:
        print(f"[recon] DRY SL {symbol} @ {sl_px:.8g} qty {q:.8g} trigger={CFG['sl_trigger']}")
        return
    try:
        _create_sl(symbol, side, q, sl_px, CFG["sl_trigger"])
        log_event("reconciler", "sl_place", symbol, CFG["sub_uid"], {"trigger": sl_px, "qty": q, "triggerBy": CFG["sl_trigger"]})
    except Exception as e:
        tg_send(f"❌ Reconciler SL err {symbol}: {e}", priority="error")

# ---- main loop ----------------------------------------------------------------
def _heartbeat():
    global _last_hb
    if CFG["hb_min"] <= 0:
        return
    now = time.time()
    if now - _last_hb >= CFG["hb_min"] * 60:
        _last_hb = now
        tg_send(
            f"🟢 Reconciler heartbeat • dry={CFG['dry']} • rungs={CFG['rung_count']} • grid={CFG['grid_mode']} • tol={CFG['price_tol_bps']}bps • subUid={CFG['sub_uid'] or 'main'}",
            priority="success"
        )

def main():
    if not CFG["enabled"]:
        tg_send("Reconciler disabled (RECON_ENABLED=false).", priority="warn")
        return

    tg_send(
        f"🟢 Reconciler online • dry={CFG['dry']} • rungs={CFG['rung_count']} • grid={CFG['grid_mode']} • tol={CFG['price_tol_bps']}bps • subUid={CFG['sub_uid'] or 'main'}",
        priority="success"
    )

    while True:
        try:
            # Fetch active positions
            positions = _fetch_positions()
            if not positions:
                _heartbeat()
                time.sleep(CFG["poll_sec"])
                continue

            # Preload instrument filters for all symbols in one shot
            syms = sorted({p.get("symbol","") for p in positions if p.get("symbol")})
            inst = _inst_info(syms)

            # Whitelist filter
            whitelist = set(CFG["sym_whitelist"]) if CFG["sym_whitelist"] else None

            for p in positions:
                sym = p.get("symbol","")
                if not sym:
                    continue
                if whitelist and sym.upper() not in whitelist:
                    continue
                side = "Buy" if float(p.get("size",0)) > 0 else "Sell"
                if side == "Buy" and not CFG["include_longs"]:
                    continue
                if side == "Sell" and not CFG["include_shorts"]:
                    continue

                filters = inst.get(sym) or {"tickSize":0.01, "lotStep":0.001, "minQty":0.001}
                _ensure_for_position(p, filters)

            _heartbeat()

        except Exception as e:
            # Only ping occasionally to avoid alert storms
            now = time.time()
            if now - _last_alert >= CFG["alert_cooldown"]:
                tg_send(f"⚠️ Reconciler loop error: {e}", priority="warn")
                _last_alert = now

        time.sleep(CFG["poll_sec"])

if __name__ == "__main__":
    main()
