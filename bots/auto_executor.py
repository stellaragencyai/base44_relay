#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots/auto_executor.py — Semi-auto Breakout-Impulse Executor
- End-to-end from scan → validate → size → place entry + SL.
- No TPs here; your TP/SL Manager handles exits via reduce-only ladder.
- Obeys PortfolioGuard limits and DRY_RUN/SAFE_MODE.

Env (.env):
  SYMBOL_SCAN_LIST=BTCUSDT,ETHUSDT,HBARUSDT
  SIGNAL_INTERVAL=5
  LOOKBACK=200
  BO_LOOKBACK=20
  MIN_VOL_Z=1.5
  MIN_ADX=20
  MIN_ATR_PCT=0.6
  MAX_ATR_PCT=5.0
  ENTRY_MODE=LIMIT_PULLBACK     # LIMIT_PULLBACK | LIMIT_BREAK | MARKET
  PULLBACK_BPS=40               # 0.40% into the breakout
  POST_ONLY=1                   # 1=maker only for limit orders
  SAFE_MODE=1                   # forbids any flattening/closing ops
  DRY_RUN=1                     # simulate orders
  LEVERAGE=20
  BASE_ASSET=USDT
  RELAY_URL
  RELAY_TOKEN
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import os, time, math, requests, uuid
from typing import Dict, List, Tuple

try:
    from core.portfolio_guard import guard
except Exception as e:
    raise RuntimeError(f"Portfolio guard required: {e}")

# Optional regime gate, if you dropped it in
_gate = None
try:
    from core.regime_gate import gate as _gate
except Exception:
    _gate = None

def env_csv(name: str, default: str="") -> List[str]:
    raw = os.getenv(name, default)
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

def tg(text: str):
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    cid = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: 
        print("[auto_executor] no Telegram configured")
        return
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    requests.post(url, json={"chat_id": cid, "text": text, "disable_web_page_preview": True})

def _relay_headers():
    tok = os.getenv("RELAY_TOKEN","")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def _relay_url(path: str) -> str:
    base = os.getenv("RELAY_URL","http://127.0.0.1:8080").rstrip("/")
    return base + (path if path.startswith("/") else "/" + path)

# ---------- Bybit helpers ----------
def bybit_proxy(target: str, params: Dict, method: str="GET"):
    url = _relay_url("/bybit/proxy")
    payload = {"target": target, "method": method, "params": params}
    r = requests.post(url, headers=_relay_headers(), json=payload, timeout=10)
    r.raise_for_status()
    js = r.json()
    if js.get("retCode") not in (0, "0", None) and "result" not in js:
        raise RuntimeError(f"Bybit proxy error: {js}")
    return js

def instruments_info(symbol: str) -> Dict:
    js = bybit_proxy("/v5/market/instruments-info", {"category":"linear","symbol":symbol}, "GET")
    lst = ((js.get("result") or {}).get("list") or [])
    return lst[0] if lst else {}

def place_order(symbol: str, side: str, qty: float, price: float=None, post_only: bool=True, reduce_only: bool=False, order_type: str="Limit") -> Dict:
    params = {
        "category":"linear",
        "symbol":symbol,
        "side":side,                     # Buy or Sell
        "orderType":order_type,          # Limit/Market
        "qty": str(qty),
        "reduceOnly": reduce_only
    }
    if order_type == "Limit":
        params["price"] = f"{price:.8f}"
        if post_only:
            params["timeInForce"] = "PostOnly"
        else:
            params["timeInForce"] = "GoodTillCancel"
    else:
        params["timeInForce"] = "ImmediateOrCancel"

    js = bybit_proxy("/v5/order/create", params, "POST")
    return js

def place_stop_loss(symbol: str, side: str, stop_px: float, qty: float) -> Dict:
    # SL opposite side, reduce-only, trigger as market
    params = {
        "category":"linear",
        "symbol":symbol,
        "side": "Sell" if side=="Buy" else "Buy",
        "orderType":"Market",
        "qty": str(qty),
        "reduceOnly": True,
        "stopLoss": f"{stop_px:.8f}"   # Bybit set_trading_stop style OR conditional if needed
    }
    # Preferred: conditional stop order
    params = {
        "category":"linear",
        "symbol":symbol,
        "side":"Sell" if side=="Buy" else "Buy",
        "orderType":"Market",
        "qty":str(qty),
        "reduceOnly": True,
        "triggerPrice": f"{stop_px:.8f}",
        "triggerDirection": 2 if side=="Buy" else 1,  # crosses below for Buy
        "tpslMode": "Partial"  # keep flexible; TP ladder elsewhere
    }
    js = bybit_proxy("/v5/order/create", params, "POST")
    return js

def kline(symbol: str, interval: str, limit: int=300) -> List[Dict]:
    js = bybit_proxy("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":str(interval),"limit":str(limit)}, "GET")
    rows = ((js.get("result") or {}).get("list") or [])
    out=[]
    for row in rows:
        out.append({"ts":int(row[0]),"open":float(row[1]),"high":float(row[2]),"low":float(row[3]),"close":float(row[4]),"volume":float(row[5])})
    out.sort(key=lambda x:x["ts"])
    return out

# ---------- TA (same as advisor, trimmed) ----------
import math
def rolling_max(arr, window, upto): 
    start=max(0,upto-window+1); return max(arr[start:upto+1])
def true_range(h,l,c):
    out=[]; pc=None
    for i in range(len(c)):
        if pc is None: tr=h[i]-l[i]
        else: tr=max(h[i]-l[i], abs(h[i]-pc), abs(l[i]-pc))
        out.append(max(tr,0.0)); pc=c[i]
    return out
def wilder(vals,period):
    out=[]; run=0.0
    for i,v in enumerate(vals):
        if i<period: run+=v; out.append(None); continue
        if i==period: w=run/period
        else: w=(out[-1]*(period-1)+v)/period
        out.append(w)
    return out
def atr(h,l,c,period=14): return wilder(true_range(h,l,c),period)
def dx_from_hlc(h,l,c,period=14):
    plus_dm=[0.0]; minus_dm=[0.0]
    for i in range(1,len(h)):
        up=h[i]-h[i-1]; dn=l[i-1]-l[i]
        plus_dm.append(up if (up>dn and up>0) else 0.0)
        minus_dm.append(dn if (dn>up and dn>0) else 0.0)
    TR=true_range(h,l,c)
    def wl(v): 
        out=[]; run=0.0
        for i,val in enumerate(v):
            if i<period: run+=val; out.append(None); continue
            if i==period: w=run/period
            else: w=(out[-1]*(period-1)+val)/period
            out.append(w)
        return out
    atrw=wl(TR); pw=wl(plus_dm); mw=wl(minus_dm)
    pdi=[]; mdi=[]; dx=[]
    for i in range(len(h)):
        if any(x is None for x in (atrw[i],pw[i],mw[i])) or atrw[i]==0:
            pdi.append(None); mdi.append(None); dx.append(None); continue
        pdi_v=100.0*(pw[i]/atrw[i]); mdi_v=100.0*(mw[i]/atrw[i]); pdi.append(pdi_v); mdi.append(mdi_v)
        den=pdi_v+mdi_v; dx.append(0.0 if den==0 else 100.0*abs(pdi_v-mdi_v)/den)
    return dx
def adx(h,l,c,period=14):
    dx=dx_from_hlc(h,l,c,period); out=[]; run=0.0; count=0
    for v in dx:
        if v is None: out.append(None); continue
        count+=1
        if count<=period: run+=v; out.append(None); continue
        if count==period+1: avg=run/period
        else: avg=(out[-1]*(period-1)+v)/period
        out.append(avg)
    return out
def zscore(arr,lookback):
    out=[]
    for i in range(len(arr)):
        if i<lookback: out.append(None); continue
        window=arr[i-lookback+1:i+1]; m=sum(window)/len(window)
        var=sum((x-m)**2 for x in window)/len(window); sd=math.sqrt(var)
        out.append(0.0 if sd==0 else (arr[i]-m)/sd)
    return out

def atr_pct(last_close, atr_val): 
    if last_close<=0 or not atr_val: return 0.0
    return (atr_val/last_close)*100.0

# ---------- sizing ----------
def get_filters(symbol: str):
    info = instruments_info(symbol)
    lot = (info.get("lotSizeFilter") or {})
    pricef = (info.get("priceFilter") or {})
    min_qty = float(lot.get("minOrderQty", "0.001"))
    step_qty = float(lot.get("qtyStep", "0.001"))
    tick = float(pricef.get("tickSize", "0.0001"))
    return min_qty, step_qty, tick

def round_step(x: float, step: float) -> float:
    return math.floor(x/step)*step

def calc_qty(symbol: str, entry: float, stop: float) -> float:
    risk_val = guard.current_risk_value()   # equity * RISK_PCT%
    risk_val = max(0.0, risk_val)
    px_delta = abs(entry - stop)
    if px_delta <= 0:
        return 0.0
    # For linear perps: PnL per 1 unit price move ≈ qty
    qty = risk_val / px_delta
    min_qty, step_qty, _ = get_filters(symbol)
    qty = max(min_qty, round_step(qty, step_qty))
    return qty

# ---------- signal ----------
def breakout_signal(c, bo_lb, min_vol_z, min_adx, min_atr_pct, max_atr_pct):
    if len(c) < max(bo_lb+30, 80): 
        return False, {"reason":"insufficient"}
    closes=[x["close"] for x in c]; highs=[x["high"] for x in c]; lows=[x["low"] for x in c]; vols=[x["volume"] for x in c]
    adx14=adx(highs,lows,closes,14); atr14=atr(highs,lows,closes,14); volz=zscore(vols,30)
    i=len(c)-1; last=closes[i]; hh=rolling_max(highs, bo_lb, i-1)
    is_bo = highs[i] > hh and last > hh
    last_adx=adx14[i]; prev_adx=adx14[i-1] if i-1>=0 else None
    adx_ok = last_adx is not None and last_adx>=min_adx and (prev_adx is None or last_adx>=prev_adx)
    atrp = atr_pct(last, atr14[i]); atr_ok = min_atr_pct <= atrp <= max_atr_pct
    vz = volz[i]; vz_ok = (vz is not None and vz >= min_vol_z)
    if not is_bo: return False, {"reason":"no breakout"}
    if not adx_ok: return False, {"reason":f"adx {last_adx:.2f if last_adx else -1} < {min_adx}"}
    if not atr_ok: return False, {"reason":f"atr% {atrp:.2f} outside [{min_atr_pct},{max_atr_pct}]"}
    if not vz_ok: return False, {"reason":f"volz {vz:.2f if vz else -99} < {min_vol_z}"}
    pullback = hh*(1 - float(os.getenv("PULLBACK_BPS","40"))/10000.0)
    invalidation = min(lows[i], hh*0.985)
    return True, {"last":last, "break":hh, "pullback":pullback, "stop":invalidation, "atrp":atrp, "adx":last_adx, "volz":vz}

# ---------- main loop ----------
def main():
    symbols = env_csv("SYMBOL_SCAN_LIST","BTCUSDT,ETHUSDT")
    interval = os.getenv("SIGNAL_INTERVAL","5")
    lookback = int(os.getenv("LOOKBACK","200"))
    bo_lb = int(os.getenv("BO_LOOKBACK","20"))
    min_vol_z = float(os.getenv("MIN_VOL_Z","1.5"))
    min_adx_v = float(os.getenv("MIN_ADX","20"))
    min_atr = float(os.getenv("MIN_ATR_PCT","0.6"))
    max_atr = float(os.getenv("MAX_ATR_PCT","5.0"))
    mode = os.getenv("ENTRY_MODE","LIMIT_PULLBACK").upper()
    post_only = os.getenv("POST_ONLY","1") == "1"
    dry = os.getenv("DRY_RUN","1") == "1"
    safe = os.getenv("SAFE_MODE","1") == "1"
    lev = int(os.getenv("LEVERAGE","20"))

    print(f"[auto_executor] online • {interval}m • mode={mode} • dry={dry} • safe={safe}")

    while True:
        for sym in symbols:
            try:
                # optional gate
                if _gate is not None:
                    ok, why, meta = _gate.ok(sym)
                    if not ok:
                        print(f"[gate] {sym} blocked: {why} {meta}")
                        continue

                c = kline(sym, interval, limit=max(lookback, 200))
                sig, info = breakout_signal(c, bo_lb, min_vol_z, min_adx_v, min_atr, max_atr)
                if not sig:
                    continue

                if not guard.allow_new_trade(sym):
                    print(f"[guard] {sym} blocked: concurrency/symbol/dd cap")
                    continue

                entry_break = info["break"]
                entry_pull  = info["pullback"]
                stop_px     = info["stop"]
                last        = info["last"]

                if mode == "LIMIT_PULLBACK":
                    side = "Buy"
                    entry_px = entry_pull
                    order_type = "Limit"
                elif mode == "LIMIT_BREAK":
                    side = "Buy"
                    entry_px = entry_break
                    order_type = "Limit"
                else:
                    side = "Buy"
                    entry_px = None
                    order_type = "Market"

                qty = calc_qty(sym, entry_px or last, stop_px)
                if qty <= 0:
                    print(f"[size] {sym} qty<=0; skip"); continue

                trade_id = f"{sym}-{uuid.uuid4().hex[:8]}"
                if dry:
                    tg(f"🟡 DRY RUN • {sym} • {side} qty≈{qty:.8f} @ {entry_px or last:.8g} SL {stop_px:.8g}")
                    print(f"[dry] would place {side} {qty} {sym} at {entry_px or last} stop {stop_px}")
                    guard.register_open(trade_id, sym)
                    continue

                # set leverage (best-effort; ignore failures silently)
                try:
                    bybit_proxy("/v5/position/set-leverage", {"category":"linear","symbol":sym,"buyLeverage":str(lev),"sellLeverage":str(lev)}, "POST")
                except Exception as e:
                    print(f"[leverage] warn: {e}")

                # entry
                if order_type == "Limit":
                    resp = place_order(sym, side, qty, price=entry_px, post_only=post_only, reduce_only=False, order_type="Limit")
                else:
                    resp = place_order(sym, side, qty, price=None, post_only=False, reduce_only=False, order_type="Market")

                tg(f"✅ ENTRY PLACED • {sym}\nside: {side} qty: {qty:.8f}\nentry: {(entry_px or last):.8g}\nstop: {stop_px:.8g}\natr%: {info['atrp']:.2f}  adx: {info['adx']:.1f}  vz: {info['volz']:.2f}")

                # stop loss
                if safe:
                    try:
                        place_stop_loss(sym, side, stop_px, qty)
                    except Exception as e:
                        tg(f"⚠️ SL place error • {sym}: {e}")

                guard.register_open(trade_id, sym)

            except Exception as e:
                print(f"[auto_executor] {sym} error: {e}")

        time.sleep(10)

if __name__ == "__main__":
    main()
