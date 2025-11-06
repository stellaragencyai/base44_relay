#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
breaker_guard.py — Global drawdown breaker for Base44

What it does
- Tracks peak equity per account (main + SUB_UIDS) and current equity
- If drawdown from peak exceeds BREAKER_DD_PCT, writes .state/risk_state.json {"breach": true, ...}
- Optional: on breach, cancels open orders and flattens positions (market reduce-only) if BREAKER_FLATTEN=true
- Telegram notifies on breach and on reset

ENV (.env)
  BREAKER_DD_PCT=3.0            # trigger if DD >= this percent
  BREAKER_FLATTEN=false         # also cancel & flatten on breach
  BREAKER_CHECK_SEC=15          # loop interval
  EQUITY_COIN=USDT              # coin to read in wallet-balance
  SUB_UIDS=comma,list           # already in your .env

Files
  .state/equity_peak.json       # persistent peaks per account
  .state/risk_state.json        # {breach: bool, reason: str, ts: int}
"""

import os, json, time, math, pathlib
from typing import Dict
from dotenv import load_dotenv

from core import relay_client as rc

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

PEAK_FILE = STATE_DIR / "equity_peak.json"
RISK_FILE = STATE_DIR / "risk_state.json"

load_dotenv(ROOT / ".env")

def _env_bool(k, default=False):
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def _env_float(k, default):
    try: return float((os.getenv(k, str(default)) or "").strip())
    except: return default

DD_PCT        = _env_float("BREAKER_DD_PCT", 3.0)
FLATTEN       = _env_bool("BREAKER_FLATTEN", False)
CHECK_SEC     = int(_env_float("BREAKER_CHECK_SEC", 15))
EQUITY_COIN   = os.getenv("EQUITY_COIN", "USDT") or "USDT"
SUB_UIDS      = [s.strip() for s in (os.getenv("SUB_UIDS","").split(",") if os.getenv("SUB_UIDS") else []) if s.strip()]

def read_json(p: pathlib.Path, default):
    try: return json.loads(p.read_text(encoding="utf-8"))
    except: return default

def write_json(p: pathlib.Path, obj: dict):
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def set_breach(reason: str):
    write_json(RISK_FILE, {"breach": True, "reason": reason, "ts": int(time.time())})
    try: rc.tg_send(f"🧨 BREAKER TRIGGERED • {reason}")
    except: pass

def clear_breach():
    write_json(RISK_FILE, {"breach": False, "reason": "", "ts": int(time.time())})
    try: rc.tg_send("🟩 Breaker reset (no breach).")
    except: pass

def breach_active() -> bool:
    j = read_json(RISK_FILE, {})
    return bool(j.get("breach"))

def _acct_key(uid: str|None) -> str:
    return "main" if not uid else f"sub:{uid}"

def _equity_for(uid: str|None) -> float:
    if uid:
        j = rc.get_wallet_balance(accountType="UNIFIED")
        # when memberId not supported by your relay’s helper body, fallback to proxy:
        try:
            body = rc.proxy("GET","/v5/account/wallet-balance", params={"accountType":"UNIFIED","memberId":uid})
        except:
            body = {}
    else:
        body = rc.get_wallet_balance(accountType="UNIFIED")

    total = 0.0
    try:
        for lst in (body.get("result",{}) or {}).get("list",[]) or []:
            # sum only selected coin, but unified totalEquity is already a number
            total += float(lst.get("totalEquity", 0))
    except:
        pass
    return total

def _positions(uid: str|None) -> list:
    p = {"category":"linear"}
    if uid: p["memberId"] = uid
    body = rc.proxy("GET","/v5/position/list", params=p)
    return ((body.get("result",{}) or {}).get("list",[]) or [])

def _open_orders(uid: str|None) -> list:
    p = {"category":"linear","openOnly":1}
    if uid: p["memberId"] = uid
    body = rc.proxy("GET","/v5/order/realtime", params=p)
    return ((body.get("result",{}) or {}).get("list",[]) or [])

def _cancel_order(order_id: str, uid: str|None):
    b = {"category":"linear","orderId":order_id}
    if uid: b["memberId"] = uid
    rc.proxy("POST","/v5/order/cancel", body=b)

def _flatten_position(pos: dict, uid: str|None):
    sym  = pos.get("symbol")
    side = (pos.get("side") or "").lower()
    qty  = float(pos.get("size") or 0)
    if not sym or qty <= 0: return
    close_side = "Sell" if side.startswith("b") else "Buy"
    b = {
        "category":"linear",
        "symbol": sym,
        "side": close_side,
        "orderType":"Market",
        "qty": f"{qty:.8f}",
        "reduceOnly": True,
        "timeInForce":"IOC"
    }
    if uid: b["memberId"] = uid
    rc.proxy("POST","/v5/order/create", body=b)

def maybe_flatten(uid: str|None):
    # cancel all open reduceOnly first to avoid collisions
    for o in _open_orders(uid):
        try:
            _cancel_order(o.get("orderId"), uid)
        except: pass
    # market close all positions
    for p in _positions(uid):
        try:
            _flatten_position(p, uid)
        except: pass

def run_once():
    peaks: Dict[str,float] = read_json(PEAK_FILE, {})
    breached = False
    worst = ("", 0.0, 0.0)  # acct, dd%, eq

    accounts = [None] + SUB_UIDS  # None = main
    for uid in accounts:
        key = _acct_key(uid)
        eq  = _equity_for(uid)
        if eq <= 0: 
            continue
        pk  = peaks.get(key, 0.0)
        if eq > pk:
            pk = eq
            peaks[key] = pk
        dd_pct = 0.0 if pk <= 0 else max(0.0, (pk - eq) / pk * 100.0)
        if dd_pct > worst[1]:
            worst = (key, dd_pct, eq)
        if dd_pct >= DD_PCT:
            breached = True

    write_json(PEAK_FILE, peaks)

    if breached and not breach_active():
        set_breach(f"Max drawdown >= {DD_PCT:.2f}% (worst: {worst[0]} {worst[1]:.2f}%, eq≈{worst[2]:.2f})")
        if FLATTEN:
            try:
                for uid in accounts:
                    maybe_flatten(uid)
                rc.tg_send("⛔ All accounts flattened and open orders canceled.")
            except Exception as e:
                rc.tg_send(f"⚠️ Flatten encountered errors: {e}")
    elif not breached and breach_active():
        clear_breach()

def main():
    rc.tg_send("🛡️ Breaker guard online.")
    while True:
        try:
            run_once()
        except Exception as e:
            rc.tg_send(f"⚠️ breaker_guard error: {e}")
        time.sleep(CHECK_SEC)

if __name__ == "__main__":
    main()
