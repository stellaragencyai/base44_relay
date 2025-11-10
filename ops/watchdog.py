#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Watchdog — cheap, loud, and sufficient.

What it checks every N seconds:
- Guard heartbeat: breaker stuck, DD % creeping, exposure too high
- Executor queue lag: observed.jsonl not advancing => dead signal loop
- Bybit ping: tickers+wallet endpoints reachable
- TP/SL sweep sanity: positions exist but no RO TP orders present for >1 sweep

Sends Telegram warnings, never raises. Doesn’t flip breakers; you already
have rules for that. This just screams.
"""

from __future__ import annotations
import os, time, json
from pathlib import Path
from typing import Optional, Tuple, Dict

from core.logger import get_logger
from core.config import settings
from core.bybit_client import Bybit
from core.notifier_bot import tg_send

log = get_logger("ops.watchdog")

# Config
ROOT = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_DIR = getattr(settings, "DIR_SIGNALS", ROOT / "signals")
QUEUE_PATH  = Path(SIGNALS_DIR) / (getattr(settings, "SIGNAL_QUEUE_FILE", "observed.jsonl"))

WD_SWEEP_SEC      = int(os.getenv("WD_SWEEP_SEC", "10"))
WD_QUEUE_STALL_S  = int(os.getenv("WD_QUEUE_STALL_S", "90"))
WD_EXPO_CAP_PCT   = float(os.getenv("WD_EXPO_CAP_PCT", "0.65"))
WD_DD_SOFT_PCT    = float(os.getenv("WD_DD_SOFT_PCT", "0.9"))   # warn when DD within 90% of cap
WD_NO_TP_GRACE_S  = int(os.getenv("WD_NO_TP_GRACE_S", "45"))

# Clients
by = Bybit()
try:
    by.sync_time()
except Exception:
    pass

# --- helpers ---
def _wallet_equity() -> float:
    ok, data, err = by.get_wallet_balance(accountType="UNIFIED")
    if not ok: raise RuntimeError(err)
    total = 0.0
    for acc in (data.get("result") or {}).get("list") or []:
        total += float(acc.get("totalEquity") or 0)
    return total

def _gross_exposure_usdt() -> float:
    ok, data, err = by.get_positions(category="linear")
    if not ok: return 0.0
    gross = 0.0
    for p in (data.get("result") or {}).get("list") or []:
        try:
            sz = float(p.get("size") or 0)
            px = float(p.get("avgPrice") or 0)
            gross += abs(sz * px)
        except Exception:
            continue
    return gross

def _guard_heartbeat() -> Dict:
    try:
        from core.portfolio_guard import guard
        return guard.heartbeat()
    except Exception:
        return {}

def _queue_progress() -> Tuple[int, int]:
    # returns (size_bytes, mtime_epoch)
    if not QUEUE_PATH.exists():
        return 0, 0
    st = QUEUE_PATH.stat()
    return int(st.st_size), int(st.st_mtime)

def _open_tp_count(symbol: str) -> int:
    ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
    if not ok: return 0
    n = 0
    for it in (data.get("result") or {}).get("list") or []:
        if str(it.get("reduceOnly","")).lower() in ("1","true") and str(it.get("orderType","")).lower() == "limit":
            n += 1
    return n

# --- main sweep ---
def sweep(prev_q_bytes: int, prev_q_mtime: int, no_tp_since: Dict[str,int]) -> Tuple[int,int,Dict[str,int]]:
    # 1) basic connectivity
    ok, data, err = by.get_tickers(category="linear", symbol="BTCUSDT")
    if not ok:
        tg_send(f"❌ Watchdog: Bybit tickers down: {err}", priority="error")

    # 2) guard heartbeat
    hb = _guard_heartbeat()
    if hb:
        if hb.get("halted"):
            reason = "manual" if hb.get("manual_halt") else ("breaker" if hb.get("breaker") else "dd_cap")
            tg_send(f"⛔ Watchdog: guard halted ({reason}) dd_cap={hb.get('dd_cap_pct')} risk_live={hb.get('risk_live_usd')}", priority="warn")
        # soft warning near DD cap
        if "dd_pct" in hb and "dd_cap_pct" in hb:
            try:
                if float(hb["dd_pct"]) >= float(hb["dd_cap_pct"])*WD_DD_SOFT_PCT:
                    tg_send(f"⚠️ Watchdog: DD {hb['dd_pct']:.2f}% approaching cap {hb['dd_cap_pct']:.2f}%", priority="warn")
            except Exception:
                pass

    # 3) gross exposure guardrail
    try:
        eq = _wallet_equity()
        gross = _gross_exposure_usdt()
        if eq > 0 and gross/eq > WD_EXPO_CAP_PCT:
            tg_send(f"⚠️ Watchdog: Gross exposure {gross/eq:.1%} > soft cap {WD_EXPO_CAP_PCT:.0%}", priority="warn")
    except Exception as e:
        tg_send(f"⚠️ Watchdog: exposure check failed: {e}", priority="warn")

    # 4) queue stall
    size, mtime = _queue_progress()
    if prev_q_bytes == size and (time.time() - prev_q_mtime) > WD_QUEUE_STALL_S:
        tg_send(f"⏸️ Watchdog: signal queue stalled > {WD_QUEUE_STALL_S}s (no new lines in {QUEUE_PATH.name})", priority="warn")

    # 5) tp/sl sanity: positions but no reduce-only TPs for too long
    ok, data, err = by.get_positions(category="linear")
    if ok:
        for p in (data.get("result") or {}).get("list") or []:
            try:
                sym = str(p.get("symbol") or "").upper()
                sz = float(p.get("size") or 0)
                if sz <= 0: 
                    no_tp_since.pop(sym, None)
                    continue
                tps = _open_tp_count(sym)
                now = int(time.time())
                if tps == 0:
                    no_tp_since.setdefault(sym, now)
                    if now - no_tp_since.get(sym, now) > WD_NO_TP_GRACE_S:
                        tg_send(f"🛡️ Watchdog: {sym} position has no reduce-only TP for >{WD_NO_TP_GRACE_S}s", priority="warn")
                else:
                    no_tp_since.pop(sym, None)
            except Exception:
                continue

    return size, mtime, no_tp_since

def main():
    tg_send("🟢 Watchdog online", priority="success")
    prev_size, prev_mtime = _queue_progress()
    no_tp_since: Dict[str,int] = {}
    while True:
        try:
            time.sleep(max(5, WD_SWEEP_SEC))
            prev_size, prev_mtime, no_tp_since = sweep(prev_size, prev_mtime, no_tp_since)
        except KeyboardInterrupt:
            break
        except Exception as e:
            tg_send(f"⚠️ Watchdog loop error: {e}", priority="warn")
            time.sleep(WD_SWEEP_SEC)

if __name__ == "__main__":
    main()
