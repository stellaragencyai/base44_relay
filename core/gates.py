#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/gates.py — Central gating logic for entries (callable from signals/executors later)

Combines:
  - breaker (hard stop)
  - trading windows (local time strings in TRADING_WINDOWS=HH:MM-HH:MM,...)
  - portfolio guard daily drawdown halt (soft stop)
  - symbol whitelist (SYMBOL_WHITELIST=CSV)
  - funding proximity lockout (from core.funding_gate)
  - spread ceiling hint (reads SIG_SPREAD_MAX_BPS env; no book fetch here)

No imports into your bots yet; you can call gates.check_all("BTCUSDT") whenever you’re ready.

Env:
  TRADING_WINDOWS=05:30-10:00,12:00-15:30
  SYMBOL_WHITELIST=BTCUSDT,ETHUSDT
  DAILY_LOSS_CAP_PCT=...
  SIG_SPREAD_MAX_BPS=8
"""

from __future__ import annotations
import os, time
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple

try:
    from core.breaker import is_active as breaker_active, remaining_ttl as breaker_ttl
except Exception:
    def breaker_active() -> bool: return False
    def breaker_ttl() -> int: return 0

try:
    from core.portfolio_guard import guard
except Exception:
    guard = None

from . import regime_gate as _regime  # if you want to extend later
from . import instruments as _instr   # placeholder for future spread/tick use
from .funding_gate import is_lockout as funding_lockout

# -------- helpers --------
def _parse_windows(s: str) -> List[Tuple[int, int]]:
    """
    Return list of (start_minute_of_day, end_minute_of_day) in LOCAL time.
    """
    s = (s or "").strip()
    out: List[Tuple[int, int]] = []
    if not s:
        return out
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk or "-" not in chunk:
            continue
        a, b = chunk.split("-", 1)
        try:
            h1, m1 = map(int, a.split(":", 1))
            h2, m2 = map(int, b.split(":", 1))
            out.append((h1*60 + m1, h2*60 + m2))
        except Exception:
            continue
    return out

def _local_minute_of_day() -> int:
    lt = time.localtime()
    return lt.tm_hour * 60 + lt.tm_min

def _now_epoch() -> int:
    return int(time.time())

def _csv(s: str) -> List[str]:
    return [x.strip().upper() for x in (s or "").split(",") if x.strip()]

# -------- config --------
_TRADING_WINDOWS = _parse_windows(os.getenv("TRADING_WINDOWS", ""))
_WHITELIST = set(_csv(os.getenv("SYMBOL_WHITELIST", "")))
_SPREAD_MAX_BPS = float(os.getenv("SIG_SPREAD_MAX_BPS", "8") or "8")

@dataclass
class GateResult:
    allow: bool
    reasons: List[str] = field(default_factory=list)
    detail: Dict[str, Any] = field(default_factory=dict)

def _gate_breaker(res: GateResult) -> None:
    if breaker_active():
        ttl = breaker_ttl()
        res.allow = False
        res.reasons.append("breaker_on")
        res.detail["breaker_ttl_sec"] = ttl

def _gate_trading_window(res: GateResult) -> None:
    if not _TRADING_WINDOWS:
        return
    mod = _local_minute_of_day()
    inside = any(a <= mod <= b for a, b in _TRADING_WINDOWS)
    if not inside:
        res.allow = False
        res.reasons.append("outside_trading_window")
        res.detail["windows"] = os.getenv("TRADING_WINDOWS")

def _gate_symbol_whitelist(symbol: str, res: GateResult) -> None:
    if not _WHITELIST:
        return
    if symbol.upper() not in _WHITELIST:
        res.allow = False
        res.reasons.append("symbol_not_whitelisted")

def _gate_daily_halt(res: GateResult) -> None:
    if guard is None:
        return
    try:
        hb = guard.heartbeat()
        if hb.get("halted"):
            res.allow = False
            res.reasons.append("daily_loss_cap_hit")
            res.detail["dd_pct"] = hb.get("dd_pct")
    except Exception:
        # guard not ready; don’t block on telemetry failure
        pass

def _gate_funding(symbol: str, res: GateResult) -> None:
    try:
        blocked, win = funding_lockout(symbol)
        res.detail["funding_minutes_to_next"] = win.minutes_to_next
        if blocked:
            res.allow = False
            res.reasons.append("funding_lockout")
    except Exception:
        # If funding calc explodes, fail open rather than brick trading
        pass

def _hint_spread_cap(res: GateResult) -> None:
    # This gate does not fetch orderbook; we only expose the configured ceiling
    res.detail["spread_max_bps"] = _SPREAD_MAX_BPS

def check_all(symbol: str, include_regime: bool = False) -> GateResult:
    """
    Lightweight evaluation with no network calls.
    Returns GateResult(allow=bool, reasons=[...], detail={...})
    """
    res = GateResult(allow=True)
    _gate_breaker(res)
    _gate_trading_window(res)
    _gate_symbol_whitelist(symbol, res)
    _gate_daily_halt(res)
    _gate_funding(symbol, res)
    _hint_spread_cap(res)

    # Optional regime check hook (kept off by default until you ask)
    if include_regime:
        try:
            ok, why = _regime.check_symbol(symbol)  # your existing function signature may differ
            if not ok:
                res.allow = False
                res.reasons.append(f"regime:{why or 'blocked'}")
        except Exception:
            # don’t brick just because regime gate isn’t wired
            pass

    res.detail["symbol"] = symbol.upper()
    res.detail["ts"] = _now_epoch()
    return res
