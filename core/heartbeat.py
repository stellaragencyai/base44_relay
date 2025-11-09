#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# core/healthbeat.py
"""
Healthbeat — shared telemetry file for breaker auto-trip logic.

Writes JSON to .state/health.json, structured for core.breaker.auto_tick():

Schema (example)
{
  "ts": 1731123456,
  "bots": {
    "signal_engine": {"last": 1731123450, "critical": true, "extra": {"syms": 6}},
    "tp_sl_manager": {"last": 1731123448, "critical": true},
    "trade_executor": {"last": 1731123442, "critical": false}
  },
  "news_active": false,
  "funding_window": false,
  "relay_unhealthy": false,
  "exchange_unhealthy": false,
  "drawdown_pct": 1.23
}

Breaker integration (already in your breaker.py):
- NEWS_LOCKOUT          -> trips if news_active is true
- FUNDING_LOCKOUT_MIN   -> trips during funding_window
- CONNECTIVITY_LOCKOUT_SEC -> trips if relay_unhealthy or exchange_unhealthy
- DD_LOCKOUT_PCT        -> trips when drawdown_pct >= threshold
- HEARTBEAT_STALE_SEC   -> trips if any bots[...].critical heartbeat is stale

Use:
    from core.healthbeat import beat, set_flag, set_drawdown_pct, probe_and_set

    # in each loop:
    beat("signal_engine", critical=True, extra={"syms": len(SYMS)})

    # mark flows:
    set_flag("news_active", True)
    set_drawdown_pct(2.1)

Env:
  HEALTH_PATH=.state/health.json   # override path if you must
"""

from __future__ import annotations
import os, json, time, threading, contextlib
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

HEALTH_PATH = Path(os.getenv("BREAKER_HEALTH_PATH", str(STATE_DIR / "health.json")))
_TMP_PATH = HEALTH_PATH.with_suffix(".tmp")

_LOCK = threading.Lock()

def _now() -> int:
    return int(time.time())

def _load() -> Dict[str, Any]:
    try:
        if HEALTH_PATH.exists():
            return json.loads(HEALTH_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"ts": _now(), "bots": {}}

def _atomic_write(obj: Dict[str, Any]) -> None:
    obj["ts"] = _now()
    try:
        _TMP_PATH.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        _TMP_PATH.replace(HEALTH_PATH)
    except Exception:
        # best-effort; don’t throw inside bots
        pass

def _update(mutator) -> None:
    with _LOCK:
        d = _load()
        mutator(d)
        _atomic_write(d)

# --------------------------
# Public API
# --------------------------

def beat(name: str, *, critical: bool = True, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Update heartbeat for a bot/component.
    - name: stable key like "signal_engine", "tp_sl_manager", "trade_executor"
    - critical: if True, breaker HEARTBEAT_STALE_SEC applies
    - extra: small dict of diagnostics (kept shallow)
    """
    name = (name or "").strip() or "unnamed"
    extra = extra or {}

    def mutate(d: Dict[str, Any]) -> None:
        bots = d.setdefault("bots", {})
        rec = bots.get(name) or {}
        rec.update({"last": _now(), "critical": bool(critical)})
        if extra:
            # keep it small; avoid nesting unbounded structures
            rec["extra"] = {k: (v if isinstance(v, (int, float, str, bool)) else str(v)) for k, v in extra.items()}
        bots[name] = rec

    _update(mutate)

def set_flag(key: str, value: bool) -> None:
    """
    Set a boolean flag used by breaker.auto_tick():
      keys: news_active, funding_window, relay_unhealthy, exchange_unhealthy
    """
    key = str(key).strip()
    if key not in {"news_active", "funding_window", "relay_unhealthy", "exchange_unhealthy"}:
        # silently ignore unknown keys to avoid crashing bots
        return

    def mutate(d: Dict[str, Any]) -> None:
        d[key] = bool(value)

    _update(mutate)

def set_drawdown_pct(pct: float) -> None:
    """
    Set current drawdown percentage (0..100). Breaker can trip if DD_LOCKOUT_PCT <= value.
    """
    try:
        val = float(pct)
    except Exception:
        return

    def mutate(d: Dict[str, Any]) -> None:
        d["drawdown_pct"] = max(0.0, val)

    _update(mutate)

# Convenience: mark unhealthy for a short period based on probe results
def probe_and_set(*, relay_ok: Optional[bool] = None, exchange_ok: Optional[bool] = None) -> None:
    """
    Map external liveness probes into flags:
      relay_ok=False  -> relay_unhealthy=True
      exchange_ok=False -> exchange_unhealthy=True
    """
    def mutate(d: Dict[str, Any]) -> None:
        if relay_ok is not None:
            d["relay_unhealthy"] = not bool(relay_ok)
        if exchange_ok is not None:
            d["exchange_unhealthy"] = not bool(exchange_ok)

    _update(mutate)

# Context manager for long tasks that should refresh heartbeat while running
@contextlib.contextmanager
def life_support(name: str, *, critical: bool = True, period_sec: int = 20, extra: Optional[Dict[str, Any]] = None):
    """
    Keeps a background thread tapping beat(name) every period_sec while the context is open.
    Useful for one-shot jobs that shouldn’t look dead if they take a while.
    """
    stop = threading.Event()

    def _pumper():
        while not stop.is_set():
            beat(name, critical=critical, extra=extra)
            stop.wait(max(1, int(period_sec)))

    t = threading.Thread(target=_pumper, name=f"hb-{name}", daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=2.0)
        beat(name, critical=critical, extra=extra)  # final tick
