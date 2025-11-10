#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.guard — centralized breaker (single source of truth) • Phase-4

Responsibilities
- Single guard API for all bots:
    * guard_blocking_reason() -> (blocked: bool, reason: str)
    * guard_set(reason, ttl_sec=..., meta=None)        # turn ON (back-compat)
    * guard_clear(note=None)                            # turn OFF
    * guard_gate(bot, action) -> context manager
    * guard_trip(reason="policy")                       # explicit trip + cooldown
    * guard_reset_session(start_equity_usd=0.0)         # reset day/session in DB

- Persistence & policy
    • Mirrors breaker state in DB (core.db.guard_state).
    • Optional file state for TTL and cross-process cache: .state/guard_state.json
    • Reads risk policy from cfg/guard_policy.yaml (if present) with env overrides:
        GUARD_POLICY_PATH=cfg/guard_policy.yaml
        GUARD_MAX_DAILY_LOSS_PCT (default 3.0)
        GUARD_MAX_REALIZED_LOSS_USD (default 0.0 disabled)
        GUARD_GROSS_EXPOSURE_CAP_PCT (default 65.0)
        GUARD_COOLDOWN_MIN (default 45)
        GUARD_REQUIRE_EQUITY (default false)

- Signals
    • Quiet Telegram ping on state changes (best-effort).
    • Plays nice with executor/tp_sl_manager/reconciler/outcome_watcher which already call guard_blocking_reason().

Notes
- DB schema is provided by core.db.migrate() and includes breaker_on/breaker_reason.
- Equity and gross exposure are fetched best-effort via Bybit.
"""

from __future__ import annotations
import json
import os
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ---- tolerant settings import ----
try:
    from core.config import settings
except Exception:  # minimal fallback for early boot
    class _S:
        ROOT = Path(os.getcwd())
    settings = _S()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(os.getcwd()))
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---- optional DB guard API ----
_DB_OK = True
try:
    from core.db import (
        guard_load,          # -> dict(session_start_ms, start_equity_usd, realized_pnl_usd, breaker_on, breaker_reason, updated_ts, breach legacy)
        guard_set_breaker,   # (active: bool, reason: str="")
        guard_reset_day,     # (start_equity_usd: float=0.0)
    )
except Exception:
    _DB_OK = False
    def guard_load() -> Dict[str, Any]:   # type: ignore
        return {
            "session_start_ms": 0,
            "start_equity_usd": 0.0,
            "realized_pnl_usd": 0.0,
            "breach": False,
            "breaker_on": False,
            "breaker_reason": "",
            "updated_ts": 0,
        }
    def guard_set_breaker(active: bool, reason: str = "") -> None:  # type: ignore
        pass
    def guard_reset_day(start_equity_usd: float = 0.0) -> None:     # type: ignore
        pass

# ---- Bybit client (optional, for equity/gross) ----
try:
    from core.bybit_client import Bybit
except Exception:
    Bybit = None  # type: ignore

# ---- notifier (best-effort, never crash) ----
try:
    from tools.notifier_telegram import tg
    def _tg(text: str):
        try:
            tg.safe_text(text, quiet=True)
        except Exception:
            pass
except Exception:
    def _tg(_: str):
        pass

# =========================
# Config & Policy
# =========================

GUARD_FILE = Path(getattr(settings, "GUARD_STATE_FILE", STATE_DIR / "guard_state.json"))
GUARD_NOTIFY = str(getattr(settings, "GUARD_NOTIFY", "true")).lower() in ("1","true","yes","on")
GUARD_PREFIX = str(getattr(settings, "GUARD_PREFIX", "[B44]"))

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","on")

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None: return default
    try: return float(v)
    except Exception: return default

def _load_yaml_policy(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # optional
    except Exception:
        return {}
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

_POLICY_PATH = Path(os.getenv("GUARD_POLICY_PATH", "cfg/guard_policy.yaml"))
_YAML = _load_yaml_policy(_POLICY_PATH)

MAX_DD_PCT   = float(_YAML.get("max_daily_loss_pct", _env_float("GUARD_MAX_DAILY_LOSS_PCT", 3.0)))
MAX_LOSS_USD = float(_YAML.get("max_realized_loss_usd", _env_float("GUARD_MAX_REALIZED_LOSS_USD", 0.0)))
GROSS_CAP_P  = float(_YAML.get("gross_exposure_cap_pct", _env_float("GUARD_GROSS_EXPOSURE_CAP_PCT", 65.0)))
COOLDOWN_MIN = int(_YAML.get("cooldown_min", _env_float("GUARD_COOLDOWN_MIN", 45)))
REQ_EQUITY   = bool(_YAML.get("require_equity", _env_bool("GUARD_REQUIRE_EQUITY", False)))

# =========================
# Local state store (file)
# =========================

_lock = threading.RLock()

_state: Dict[str, Any] = {
    "blocked": False,
    "reason": "",
    "meta": {},
    "since_ts": 0,        # epoch seconds when breaker turned ON
    "expires_ts": 0,      # epoch seconds when breaker should auto-clear; 0 = no TTL
    "last_change_ts": 0,  # epoch seconds of last state change (on/off)
    "version": 2,
}

def _now() -> int:
    return int(time.time())

def _load_state() -> None:
    global _state
    try:
        if GUARD_FILE.exists():
            js = json.loads(GUARD_FILE.read_text(encoding="utf-8"))
            if isinstance(js, dict):
                _state.update(js)
    except Exception:
        pass

def _save_state() -> None:
    try:
        tmp = GUARD_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(GUARD_FILE)
    except Exception:
        pass

# =========================
# Equity / Exposure taps
# =========================

def _get_equity_and_gross() -> Tuple[Optional[float], Optional[float]]:
    """Best-effort fetch from Bybit; returns (equity_usd, gross_usd)."""
    if Bybit is None:
        return None, None
    by = Bybit()
    try:
        by.sync_time()
    except Exception:
        pass

    eq = None
    ok, data, err = by._request_private_json("/v5/account/wallet-balance", params={"accountType":"UNIFIED"})
    if ok and isinstance(data, dict):
        try:
            eq = sum(float(acc.get("totalEquity") or 0.0) for acc in (data.get("result") or {}).get("list") or [])
        except Exception:
            eq = None

    gross = 0.0
    ok2, d2, _ = by.get_positions(category="linear")
    if ok2 and isinstance(d2, dict):
        try:
            for p in (d2.get("result") or {}).get("list") or []:
                sz = float(p.get("size") or 0)
                px = float(p.get("avgPrice") or 0)
                gross += abs(sz * px)
        except Exception:
            pass
    else:
        gross = None

    return eq, gross

# =========================
# Core API
# =========================

# Internal helper to format reasons consistently
def _r(txt: str) -> str:
    return f"guard: {txt}"

# Cooldown tracking (derived from DB or file state)
_last_trip_ts: Optional[float] = None

def _cooldown_active(now: float, db_state: Dict[str, Any]) -> bool:
    global _last_trip_ts
    # if DB shows breaker_on and we have no timestamp yet, seed a recent trip
    if db_state.get("breaker_on") and _last_trip_ts is None:
        _last_trip_ts = now - 1
    if _last_trip_ts is None:
        return False
    return (now - _last_trip_ts) < (COOLDOWN_MIN * 60.0)

def _maybe_autoclear_locked() -> None:
    # Called under _lock; applies only to file TTL and mirrors DB clear if possible.
    if not _state.get("blocked"):
        return
    exp = int(_state.get("expires_ts") or 0)
    if exp > 0 and _now() >= exp:
        _state["blocked"] = False
        _state["reason"] = ""
        _state["meta"] = {}
        _state["since_ts"] = 0
        _state["expires_ts"] = 0
        _state["last_change_ts"] = _now()
        _save_state()
        # mirror DB clear on TTL auto-clear
        try:
            guard_set_breaker(False, reason="")
        except Exception:
            pass
        if GUARD_NOTIFY:
            _tg(f"{GUARD_PREFIX} Breaker auto-cleared (TTL expired)")

def guard_blocking_reason() -> Tuple[bool, str]:
    """
    Returns (blocked, reason). Safe to call frequently from any bot.
    Decision order:
      1) If DB breaker_on: block. If cooldown window still active, say so.
      2) Apply policy checks: daily realized DD % and absolute USD loss.
      3) Apply gross exposure cap (equity/gross best-effort).
      4) Else allowed.
    Also performs TTL auto-clear for the local file state and mirrors DB OFF if TTL expired.
    """
    # Honor local TTL for back-compat callers that only ever set guard_set(..., ttl_sec)
    with _lock:
        _maybe_autoclear_locked()

    st = guard_load()  # DB mirror
    now = time.time()

    # If DB breaker is on, we block regardless of file TTL
    if st.get("breaker_on"):
        if _cooldown_active(now, st):
            return True, _r("cooldown active")
        reason = st.get("breaker_reason") or "manual"
        return True, _r(reason)

    # Policy checks (drawdown)
    start_eq = float(st.get("start_equity_usd") or 0.0)
    realized = float(st.get("realized_pnl_usd") or 0.0)

    dd_pct = (-realized / start_eq) * 100.0 if start_eq > 0 else 0.0
    if MAX_LOSS_USD > 0 and (-realized) >= MAX_LOSS_USD:
        return True, _r(f"realized_loss_usd {-realized:.2f} >= {MAX_LOSS_USD:.2f}")
    if MAX_DD_PCT > 0 and dd_pct >= MAX_DD_PCT:
        return True, _r(f"daily_dd {dd_pct:.2f}% >= {MAX_DD_PCT:.2f}%")

    # Gross exposure cap
    if GROSS_CAP_P > 0:
        eq, gross = _get_equity_and_gross()
        if REQ_EQUITY and (eq is None or eq <= 0):
            return True, _r("equity_unavailable")
        if eq and gross is not None and eq > 0:
            if (gross / eq) * 100.0 >= GROSS_CAP_P:
                return True, _r(f"gross {gross/eq*100:.1f}% >= cap {GROSS_CAP_P:.0f}%")

    # Back-compat: if local file breaker is on but DB isn't, we still block for legacy users
    with _lock:
        if bool(_state.get("blocked")):
            return True, _r(str(_state.get("reason") or "file_breaker"))

    return False, ""

def guard_set(reason: str, ttl_sec: int = 0, meta: Optional[Dict[str, Any]] = None) -> None:
    """
    Back-compat: turn breaker ON with a human reason and optional TTL.
    Mirrors to DB and records local TTL (if provided).
    """
    if not reason:
        reason = "breaker_on"
    expires_ts = (_now() + int(ttl_sec)) if ttl_sec and ttl_sec > 0 else 0

    # DB first (source of truth for bots)
    try:
        guard_set_breaker(True, reason=str(reason))
    except Exception:
        pass

    # Local cache + TTL
    with _lock:
        _state["blocked"] = True
        _state["reason"] = reason
        _state["meta"] = dict(meta or {})
        _state["since_ts"] = _state["since_ts"] or _now()
        _state["expires_ts"] = max(int(_state.get("expires_ts") or 0), expires_ts)
        _state["last_change_ts"] = _now()
        _save_state()

    if GUARD_NOTIFY:
        _tg(f"{GUARD_PREFIX} Breaker ON • {reason}" + (f" • ttl={ttl_sec}s" if ttl_sec and ttl_sec > 0 else ""))

def guard_clear(note: Optional[str] = None) -> None:
    """
    Turn breaker OFF immediately (DB + local).
    """
    # DB off
    try:
        guard_set_breaker(False, reason="")
    except Exception:
        pass

    with _lock:
        was_on = bool(_state.get("blocked"))
        _state["blocked"] = False
        _state["reason"] = ""
        _state["meta"] = {}
        _state["since_ts"] = 0
        _state["expires_ts"] = 0
        _state["last_change_ts"] = _now()
        _save_state()

    if GUARD_NOTIFY:
        msg = f"{GUARD_PREFIX} Breaker OFF"
        if note:
            msg += f" • {note}"
        _tg(msg)

# Explicit trip with cooldown semantics (preferred by tooling/ops)
def guard_trip(reason: str = "policy") -> None:
    """
    Trip the breaker and start cooldown timer. Bots will see guard_blocking_reason() == blocked.
    """
    global _last_trip_ts
    _last_trip_ts = time.time()
    guard_set(reason=reason, ttl_sec=0, meta={"cooldown_min": COOLDOWN_MIN})

def guard_reset_session(start_equity_usd: float = 0.0) -> None:
    """
    Reset daily/session anchors in DB and clear breaker state.
    """
    try:
        guard_reset_day(start_equity_usd=float(start_equity_usd))
    finally:
        guard_clear(note="session reset")

# Context manager for quick gating
@contextmanager
def guard_gate(bot: str = "bot", action: str = "op"):
    """
    with guard_gate(bot="executor", action="place") as allowed:
        if not allowed: return
        ...
    """
    blocked, _ = guard_blocking_reason()
    try:
        yield not blocked
    finally:
        pass

# Convenience helpers
def breaker_on() -> bool:
    b, _ = guard_blocking_reason()
    return b

def breaker_reason() -> str:
    _, r = guard_blocking_reason()
    return r

# Optional metrics tap
def guard_metrics() -> Dict[str, Any]:
    st = guard_load()
    eq, gross = _get_equity_and_gross()
    blocked, why = guard_blocking_reason()
    return {
        "blocked": blocked,
        "reason": why,
        "start_equity_usd": st.get("start_equity_usd", 0.0),
        "realized_pnl_usd": st.get("realized_pnl_usd", 0.0),
        "breaker_on_db": bool(st.get("breaker_on")),
        "equity_usd": eq,
        "gross_usd": gross,
        "cooldown_min": COOLDOWN_MIN,
        "policy": {
            "max_daily_loss_pct": MAX_DD_PCT,
            "max_realized_loss_usd": MAX_LOSS_USD,
            "gross_exposure_cap_pct": GROSS_CAP_P,
            "require_equity": REQ_EQUITY,
        },
    }

# Initialize state at import and run one TTL autoclear pass
_load_state()
with _lock:
    _maybe_autoclear_locked()
