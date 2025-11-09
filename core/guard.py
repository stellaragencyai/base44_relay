#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.guard — centralized breaker (single source of truth)

Responsibilities
- Keep a global "breaker" state: ON means trading is blocked (except safety tasks).
- Persist state to disk for cross-process visibility.
- Provide a simple API used everywhere:
    * guard_blocking_reason() -> (blocked: bool, reason: str)
    * guard_set(reason, ttl_sec=..., meta=None)  # turn ON with TTL
    * guard_clear(note=None)                      # turn OFF
    * guard_gate(bot, action) -> context manager yielding allowed: bool
- Optional Telegram notification (quiet) when toggled.

Environment (via core.config.settings or .env):
    GUARD_STATE_FILE=.state/guard_state.json
    GUARD_NOTIFY=true
    GUARD_PREFIX=[B44]
"""

from __future__ import annotations
import json
import os
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    from core.config import settings
except Exception:  # minimal fallback if settings is not available during early boot
    class _S:
        ROOT = Path(os.getcwd())
    settings = _S()  # type: ignore

# Optional notifier (best-effort, never crash)
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

ROOT: Path = getattr(settings, "ROOT", Path(os.getcwd()))
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

GUARD_FILE = Path(getattr(settings, "GUARD_STATE_FILE", STATE_DIR / "guard_state.json"))
GUARD_NOTIFY = str(getattr(settings, "GUARD_NOTIFY", "true")).lower() in ("1","true","yes","on")
GUARD_PREFIX = str(getattr(settings, "GUARD_PREFIX", "[B44]"))

_lock = threading.RLock()

_state: Dict[str, Any] = {
    "blocked": False,
    "reason": "",
    "meta": {},
    "since_ts": 0,        # epoch seconds when breaker turned ON
    "expires_ts": 0,      # epoch seconds when breaker should auto-clear; 0 = no TTL
    "last_change_ts": 0,  # epoch seconds of last state change (on/off)
    "version": 1,
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
        # ignore corrupt file; keep defaults
        pass

def _save_state() -> None:
    try:
        tmp = GUARD_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(GUARD_FILE)
    except Exception:
        pass

def _maybe_autoclear_locked() -> None:
    # Called under _lock
    if not _state.get("blocked"):
        return
    exp = int(_state.get("expires_ts") or 0)
    if exp > 0 and _now() >= exp:
        # TTL expired
        _state["blocked"] = False
        _state["reason"] = ""
        _state["meta"] = {}
        _state["since_ts"] = 0
        _state["expires_ts"] = 0
        _state["last_change_ts"] = _now()
        _save_state()
        if GUARD_NOTIFY:
            _tg(f"{GUARD_PREFIX} Breaker auto-cleared (TTL expired)")

def guard_blocking_reason() -> Tuple[bool, str]:
    """
    Returns (blocked, reason). Safe to call frequently from any bot.
    Performs TTL auto-clear if needed.
    """
    with _lock:
        _maybe_autoclear_locked()
        return bool(_state.get("blocked")), str(_state.get("reason") or "")

def guard_set(reason: str, ttl_sec: int = 0, meta: Optional[Dict[str, Any]] = None) -> None:
    """
    Turn breaker ON with a human reason. Optional TTL in seconds after which it auto-clears.
    """
    if not reason:
        reason = "breaker_on"
    expires_ts = (_now() + int(ttl_sec)) if ttl_sec and ttl_sec > 0 else 0
    with _lock:
        # If already blocked with same reason and longer/equal TTL, do nothing
        already = bool(_state.get("blocked"))
        same_reason = (str(_state.get("reason") or "") == reason)
        prev_exp = int(_state.get("expires_ts") or 0)

        _state["blocked"] = True
        _state["reason"] = reason
        _state["meta"] = dict(meta or {})
        _state["since_ts"] = _state["since_ts"] or _now()
        _state["expires_ts"] = max(prev_exp, expires_ts)
        _state["last_change_ts"] = _now()
        _save_state()

    if GUARD_NOTIFY and not (already and same_reason and expires_ts <= prev_exp):
        _tg(f"{GUARD_PREFIX} Breaker ON • {reason}"
            + (f" • ttl={ttl_sec}s" if ttl_sec and ttl_sec > 0 else ""))

def guard_clear(note: Optional[str] = None) -> None:
    """
    Turn breaker OFF immediately.
    """
    with _lock:
        was_on = bool(_state.get("blocked"))
        _state["blocked"] = False
        _state["reason"] = ""
        _state["meta"] = {}
        _state["since_ts"] = 0
        _state["expires_ts"] = 0
        _state["last_change_ts"] = _now()
        _save_state()
    if was_on and GUARD_NOTIFY:
        _tg(f"{GUARD_PREFIX} Breaker OFF"
            + (f" • {note}" if note else ""))

# Backwards-compat shim used in some older files
def set_breaker(on: bool, reason: str = "", ttl_sec: int = 0, meta: Optional[Dict[str, Any]] = None) -> None:
    if on:
        guard_set(reason or "breaker_on", ttl_sec=ttl_sec, meta=meta)
    else:
        guard_clear(note=reason or None)

@contextmanager
def guard_gate(bot: str = "bot", action: str = "op"):
    """
    Context manager that yields allowed: bool (False when breaker ON).
    Usage:
        with guard_gate(bot="executor", action="place") as allowed:
            if not allowed: return
            ...
    """
    blocked, why = guard_blocking_reason()
    allowed = not blocked
    try:
        yield allowed
    finally:
        # no-op; placeholder if you later want per-op telemetry
        pass

# Convenience helpers
def breaker_on() -> bool:
    b, _ = guard_blocking_reason()
    return b

def breaker_reason() -> str:
    _, r = guard_blocking_reason()
    return r

# Initialize state at import
_load_state()
# Try TTL autoclear once on import
with _lock:
    _maybe_autoclear_locked()
