#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Guard helpers (single source of truth for "can we act right now?")

Why this exists
- Every bot should obey the global breaker without duplicating logic.
- Uniform behavior: skip actions when breaker is ON, but keep heartbeating.

Quick use
    from core.guard import guard_blocking_reason

    blocked, why = guard_blocking_reason()
    if blocked:
        tg_send(f"🧯 Guarded: {why} — skipping", priority="warn")
        return

Extras
- guard_skip_notify(bot="signal_engine", action="emit")  # one-liner with optional Telegram notify
- require_safe()  # raises RuntimeError if breaker is active (for executors)
- guard_gate(bot="tp_sl", action="manage")  # context manager that yields allowed: bool
"""

from __future__ import annotations
from typing import Tuple, Iterator
import contextlib

# Breaker import with safe fallback
try:
    from core import breaker  # type: ignore
except Exception:  # pragma: no cover
    class _B:  # minimal fallback if breaker missing during dev
        @staticmethod
        def is_active() -> bool: return False
        @staticmethod
        def status() -> dict: return {"local_active": False, "db_active": False, "reason": ""}
    breaker = _B()  # type: ignore

# Optional notifier
try:
    from core.notifier_bot import tg_send  # type: ignore
except Exception:  # pragma: no cover
    def tg_send(msg: str, priority: str = "info", **_):  # type: ignore
        # Silent fallback to stdout to avoid import-time crashes
        print(f"[notify/{priority}] {msg}")

def guard_blocking_reason() -> Tuple[bool, str]:
    """
    Returns (blocked, reason). If blocked, caller should skip action but continue heartbeating.
    Reason is derived from breaker.status() with sane fallbacks.
    """
    if breaker.is_active():
        st = breaker.status()
        reason = (
            st.get("local_reason")
            or st.get("db_reason")
            or st.get("reason")
            or "risk_breaker_active"
        )
        return True, str(reason)
    return False, ""

def guard_skip_notify(bot: str = "", action: str = "", *, once: bool = False) -> bool:
    """
    Convenience: if blocked, emit a standardized notify and return True.
    Use when you want a one-liner gate at call sites.

        if guard_skip_notify("signal_engine", "emit"):
            return
    """
    blocked, why = guard_blocking_reason()
    if not blocked:
        return False
    # include minimal context; caller can throttle if noisy
    tag = f"{bot}/{action}".strip("/").strip()
    prefix = f"[{tag}] " if tag else ""
    tg_send(f"🧯 {prefix}Guarded: {why} — skipping", priority="warn")
    return True

def require_safe() -> None:
    """
    Hard gate: raise RuntimeError if breaker is ON.
    Intended for components that MUST NOT act under breaker, e.g., order executors.
    """
    blocked, why = guard_blocking_reason()
    if blocked:
        raise RuntimeError(f"Guard breaker active: {why}")

@contextlib.contextmanager
def guard_gate(bot: str = "", action: str = "") -> Iterator[bool]:
    """
    Context manager that yields `allowed: bool`. If blocked, sends a standardized notify once.

        with guard_gate("tp_sl", "manage") as allowed:
            if not allowed:
                return
            ... do the thing ...
    """
    if guard_skip_notify(bot=bot, action=action):
        yield False
    else:
        yield True
