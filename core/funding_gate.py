#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/funding_gate.py — Funding proximity lockout for perpetuals (Bybit cadence)
- Bybit funding occurs every 8 hours at 00:00, 08:00, 16:00 UTC.
- We lock new entries within FUNDING_LOCKOUT_MIN minutes before the next funding.
- Pure time math (no API dependency), so it works even when relay/public endpoints sulk.

Env:
  FUNDING_LOCKOUT_MIN=10        # minutes before funding to block new entries (default 10)
  FUNDING_LOCKOUT_ENABLE=true   # master toggle
"""

from __future__ import annotations
import os, time
from dataclasses import dataclass
from typing import Optional, Tuple

_LOCKOUT_MIN = int(os.getenv("FUNDING_LOCKOUT_MIN", "10") or "10")
_ENABLED = (os.getenv("FUNDING_LOCKOUT_ENABLE", "true").strip().lower() in {"1","true","yes","on"})

@dataclass(frozen=True)
class FundingWindow:
    next_epoch: int         # unix seconds of next funding
    minutes_to_next: int    # ceil minutes to that stamp

def _utc_now() -> int:
    return int(time.time())

def _next_funding_epoch(now_utc: Optional[int] = None) -> int:
    """
    Bybit: funding at 00:00, 08:00, 16:00 UTC (every 8h).
    Compute the next stamp >= now.
    """
    if now_utc is None:
        now_utc = _utc_now()
    # round down to the last 8h boundary
    eight_h = 8 * 3600
    # Align epoch 0 to a boundary for simplicity
    last_boundary = (now_utc // eight_h) * eight_h
    next_boundary = last_boundary if now_utc == last_boundary else last_boundary + eight_h
    return next_boundary

def minutes_to_next(now_utc: Optional[int] = None) -> int:
    nxt = _next_funding_epoch(now_utc)
    remain_sec = max(0, nxt - (now_utc if now_utc is not None else _utc_now()))
    # ceil to minutes
    return (remain_sec + 59) // 60

def funding_window(now_utc: Optional[int] = None) -> FundingWindow:
    nxt = _next_funding_epoch(now_utc)
    return FundingWindow(next_epoch=nxt, minutes_to_next=minutes_to_next(now_utc))

def is_lockout(symbol: str, now_utc: Optional[int] = None) -> Tuple[bool, FundingWindow]:
    """
    Returns (blocked, window) where blocked is True if we’re within lockout minutes.
    Symbol is accepted for future per-exchange overrides; currently unused.
    """
    win = funding_window(now_utc)
    if not _ENABLED:
        return False, win
    return (win.minutes_to_next <= _LOCKOUT_MIN), win
