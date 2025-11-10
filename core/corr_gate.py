#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/corr_gate.py — correlation gate (minimal)
Default: allow everything. You can tighten later.

Env:
  CORR_DISABLE=true            -> always allow
  CORR_BLOCKLIST=BTCUSDT,PEPEUSDT
"""

from __future__ import annotations
import os

_BLOCK = {s.strip().upper() for s in (os.getenv("CORR_BLOCKLIST","") or "").split(",") if s.strip()}
_ALWAYS_ALLOW = str(os.getenv("CORR_DISABLE","false")).lower() in ("1","true","yes","on")

def allow(symbol: str) -> bool:
    if _ALWAYS_ALLOW:
        return True
    if symbol and symbol.upper() in _BLOCK:
        return False
    # TODO: plug real correlation logic here when you actually want it
    return True
