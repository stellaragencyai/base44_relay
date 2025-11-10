#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Forward-return labeler. Given entry px and horizon N minutes, returns 1 if
max favorable excursion hits +R before -R; else 0. Fallback: sign(px_T - px) > 0.
"""

from __future__ import annotations
from typing import Dict, List
from core.ai.featurizer import _fetch_kline

def label_forward(symbol: str, entry_px: float, side: str, horizon_min: int = 30, r_mult: float = 1.0, atr: float = 0.0) -> int:
    rows = _fetch_kline(symbol, "1", max(60, horizon_min+5))
    if not rows or entry_px <= 0: return 0
    # crude: use ATR if provided, else 0.25% of price as R
    R = atr if atr > 0 else entry_px * 0.0025
    up  = entry_px + r_mult*R
    dn  = entry_px - r_mult*R
    for it in rows[:horizon_min]:
        h = float(it[2]); l = float(it[3])
        if side == "Buy":
            if h >= up: return 1
            if l <= dn: return 0
        else:
            if l <= dn: return 1
            if h >= up: return 0
    # fallback: simple terminal outcome
    last = float(rows[min(horizon_min-1, len(rows)-1)][4])
    return 1 if ((last-entry_px) > 0 and side=="Buy") or ((entry_px-last) > 0 and side=="Sell") else 0
