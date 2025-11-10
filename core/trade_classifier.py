#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/trade_classifier.py — tiny heuristic classifier
Returns one of: "trend", "breakout", "meanrev", "other"
"""

from __future__ import annotations
from typing import Dict

def classify(f: Dict) -> str:
    try:
        e20  = float(f.get("ema20", 0))
        e50  = float(f.get("ema50", 0))
        e200 = float(f.get("ema200", 0))
        close= float(f.get("close", 0))
        vz   = float(f.get("intra_vz", f.get("vz", 0)))
        if e20 > e50 > e200 or e20 < e50 < e200:
            return "trend"
        if vz > 1.0 and (close > max(e20, e50) or close < min(e20, e50)):
            return "breakout"
        if abs(close - e20) / (e20 or 1e-9) < 0.002 and abs(e20 - e50) / (e50 or 1e-9) < 0.002:
            return "meanrev"
        return "other"
    except Exception:
        return "other"
