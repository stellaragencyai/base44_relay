#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Classifier facade: returns a class name, and stashes a probability for sizing.
"""

from __future__ import annotations
from typing import Dict, Tuple

def _heuristic(features: Dict) -> str:
    vz = float(features.get("vol_z", 0.0))
    if vz > 1.2: return "Breakout"
    if features.get("mom20", 0.0) > 0 and features.get("sma20",0.0) > features.get("sma50",0.0):
        return "TrendPullback"
    return "Default"

_last_p = 0.55

def classify(features: Dict) -> str:
    global _last_p
    # Try AI inference if caller provided symbol
    sym = features.get("_symbol")
    if sym:
        try:
            from core.ai.infer import score_symbol
            r = score_symbol(str(sym))
            _last_p = float(r.get("p_win", 0.55))
            # merge returned features into current dict for logging
            for k,v in (r.get("features") or {}).items():
                features.setdefault(k, v)
        except Exception:
            _last_p = 0.55
    return _heuristic(features)

def get_last_p() -> float:
    return _last_p
