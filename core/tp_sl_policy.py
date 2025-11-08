#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamic TP/SL policy for 5-rung ladders.
- spacing: front|linear|back chosen by class & volatility
- returns list of TP offsets (bps) and SL offset (bps or atr mult)
"""
from __future__ import annotations
from typing import Dict, List, Tuple

def _linear(bps: float, n: int=5) -> List[float]:
    step = bps
    return [step*i for i in range(1, n+1)]

def _frontload(bps: float, n: int=5) -> List[float]:
    base = _linear(bps, n)
    return [x*0.7 if i<3 else x*1.4 for i,x in enumerate(base)]

def _backload(bps: float, n: int=5) -> List[float]:
    base = _linear(bps, n)
    return [x*1.4 if i>=3 else x*0.6 for i,x in enumerate(base)]

def pick_policy(trade_class: str, atr_bps: float, signal_strength: float=1.0) -> Dict:
    # base spacing ~ ATR; clamp
    base_bps = max(8.0, min(atr_bps*0.8, 120.0))
    if trade_class == "trend":
        spacing = _backload(base_bps)
        sl_mode = {"type":"ATR", "mult": 2.5}
    elif trade_class == "breakout":
        spacing = _frontload(base_bps*0.9)
        sl_mode = {"type":"ATR", "mult": 2.0}
    else:
        spacing = _linear(base_bps*0.7)
        sl_mode = {"type":"ATR", "mult": 1.8}
    return {"tp_bps": spacing, "sl": sl_mode}

def ratchet_remaining(tp_prices: List[float], mfe_bps: float) -> List[float]:
    """If MFE large, push remaining rungs outward a bit."""
    if mfe_bps < 2*min(12.0, tp_prices[0] if tp_prices else 12.0):
        return tp_prices
    return [p*(1.0 + 0.10) for p in tp_prices]  # +10% stretch
