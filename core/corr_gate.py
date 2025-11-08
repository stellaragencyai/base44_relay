#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Correlation/confirmation gate: require supportive regime from majors.
Simple version: if BTC & ETH trend_slope < 0 and realized_vol low, block high-beta alts.
"""
from __future__ import annotations
import json
from pathlib import Path

STATE = Path("./.state/regime_state.json")

def allow(symbol: str) -> bool:
    try:
        st = json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        return True
    trend = float(st.get("trend_slope", 0.0))
    vol   = float(st.get("realized_vol_bps", 0.0))
    if symbol.upper() not in ("BTCUSDT","ETHUSDT"):
        if trend < -0.0004 and vol < 15:
            return False
    return True
