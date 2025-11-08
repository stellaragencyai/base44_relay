#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade lifecycle classifier.
Returns: "trend", "breakout", "meanrev" based on features. Pluggable.
"""
from __future__ import annotations
from typing import Dict

def classify(features: Dict) -> str:
    slope = float(features.get("trend_slope", 0.0))
    volz  = float(features.get("vol_z", 0.0))
    rvbps = float(features.get("realized_vol_bps", 0.0))
    # crude but effective baseline:
    if slope > 0.0005 and volz > 0.5:
        return "trend"
    if rvbps > 25 and volz > 0.8:
        return "breakout"
    return "meanrev"
