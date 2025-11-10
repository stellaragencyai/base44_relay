#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/feature_extractor.py — trade feature analysis & setup tagging

Purpose:
- Load raw trade features (from core/feature_store or live context)
- Derive secondary metrics: volatility regimes, momentum bursts, mean-reversion scores
- Tag setups by pattern match ("Breakout", "Pullback", "Range-Fade", "Momentum-Spike", "Vol-Crush")
- Return a normalized feature dict ready for ML, ranking, or similarity checks
"""

from __future__ import annotations
import math, statistics, time
from typing import Dict, Any

def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        if b == 0:
            return default
        return a / b
    except Exception:
        return default

def normalize_feature_set(features: Dict[str, Any]) -> Dict[str, float]:
    """Convert raw mixed-type feature dict into normalized floats."""
    norm = {}
    for k, v in features.items():
        try:
            val = float(v)
            if math.isnan(val) or math.isinf(val):
                val = 0.0
            norm[k] = val
        except Exception:
            # skip non-numeric safely
            continue
    return norm

def derive_secondary_features(base: Dict[str, Any]) -> Dict[str, float]:
    """Compute derived features like volatility ratios, momentum slope, and volume spikes."""
    f = normalize_feature_set(base)
    out = {}

    # volatility & momentum
    atr = f.get("atr", f.get("atr_pct", 0))
    adx = f.get("adx", 0)
    vol_z = f.get("vol_z", 0)
    price_chg = f.get("price_change_pct", 0)
    slope = f.get("trend_slope", 0)

    out["volatility_score"] = min(1.0, abs(atr) / 0.05)            # normalized to 5% ATR scale
    out["momentum_score"] = min(1.0, abs(price_chg) / 0.05)         # strong if >5% change
    out["volume_intensity"] = max(-3, min(3, vol_z))                # clamp z-score
    out["trend_strength"] = min(1.0, adx / 50.0)
    out["slope_sign"] = 1 if slope > 0 else -1 if slope < 0 else 0
    out["mean_reversion"] = max(0.0, 1 - abs(price_chg) / 0.05)     # strong mean reversion if no change
    return out

def tag_setup(features: Dict[str, Any]) -> str:
    """
    Assign setup type based on feature constellation.
    """
    f = derive_secondary_features(features)
    vol = f["volatility_score"]
    mom = f["momentum_score"]
    volz = f["volume_intensity"]
    slope = f["slope_sign"]
    trend = f["trend_strength"]
    meanrev = f["mean_reversion"]

    # crude but effective heuristic tree
    if mom > 0.7 and vol > 0.7 and trend > 0.6 and slope > 0:
        return "Breakout"
    elif mom < 0.4 and trend > 0.5 and slope > 0:
        return "Pullback"
    elif meanrev > 0.6 and vol < 0.4 and abs(volz) < 1.5:
        return "Range-Fade"
    elif volz > 1.8 and mom > 0.6:
        return "Momentum-Spike"
    elif vol < 0.3 and trend < 0.3:
        return "Vol-Crush"
    else:
        return "Unclassified"

def extract_and_tag(features: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine normalized + derived + setup tag into one dict.
    """
    f = normalize_feature_set(features)
    f.update(derive_secondary_features(f))
    f["setup_tag"] = tag_setup(f)
    f["timestamp_ms"] = int(time.time() * 1000)
    return f
