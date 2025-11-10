#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/sizing.py — position sizing helpers for Base44

Exports
  bayesian_size(base_qty, prior_win_p, evidence_win_p, k=0.8) -> float
  risk_capped_qty(remaining_risk_usd, stop_dist_px, px, min_qty=0.0) -> float

Notes
- bayesian_size nudges up/down around base_qty using simple Bayesian update mapped to a multiplier.
- risk_capped_qty converts a USD risk budget into a quantity given price and stop distance.
"""

from __future__ import annotations
from math import isfinite

def _clamp(x: float, lo: float, hi: float) -> float:
    return hi if x > hi else lo if x < lo else x

def bayesian_size(base_qty: float,
                  prior_win_p: float,
                  evidence_win_p: float,
                  k: float = 0.8) -> float:
    """
    base_qty          : baseline quantity (already notional or fixed base)
    prior_win_p       : prior probability of a win (0..1)
    evidence_win_p    : evidence-derived win prob (0..1)
    k                 : gain factor for how aggressively to scale (0..1.5 typical)

    Returns base_qty scaled by a factor in ~[0.5, 1.5] depending on evidence vs 0.5.
    """
    try:
        p0 = _clamp(float(prior_win_p), 0.0, 1.0)
        pe = _clamp(float(evidence_win_p), 0.0, 1.0)
        base = max(0.0, float(base_qty))
        kk = max(0.0, float(k))

        # simple log-odds style nudge around 1.0
        center = 0.5*(p0 + pe)
        # map [0,1] -> [-1,1], then scale, then clamp multiplier
        tilt = (center - 0.5) * 2.0
        mult = _clamp(1.0 + kk * tilt, 0.5, 1.5)
        out = base * mult
        return out if isfinite(out) else 0.0
    except Exception:
        return max(0.0, float(base_qty))

def risk_capped_qty(*,
                    remaining_risk_usd: float | None,
                    stop_dist_px: float,
                    px: float,
                    min_qty: float = 0.0) -> float:
    """
    remaining_risk_usd : how many USD you're allowed to lose if stop hits (None -> 0)
    stop_dist_px       : absolute price distance between entry and stop (must be > 0)
    px                 : current/entry price (must be > 0)
    min_qty            : floor to avoid dust

    qty = max( min_qty, remaining_risk_usd / stop_dist_px / px )
    """
    try:
        risk = float(remaining_risk_usd or 0.0)
        dist = float(stop_dist_px)
        price = float(px)
        if risk <= 0 or dist <= 0 or price <= 0:
            return 0.0
        q = risk / dist / price
        if not isfinite(q):
            return 0.0
        return max(float(min_qty), max(0.0, q))
    except Exception:
        return 0.0
