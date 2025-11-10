#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/sizing.py — risk-aware quantity helpers

Exports:
- bayesian_size(base_qty, prior_win_p, evidence_win_p, k)
- risk_capped_qty(remaining_risk_usd, stop_dist_px, px, min_qty=0.0)
"""

from __future__ import annotations
from typing import Optional
import math

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def bayesian_size(base_qty: float,
                  prior_win_p: float = 0.55,
                  evidence_win_p: float = 0.55,
                  k: float = 0.8) -> float:
    """
    Combine prior and evidence into a posterior-ish confidence and nudge size.
    Multiplier is centered at 1.0 when p=0.5. Clamped to keep it sane.

    multiplier = 1 + k * (post - 0.5) * 2  → post ∈ [0,1], k ∈ [0, 1.5]
    """
    try:
        prior = _clip(float(prior_win_p), 0.01, 0.99)
        evid  = _clip(float(evidence_win_p), 0.01, 0.99)
        # Log-odds blending
        lo_prior = math.log(prior/(1-prior))
        lo_evid  = math.log(evid/(1-evid))
        lo_post  = (lo_prior + lo_evid) / 2.0
        post     = 1.0 / (1.0 + math.exp(-lo_post))
        mult     = 1.0 + float(k) * (post - 0.5) * 2.0
        mult     = _clip(mult, 0.2, 3.0)
        out      = max(0.0, float(base_qty) * mult)
        return out
    except Exception:
        return max(0.0, float(base_qty))

def risk_capped_qty(remaining_risk_usd: Optional[float],
                    stop_dist_px: float,
                    px: float,
                    min_qty: float = 0.0) -> float:
    """
    Basic stop-based sizing:
      qty = remaining_risk_usd / stop_dist_px
    Safeguards and clamping included.
    """
    try:
        risk = float(remaining_risk_usd if remaining_risk_usd is not None else 0.0)
        stop = max(1e-9, float(stop_dist_px))
        q    = max(float(min_qty), risk / stop)
        # If price is nonsense, still return base qty
        return max(0.0, q)
    except Exception:
        return max(0.0, float(min_qty))
