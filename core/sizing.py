#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sizing helpers:
- bayesian_size: adjusts size given prior win prob and new evidence
- risk_capped_qty: caps base qty using remaining risk budget and stop distance
"""
from __future__ import annotations
from typing import Optional

def bayesian_size(qty: float, prior_win_p: float, evidence_win_p: float, k: float=1.0) -> float:
    prior = max(1e-6, min(1-1e-6, prior_win_p))
    ev    = max(1e-6, min(1-1e-6, evidence_win_p))
    post  = (prior*ev) / (prior*ev + (1-prior)*(1-ev))
    scale = 0.5 + k*(post - 0.5)  # scale ~ [0,1], centered at 0.5
    return max(0.0, qty * max(0.1, min(2.0, scale)))

def risk_capped_qty(remaining_risk_usd: float, stop_dist_px: float, px: float, min_qty: float) -> float:
    if stop_dist_px <= 0 or px <= 0:
        return max(min_qty, 0.0)
    qty = remaining_risk_usd / stop_dist_px
    return max(min_qty, qty)
