#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load latest model and score a symbol. Falls back to 0.55 if model missing.
"""

from __future__ import annotations
import os
from typing import Dict
from core.ai.featurizer import make_features
from core.ai.model_lr import load, predict_proba

MODEL_PATH = ".state/models/latest.json"

def score_symbol(symbol: str) -> Dict[str, float]:
    feats = make_features(symbol)
    if not os.path.exists(MODEL_PATH) or not feats:
        return {"p_win": 0.55, "features": feats or {}}
    try:
        m = load(MODEL_PATH)
        p = predict_proba(m, feats)
        return {"p_win": float(p), "features": feats}
    except Exception:
        return {"p_win": 0.55, "features": feats}
