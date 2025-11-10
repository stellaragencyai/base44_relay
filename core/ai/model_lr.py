#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tiny logistic regression with L2, pure Python+numpy if available.
Stored as JSON: {"w": [...], "b": float, "features": ["f1","f2",...]}
"""

from __future__ import annotations
import json, math, os
from typing import Dict, List, Tuple

try:
    import numpy as np
    _NP = True
except Exception:
    _NP = False

FEATURES = ["atr14","mom20","rng20","vol_z","px","sma20","sma50"]

def _vec(x: Dict[str, float]) -> List[float]:
    return [float(x.get(k, 0.0)) for k in FEATURES]

def _sig(z: float) -> float:
    if z >= 0: return 1.0/(1.0+math.exp(-z))
    ez = math.exp(z); return ez/(1.0+ez)

def fit(X: List[Dict[str, float]], y: List[int], lr: float = 0.05, l2: float = 1e-4, iters: int = 400) -> Dict:
    w = [0.0]*len(FEATURES); b = 0.0
    for _ in range(iters):
        gw = [0.0]*len(FEATURES); gb = 0.0
        for xi, yi in zip(X, y):
            v = _vec(xi)
            z = sum(wi*vi for wi,vi in zip(w,v)) + b
            p = _sig(z)
            e = p - float(yi)
            for j in range(len(w)):
                gw[j] += e*v[j]
            gb += e
        n = max(1, len(X))
        for j in range(len(w)):
            w[j] -= lr*((gw[j]/n) + l2*w[j])
        b -= lr*(gb/n)
    return {"w": w, "b": b, "features": FEATURES}

def predict_proba(model: Dict, x: Dict[str, float]) -> float:
    feats = model.get("features", FEATURES)
    v = [float(x.get(k, 0.0)) for k in feats]
    z = sum(wi*vi for wi,vi in zip(model.get("w", [0.0]*len(v)), v)) + float(model.get("b", 0.0))
    return _sig(z)

def save(model: Dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(model, f)

def load(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
