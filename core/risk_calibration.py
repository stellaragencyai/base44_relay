#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Risk calibration: estimate per-symbol MAE percentile for initial SL.
Uses outcomes logged by feature_store; falls back to defaults.
"""
from __future__ import annotations
import json, math
from pathlib import Path
from typing import Dict, List

DATA = Path("./data/features/outcomes.jsonl")

def _read_outcomes(symbol: str) -> List[float]:
    if not DATA.exists():
        return []
    maes = []
    with open(DATA, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                if row.get("kind")=="outcome" and row.get("symbol","").upper()==symbol.upper():
                    mae = float(row.get("outcome",{}).get("mae_bps", 0.0))
                    if mae > 0: maes.append(mae)
            except Exception:
                continue
    return maes

def sl_mae_percentile(symbol: str, pct: float=0.7, default_bps: float=60.0) -> float:
    xs = sorted(_read_outcomes(symbol))
    if not xs:
        return default_bps
    k = max(0, min(len(xs)-1, int(pct*len(xs))-1))
    return float(xs[k])
