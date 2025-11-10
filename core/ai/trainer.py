#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Daily trainer: reads yesterday’s DRY/LIVE entries from decision_log jsonl,
builds features/labels, trains logistic, writes model to .state/models/latest.json
"""

from __future__ import annotations
import os, json, time, glob, datetime as dt
from typing import Dict, List
from core.ai.featurizer import make_features
from core.ai.labeler import label_forward
from core.ai.model_lr import fit, save

LOG_DIR = os.getenv("DECISION_LOG_DIR", "./logs/decisions")
OUT_DIR = ".state/models"
os.makedirs(OUT_DIR, exist_ok=True)

def _yesterday_dirs(base: str) -> List[str]:
    y = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%Y")
    m = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%m")
    d = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%d")
    return [os.path.join(base, "jsonl", y, m, f"{d}.jsonl")]

def _load_events(paths: List[str]) -> List[Dict]:
    out = []
    for p in paths:
        if not os.path.exists(p): continue
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    js = json.loads(line)
                    if js.get("component") == "executor" and js.get("event") in ("entry_ok","entry_dry"):
                        out.append(js)
                except Exception:
                    pass
    return out

def main():
    files = _yesterday_dirs(LOG_DIR)
    evs = _load_events(files)
    X, y = [], []
    for e in evs:
        sym = str(e.get("symbol","")).upper()
        side = str((e.get("payload") or {}).get("side","Buy"))
        px = float((e.get("payload") or {}).get("px") or (e.get("payload") or {}).get("price") or 0.0)
        feats = make_features(sym)
        if not feats: continue
        label = label_forward(sym, entry_px=feats.get("px", px or 0.0), side=side, horizon_min=30, r_mult=1.0, atr=feats.get("atr14",0.0))
        X.append(feats); y.append(int(label))
    if len(X) < 20:
        return  # not enough data; try again tomorrow
    model = fit(X, y, lr=0.05, l2=1e-4, iters=400)
    save(model, os.path.join(OUT_DIR, "latest.json"))

if __name__ == "__main__":
    main()
