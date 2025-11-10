#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Replay sanity for the AI loop.
- Reads yesterday's executor entries from decision log (dry/live).
- Recomputes labels with labeler.
- Prints hit-rate and a few buckets vs model p_win.

This is NOT a full backtester; it’s a guardrail for “did the brain make things worse?”
"""

from __future__ import annotations
import os, json, datetime as dt
from typing import Dict, List
from core.ai.infer import score_symbol
from core.ai.labeler import label_forward

LOG_DIR = os.getenv("DECISION_LOG_DIR", "./logs/decisions")

def _yesterday_jsonl():
    y = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%Y")
    m = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%m")
    d = (dt.datetime.utcnow() - dt.timedelta(days=1)).strftime("%d")
    return os.path.join(LOG_DIR, "jsonl", y, m, f"{d}.jsonl")

def _load_entries() -> List[Dict]:
    p = _yesterday_jsonl()
    out = []
    if not os.path.exists(p):
        return out
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
                if e.get("component") == "executor" and e.get("event") in ("entry_ok","entry_dry"):
                    out.append(e)
            except Exception:
                pass
    return out

def main():
    evs = _load_entries()
    if not evs:
        print("no entries found for yesterday")
        return

    rows = []
    for e in evs:
        sym = str(e.get("symbol","")).upper()
        side = (e.get("payload") or {}).get("side") or "Buy"
        s = score_symbol(sym)
        p = float(s.get("p_win", 0.55))
        # label around current px as proxy (we’re simulating)
        px = float((s.get("features") or {}).get("px", 0.0))
        lbl = label_forward(sym, entry_px=px, side=side, horizon_min=30, r_mult=1.0, atr=float((s.get("features") or {}).get("atr14",0.0)))
        rows.append((p, int(lbl)))

    # crude calibration view
    if not rows:
        print("no rows to score")
        return

    bins = [(0.0,0.55),(0.55,0.60),(0.60,0.65),(0.65,1.01)]
    print("bucket\tcount\twin_rate")
    for lo, hi in bins:
        bucket = [lbl for p,lbl in rows if lo <= p < hi]
        if bucket:
            wr = sum(bucket)/len(bucket)
            print(f"{lo:.2f}-{hi:.2f}\t{len(bucket)}\t{wr:.2f}")
    overall = sum(l for _,l in rows)/len(rows)
    print(f"overall\t{len(rows)}\t{overall:.2f}")

if __name__ == "__main__":
    main()
