#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/outcome_bus.py — append-only outcomes feed for online learning

File layout:
  logs/outcomes/outcomes.jsonl  (one JSON per outcome)

Schema (per line):
  {
    "ts": 1731020400123,         # ms
    "link": "B44-BTC-...|sub:strat:session",
    "symbol": "BTCUSDT",
    "setup_tag": "TrendPullback_5m",
    "pnl_r": 0.85,               # realized R multiple (neg for loss)
    "won": true,                 # convenience flag
    "features": {...}            # optional: snapshot of pretrade features
  }

Public helpers:
  emit_outcome(link_id, symbol, setup_tag, pnl_r, won, features=None)
  tail_outcomes(start_pos) -> (new_pos, [dict,...])
"""

from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Dict, Any, List, Tuple

ROOT = Path(os.getenv("B44_ROOT", Path(__file__).resolve().parents[1]))
OUT_DIR = ROOT / "logs" / "outcomes"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BUS_PATH = OUT_DIR / "outcomes.jsonl"

def emit_outcome(link_id: str,
                 symbol: str,
                 setup_tag: str,
                 pnl_r: float,
                 won: bool,
                 features: Dict[str, Any] | None = None) -> None:
    row = {
        "ts": int(time.time()*1000),
        "link": str(link_id),
        "symbol": str(symbol).upper(),
        "setup_tag": str(setup_tag or "Unclassified"),
        "pnl_r": float(pnl_r),
        "won": bool(won),
        "features": dict(features or {}),
    }
    try:
        with open(BUS_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        pass

def tail_outcomes(start_pos: int = 0) -> Tuple[int, List[Dict[str, Any]]]:
    if not BUS_PATH.exists():
        return start_pos, []
    size = BUS_PATH.stat().st_size
    pos = start_pos if 0 <= start_pos <= size else 0
    out: List[Dict[str, Any]] = []
    with open(BUS_PATH, "r", encoding="utf-8") as fh:
        fh.seek(pos, 0)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
        new_pos = fh.tell()
    return new_pos, out
