#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Feature store: logs pre-trade features and post-trade outcomes.
- Writes compact JSONL always
- Optionally writes Parquet if pyarrow/fastparquet is installed
Schema keys:
  signal_id: str (stable id, e.g., orderLinkId)
  ts_ms: int
  symbol: str
  sub_uid: str
  kind: "features" | "outcome"
"""
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Dict, Any

ROOT = Path(os.getenv("STATE_DIR", "./state")).resolve()
DATA_DIR = Path(os.getenv("FEATURE_STORE_DIR", "./data/features")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
FEAT_JSONL = DATA_DIR / "features.jsonl"
OUT_JSONL  = DATA_DIR / "outcomes.jsonl"

def _write_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")

def log_features(signal_id: str, symbol: str, sub_uid: str, features: Dict[str, Any], ts_ms: int | None=None) -> None:
    row = {"signal_id": signal_id, "ts_ms": ts_ms or int(time.time()*1000), "symbol": symbol, "sub_uid": str(sub_uid), "kind": "features", "features": features}
    _write_jsonl(FEAT_JSONL, row)
    _maybe_parquet("features", row)

def log_outcome(signal_id: str, symbol: str, sub_uid: str, outcome: Dict[str, Any], ts_ms: int | None=None) -> None:
    row = {"signal_id": signal_id, "ts_ms": ts_ms or int(time.time()*1000), "symbol": symbol, "sub_uid": str(sub_uid), "kind": "outcome", "outcome": outcome}
    _write_jsonl(OUT_JSONL, row)
    _maybe_parquet("outcomes", row)

def _maybe_parquet(name: str, row: Dict[str, Any]) -> None:
    try:
        import pandas as pd
        df = pd.DataFrame([row])
        out = DATA_DIR / f"{name}.parquet"
        if out.exists():
            old = pd.read_parquet(out)
            df = pd.concat([old, df], ignore_index=True)
        df.to_parquet(out, index=False)  # requires pyarrow or fastparquet if available
    except Exception:
        # parquet is best-effort; jsonl is authoritative
        pass
