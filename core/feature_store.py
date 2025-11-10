#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/feature_store.py — append-only feature logging for pre-trade context

Writes both JSONL and CSV to logs/features/.
"""

from __future__ import annotations
import os, json, csv, time
from pathlib import Path
from typing import Dict, Any

ROOT = Path(os.getenv("B44_ROOT", Path(__file__).resolve().parents[1]))
OUT_DIR = ROOT / "logs" / "features"
OUT_DIR.mkdir(parents=True, exist_ok=True)

JSONL = OUT_DIR / "pretrade_features.jsonl"
CSV   = OUT_DIR / "pretrade_features.csv"

def log_features(link_id: str, symbol: str, account: str, features: Dict[str, Any]) -> None:
    ts = int(time.time() * 1000)
    row = {"ts": ts, "link": link_id, "symbol": symbol, "account": account, **(features or {})}

    # JSONL
    try:
        with open(JSONL, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        pass

    # CSV with evolving header
    try:
        header = list(row.keys())
        write_header = not CSV.exists()
        if CSV.exists():
            # try to keep a stable superset of columns
            with open(CSV, "r", encoding="utf-8", newline="") as fh:
                rd = csv.reader(fh)
                first = next(rd, None)
                if first:
                    old = list(first)
                    for k in header:
                        if k not in old:
                            old.append(k)
                    header = old
        with open(CSV, "a", encoding="utf-8", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=header, extrasaction="ignore")
            if write_header:
                wr.writeheader()
            wr.writerow(row)
    except Exception:
        pass
