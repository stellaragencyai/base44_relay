#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
detectors/live_detector.py — real-time feature scoring → signal queue

- Takes raw feature dicts (from websocket or local feed)
- Produces setup_tag + score
- Writes to signals/observed.jsonl for the executor

Environment:
  B44_ROOT=base dir (optional)
  DETECTOR_MIN_SCORE=0.10
  DETECTOR_QUEUE=signals/observed.jsonl
  DETECTOR_ACCOUNT_UID=MAIN
"""

from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Dict, Any

from core.feature_extractor import extract_and_tag
from core.setup_ranker import SetupRanker

ROOT = Path(os.getenv("B44_ROOT", Path(__file__).resolve().parents[1]))
SIG_DIR = ROOT / "signals"
SIG_DIR.mkdir(parents=True, exist_ok=True)
QUEUE = SIG_DIR / (os.getenv("DETECTOR_QUEUE", "observed.jsonl") or "observed.jsonl")

MIN_SCORE = float(os.getenv("DETECTOR_MIN_SCORE", "0.10") or "0.10")
ACCOUNT_UID = os.getenv("DETECTOR_ACCOUNT_UID", "MAIN")

ranker = SetupRanker()
ranker.load()

def _append_signal(obj: Dict[str, Any]) -> None:
    try:
        with open(QUEUE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception:
        pass

def process_feature(symbol: str, side_hint: str, raw_features: Dict[str, Any]) -> float:
    """
    side_hint: 'LONG' or 'SHORT' proposed by your strategy
    returns score used to decide if it should be enqueued
    """
    # enrich + tag
    feats = dict(raw_features or {})
    feats["symbol"] = symbol
    tagged = extract_and_tag(feats)
    score = ranker.score(tagged)

    if abs(score) < MIN_SCORE:
        return score

    signal = "LONG" if (score >= 0 and side_hint.upper().startswith("L")) else (
             "SHORT" if (score < 0 and side_hint.upper().startswith("S")) else side_hint.upper())

    payload = {
        "ts": int(time.time()*1000),
        "symbol": symbol.upper(),
        "signal": signal,
        "params": {
            "tag": "B44",                # will be augmented with ownership tag by executor
            "maker_only": True,
            "spread_max_bps": 8,
        },
        "features": {**tagged, "ranker_score": round(score, 6)},
        "account_uid": ACCOUNT_UID,
    }
    _append_signal(payload)
    return score

# Example CLI: fake feed for quick smoke test
if __name__ == "__main__":
    import random, argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--side", default="LONG")
    args = ap.parse_args()

    raw = {
        "atr": 0.018 + random.random()*0.02,
        "price_change_pct": (random.random()-0.5)*0.04,
        "adx": 15 + random.random()*25,
        "vol_z": (random.random()-0.5)*3,
        "trend_slope": (random.random()-0.5)*2,
    }
    sc = process_feature(args.symbol, args.side, raw)
    print(f"scored {args.symbol} {args.side}: {sc:.3f} → queued @ {QUEUE}")
