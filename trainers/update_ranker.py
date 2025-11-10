#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trainers/update_ranker.py — online learner for SetupRanker

Tails logs/outcomes/outcomes.jsonl, updates model, and persists it to:
  logs/models/setup_ranker.json

It keeps its own offset in:
  .state/ranker_outcome_offset.json
"""

from __future__ import annotations
import json, time, os
from pathlib import Path
from typing import Dict, Any

from core.setup_ranker import SetupRanker
from core.outcome_bus import tail_outcomes

ROOT = Path(os.getenv("B44_ROOT", Path(__file__).resolve().parents[1]))
STATE = ROOT / ".state"
STATE.mkdir(parents=True, exist_ok=True)
OFF_PATH = STATE / "ranker_outcome_offset.json"

def _read_off() -> int:
    try:
        return int(json.loads(OFF_PATH.read_text(encoding="utf-8")).get("pos", 0))
    except Exception:
        return 0

def _write_off(pos: int) -> None:
    try:
        OFF_PATH.write_text(json.dumps({"pos": int(pos)}), encoding="utf-8")
    except Exception:
        pass

def _safe_setup_tag(e: Dict[str, Any]) -> str:
    s = str(e.get("setup_tag") or "").strip()
    return s if s else "Unclassified"

def main(poll_sec: float = 1.0, autosave_every: int = 20) -> None:
    ranker = SetupRanker()
    ranker.load()

    pos = _read_off()
    seen = 0
    last_save = time.time()

    print("[ranker] online, pos=", pos, " lr_prior=", ranker.lr_prior, " lr_weights=", ranker.lr_weights)

    while True:
        try:
            new_pos, rows = tail_outcomes(pos)
            if rows:
                for e in rows:
                    try:
                        features = dict(e.get("features") or {})
                        # ensure the setup tag is present for the update call
                        features["setup_tag"] = _safe_setup_tag(e)
                        pnl_r = float(e.get("pnl_r", 0.0))
                        won = bool(e.get("won", pnl_r > 0))
                        ranker.update(features, pnl_r=pnl_r, won=won)
                        seen += 1
                    except Exception as ex:
                        print("[ranker] bad row:", ex)
                pos = new_pos
                _write_off(pos)

            # periodic save
            if seen >= autosave_every or (time.time() - last_save) > 10:
                ranker.save()
                last_save = time.time()
                seen = 0

            time.sleep(max(0.1, float(poll_sec)))
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("[ranker] loop error:", e)
            time.sleep(0.8)
    try:
        ranker.save()
    except Exception:
        pass

if __name__ == "__main__":
    main()
