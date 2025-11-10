#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
learn.online_learner — trivial Bayesian updater for per-setup priors.

We maintain Beta parameters per setup_tag:
  map: { setup_tag: { "alpha": float, "beta": float, "n": int, "last_ts": int } }

Interpretation
- Prior win probability estimate = alpha / (alpha + beta)
- Start with weak prior Beta(α0,β0). Defaults α0=2.0, β0=2.0
- Each outcome: won=True -> alpha += w; won=False -> beta += w
- Weight w is |pnl_r| clipped to [0.25, 2.0] to give stronger updates to big R

Outputs
- Writes to state/model_state.json
- Provides `get_prior_win_p(setup_tag)` and `update_from_outcome(outcome_obj)`

Executor integration (optional):
- Your executor can read these priors if you wish:
    from learn.online_learner import get_prior_win_p
"""

from __future__ import annotations
import json, os, time, threading
from pathlib import Path
from typing import Dict, Any

# settings optional
try:
    from core.config import settings
except Exception:
    class _S: ROOT = Path(os.getcwd())
    settings = _S()  # type: ignore

ROOT = getattr(settings, "ROOT", Path(os.getcwd()))
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

MODEL_PATH = Path(os.getenv("MODEL_STATE_PATH", STATE_DIR / "model_state.json"))
ALPHA0 = float(os.getenv("MODEL_PRIOR_ALPHA", "2.0"))
BETA0  = float(os.getenv("MODEL_PRIOR_BETA",  "2.0"))
W_MIN, W_MAX = 0.25, 2.0

_lock = threading.RLock()
_state: Dict[str, Dict[str, float]] = {}  # setup_tag -> params

def _load() -> None:
    global _state
    try:
        if MODEL_PATH.exists():
            _state = json.loads(MODEL_PATH.read_text(encoding="utf-8"))
            if not isinstance(_state, dict):
                _state = {}
    except Exception:
        _state = {}

def _save() -> None:
    try:
        tmp = MODEL_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(_state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(MODEL_PATH)
    except Exception:
        pass

def _ensure_tag(tag: str) -> Dict[str, float]:
    d = _state.get(tag)
    if not d:
        d = {"alpha": ALPHA0, "beta": BETA0, "n": 0, "last_ts": 0}
        _state[tag] = d
    return d

def get_prior_win_p(setup_tag: str) -> float:
    with _lock:
        if not _state:
            _load()
        d = _ensure_tag(setup_tag or "Unknown")
        a, b = float(d["alpha"]), float(d["beta"])
        tot = max(1e-9, a + b)
        return max(0.01, min(0.99, a / tot))

def update_from_outcome(out: Dict[str, Any]) -> None:
    """
    out keys: setup_tag, won(bool), pnl_r(float), ts_ms(int)
    """
    tag = str(out.get("setup_tag") or "Unknown")
    won = bool(out.get("won"))
    try:
        r = float(out.get("pnl_r", 0.0))
    except Exception:
        r = 0.0
    weight = max(W_MIN, min(W_MAX, abs(r)))
    ts = int(out.get("ts_ms") or int(time.time()*1000))

    with _lock:
        if not _state:
            _load()
        d = _ensure_tag(tag)
        if won:
            d["alpha"] = float(d["alpha"]) + weight
        else:
            d["beta"] = float(d["beta"]) + weight
        d["n"] = int(d.get("n", 0)) + 1
        d["last_ts"] = ts
        _save()
