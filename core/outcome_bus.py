#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.outcome_bus — single-writer outcome emitter with fan-out hooks.

Responsibilities
- Append each outcome as a JSON line to state/outcomes.jsonl (durable log).
- Best-effort notify via Telegram (compact).
- Wake the online learner (local function call) to update priors immediately.
- Expose a no-throw API: emit_outcome(...).

Outcome schema (keys):
  link_id (str), symbol (str), setup_tag (str), pnl_r (float), won (bool),
  features (dict), ts_ms (int)

Env / settings (optional):
  OUTCOME_PATH=state/outcomes.jsonl
  OUTCOME_NOTIFY=true|false
  OUTCOME_TG_SUB_UID=<route to specific subaccount bot>
"""

from __future__ import annotations
import os, json, time, threading
from pathlib import Path
from typing import Dict, Any, Optional

# settings optional
try:
    from core.config import settings
except Exception:
    class _S: ROOT = Path(os.getcwd())
    settings = _S()  # type: ignore

# notifier optional; do not crash
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(*_a, **_k):  # type: ignore
        pass

# inline learner hook (best-effort)
try:
    from learn.online_learner import update_from_outcome  # type: ignore
except Exception:
    def update_from_outcome(*_a, **_k):  # type: ignore
        pass

ROOT = getattr(settings, "ROOT", Path(os.getcwd()))
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

OUTCOME_PATH = Path(os.getenv("OUTCOME_PATH", STATE_DIR / "outcomes.jsonl"))
OUTCOME_NOTIFY = (str(os.getenv("OUTCOME_NOTIFY", "true")).lower() in ("1","true","yes","on"))
OUTCOME_TG_SUB_UID = os.getenv("OUTCOME_TG_SUB_UID") or None

_lock = threading.RLock()

def _append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    with _lock:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

def emit_outcome(*,
                 link_id: str,
                 symbol: str,
                 setup_tag: str,
                 pnl_r: float,
                 won: bool,
                 features: Optional[Dict[str, Any]] = None) -> None:
    """Durably emit an outcome and nudge the learner. Never raises upstream."""
    ts_ms = int(time.time() * 1000)
    obj = {
        "ts_ms": ts_ms,
        "link_id": str(link_id or ""),
        "symbol": str(symbol or "").upper(),
        "setup_tag": str(setup_tag or "Unknown"),
        "pnl_r": float(pnl_r),
        "won": bool(won),
        "features": dict(features or {}),
    }
    try:
        _append_jsonl(OUTCOME_PATH, obj)
    except Exception:
        pass

    # Nudge learner (best-effort)
    try:
        update_from_outcome(obj)
    except Exception:
        pass

    # Optional compact notify
    if OUTCOME_NOTIFY:
        try:
            emoji = "🟢" if won else "🔴"
            short = f"{emoji} OUTCOME • {obj['symbol']} • {obj['setup_tag']} • R={pnl_r:+.2f}"
            tg_send(short, priority=("success" if won else "warn"), sub_uid=OUTCOME_TG_SUB_UID)
        except Exception:
            pass
