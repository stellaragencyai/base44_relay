from __future__ import annotations
import os, time
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

STATE = ROOT / ".state"
LOGS  = ROOT / "logs"

# crude heartbeat: file mtimes as proxies
CHECKS = [
    ("risk_daemon", STATE / "risk_state.json"),
    ("tp_sl_manager", STATE / "tp_manager.touch"),
    ("auto_executor", STATE / "executor_offset.json"),
    ("pnl_logger", LOGS / "pnl" / "daily_pnl_log.jsonl"),
]

def _status_for(path: Path, stale_sec: int = 90) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {"exists": False, "status": "unknown", "age_sec": None}
        age = time.time() - path.stat().st_mtime
        return {
            "exists": True,
            "status": "ok" if age < stale_sec else "stale",
            "age_sec": int(age)
        }
    except Exception:
        return {"exists": False, "status": "error", "age_sec": None}

def get_health_payload() -> Dict[str, Any]:
    rows: List[dict] = []
    for name, path in CHECKS:
        meta = _status_for(path)
        rows.append({"component": name, **meta, "path": str(path)})
    return {"rows": rows}
