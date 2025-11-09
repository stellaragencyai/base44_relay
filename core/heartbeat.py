# core/heartbeat.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Optional, Dict, Any

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state" / "effective"
STATE_DIR.mkdir(parents=True, exist_ok=True)

def _path(name: str) -> Path:
    safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", ".")).strip() or "bot"
    return STATE_DIR / f"{safe}.json"

def ping(name: str,
         critical: bool = True,
         extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Write/update a simple heartbeat file:
      { "last": <unix_sec>, "critical": true, "pid": X, "host": Y, ...extra }
    The watchdog reads these to decide if a bot is stale.
    """
    obj: Dict[str, Any] = {
        "last": int(time.time()),
        "critical": bool(critical),
        "pid": os.getpid(),
        "host": os.getenv("COMPUTERNAME") or os.uname().nodename if hasattr(os, "uname") else "unknown"
    }
    if extra:
        try:
            obj.update({k: v for k, v in extra.items() if v is not None})
        except Exception:
            pass
    p = _path(name)
    try:
        p.write_text(json.dumps(obj, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    except Exception:
        # best-effort; never crash a bot for telemetry
        pass
