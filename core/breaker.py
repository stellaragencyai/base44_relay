#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/breaker.py — file-backed global breaker with CLI and Telegram notice.

State file: .state/risk_state.json
Schema: {"breach": true|false, "reason": "...", "ts": 1730820000}
"""

from __future__ import annotations
import os, json, time, pathlib, argparse
from typing import Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "risk_state.json"

# optional notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

def _load() -> dict:
    if not STATE_FILE.exists():
        return {"breach": False, "reason": "", "ts": 0}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"breach": False, "reason": "", "ts": 0}

def _save(d: dict) -> None:
    d.setdefault("ts", int(time.time()))
    STATE_FILE.write_text(json.dumps(d, indent=2), encoding="utf-8")

def is_active() -> bool:
    return bool(_load().get("breach"))

def status() -> dict:
    return _load()

def set_on(reason: str = "manual") -> None:
    data = _load()
    data["breach"] = True
    data["reason"] = reason
    data["ts"] = int(time.time())
    _save(data)
    tg_send(f"🛑 Breaker ON • reason: {reason}", priority="error")

def set_off(reason: str = "manual_clear") -> None:
    data = _load()
    data["breach"] = False
    data["reason"] = reason
    data["ts"] = int(time.time())
    _save(data)
    tg_send("✅ Breaker OFF • entries re-enabled", priority="success")

def main():
    ap = argparse.ArgumentParser(description="Global breaker control")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--on",  nargs="?", const="manual", help="Turn breaker ON with optional reason")
    g.add_argument("--off", nargs="?", const="manual_clear", help="Turn breaker OFF with optional reason")
    ap.add_argument("--status", action="store_true", help="Print breaker status")
    args = ap.parse_args()

    if args.on is not None:
        set_on(args.on)
    elif args.off is not None:
        set_off(args.off)
    elif args.status:
        print(json.dumps(status(), indent=2))
    else:
        ap.print_help()

if __name__ == "__main__":
    main()
