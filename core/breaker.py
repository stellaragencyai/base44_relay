#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/breaker.py — file-backed global breaker with TTL, atomic writes, and CLI.

State file: .state/risk_state.json
Schema:
{
  "breach": true|false,
  "reason": "string",
  "ts": 1730820000,          # set/last change (unix seconds)
  "ttl": 0,                  # seconds, 0 = no expiry
  "source": "human|bot|..."  # optional provenance tag
}

Env (optional):
  BREAKER_DEFAULT_TTL_SEC=0       # default TTL when turning ON without explicit ttl
  BREAKER_NOTIFY_COOLDOWN_SEC=8   # min seconds between identical tg notifications
"""

from __future__ import annotations
import os, json, time, pathlib, argparse, contextlib
from typing import Optional, Dict, Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "risk_state.json"
_TMP_FILE = STATE_DIR / ".risk_state.tmp"

DEFAULT_TTL = int(os.getenv("BREAKER_DEFAULT_TTL_SEC", "0") or "0")
NOTIFY_COOLDOWN = int(os.getenv("BREAKER_NOTIFY_COOLDOWN_SEC", "8") or "8")

# optional notifier
try:
    from core.notifier_bot import tg_send  # type: ignore
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):  # type: ignore
        print(f"[notify/{priority}] {msg}")

# optional structured decision log
try:
    from core.decision_log import log_event  # type: ignore
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass

# ---- low-level IO (atomic) ----------------------------------------------------
def _atomic_write_json(path: pathlib.Path, data: Dict[str, Any]) -> None:
    _TMP_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(_TMP_FILE, path)

def _now() -> int:
    return int(time.time())

def _load_raw() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {"breach": False, "reason": "", "ts": 0, "ttl": 0, "source": ""}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"breach": False, "reason": "", "ts": 0, "ttl": 0, "source": ""}

def _save_raw(d: Dict[str, Any]) -> None:
    d.setdefault("ts", _now())
    d.setdefault("ttl", 0)
    d.setdefault("reason", "")
    d.setdefault("source", "")
    _atomic_write_json(STATE_FILE, d)

# ---- semantics ----------------------------------------------------------------
def _expired(d: Dict[str, Any]) -> bool:
    ttl = int(d.get("ttl") or 0)
    if ttl <= 0:
        return False
    ts = int(d.get("ts") or 0)
    return (_now() - ts) >= ttl

def _normalize(d: Dict[str, Any]) -> Dict[str, Any]:
    # Auto-clear expired breach
    if d.get("breach") and _expired(d):
        d = dict(d)
        d["breach"] = False
        d["reason"] = "auto_expired"
        d["ts"] = _now()
        d["ttl"] = 0
        _save_raw(d)
    return d

def status() -> Dict[str, Any]:
    """Return current breaker state (auto-expiring if necessary)."""
    return _normalize(_load_raw())

def is_active() -> bool:
    return bool(status().get("breach"))

def should_block(component: str = "", why: str = "") -> bool:
    """
    Cheap gate other modules can call. If active, logs a decision-log event once per call site.
    """
    if not is_active():
        return False
    log_event("guard", "breaker_block", symbol="", account_uid="", payload={
        "component": component, "why": why, "state": status()
    })
    return True

# ---- toggles ------------------------------------------------------------------
_last_notify = {"on": 0, "off": 0}

def _can_notify(kind: str) -> bool:
    now = _now()
    if now - _last_notify.get(kind, 0) >= NOTIFY_COOLDOWN:
        _last_notify[kind] = now
        return True
    return False

def set_on(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    """
    Turn breaker ON. Optional ttl_sec overrides env default; 0 disables expiry.
    """
    ttl = int(ttl_sec if ttl_sec is not None else DEFAULT_TTL)
    d = status()
    changed = (not d.get("breach")) or (ttl != d.get("ttl")) or (reason != d.get("reason"))
    d.update({"breach": True, "reason": reason, "ts": _now(), "ttl": max(0, ttl), "source": source})
    _save_raw(d)

    log_event("guard", "breaker_on", symbol="", account_uid="", payload={"reason": reason, "ttl": ttl, "source": source})
    if changed and _can_notify("on"):
        extra = f" • ttl={ttl}s" if ttl > 0 else ""
        tg_send(f"🛑 Breaker ON • reason: {reason}{extra}", priority="error")

def set_on_until(reason: str, until_epoch_sec: int, source: str = "human") -> None:
    ttl = max(0, int(until_epoch_sec) - _now())
    set_on(reason=reason, ttl_sec=ttl, source=source)

def extend(ttl_delta_sec: int) -> None:
    """
    If active, extends TTL by ttl_delta_sec from NOW (not from original ts).
    If inactive, no-op.
    """
    d = status()
    if not d.get("breach"):
        return
    new_ttl = max(0, int(ttl_delta_sec))
    d.update({"ts": _now(), "ttl": new_ttl})
    _save_raw(d)
    log_event("guard", "breaker_extend", symbol="", account_uid="", payload={"ttl": new_ttl})
    if _can_notify("on"):
        tg_send(f"⏩ Breaker TTL extended • ttl={new_ttl}s", priority="info")

def set_off(reason: str = "manual_clear", source: str = "human") -> None:
    d = status()
    changed = bool(d.get("breach"))
    d.update({"breach": False, "reason": reason, "ts": _now(), "ttl": 0, "source": source})
    _save_raw(d)

    log_event("guard", "breaker_off", symbol="", account_uid="", payload={"reason": reason, "source": source})
    if changed and _can_notify("off"):
        tg_send("✅ Breaker OFF • entries re-enabled", priority="success")

# convenient alias
def breach(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    set_on(reason=reason, ttl_sec=ttl_sec, source=source)

# ---- context manager ----------------------------------------------------------
@contextlib.contextmanager
def breaker_guard(component: str = "", block_reason: str = "breaker_active"):
    """
    Usage:
        with breaker_guard("tp_manager"):
            ... do risky things ...
    If active, raises RuntimeError to let caller unwind cleanly.
    """
    if is_active():
        log_event("guard", "breaker_block_enter", symbol="", account_uid="", payload={
            "component": component, "reason": block_reason, "state": status()
        })
        raise RuntimeError(f"Breaker active: {block_reason}")
    yield

# ---- CLI ----------------------------------------------------------------------
def _cli():
    ap = argparse.ArgumentParser(description="Global breaker control")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--on", action="store_true", help="Turn breaker ON")
    g.add_argument("--off", action="store_true", help="Turn breaker OFF")
    g.add_argument("--status", action="store_true", help="Print breaker status JSON")

    ap.add_argument("--reason", type=str, default=None, help="Reason for ON/OFF")
    ap.add_argument("--on-ttl", type=int, default=None, help="When used with --on, set TTL seconds (0 = no expiry)")
    ap.add_argument("--extend", type=int, default=None, help="Extend or set TTL seconds if breaker is ON")
    ap.add_argument("--until", type=int, default=None, help="UNIX seconds; with --on set expiry absolute time")
    ap.add_argument("--source", type=str, default="cli", help="Provenance tag")

    args = ap.parse_args()

    if args.status:
        print(json.dumps(status(), indent=2))
        return

    if args.on:
        if args.until is not None:
            set_on_until(reason=(args.reason or "manual"), until_epoch_sec=args.until, source=args.source)
        else:
            set_on(reason=(args.reason or "manual"), ttl_sec=args.on_ttl, source=args.source)
        print(json.dumps(status(), indent=2))
        return

    if args.off:
        set_off(reason=(args.reason or "manual_clear"), source=args.source)
        print(json.dumps(status(), indent=2))
        return

    if args.extend is not None:
        extend(args.extend)
        print(json.dumps(status(), indent=2))
        return

    # default: show help if no actionable flag
    ap.print_help()

def main():
    _cli()

if __name__ == "__main__":
    main()
