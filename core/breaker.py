#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/breaker.py — file-backed global breaker with TTL, atomic writes, helpers, CLI,
and optional friend-approval enforcement for CLEAR/OFF.

State file: .state/risk_state.json
Schema:
{
  "breach": true|false,
  "reason": "string",
  "ts": 1730820000,          # set/last change (unix seconds)
  "ttl": 0,                  # seconds, 0 = no expiry
  "source": "human|bot|...", # optional provenance tag
  "version": 1               # schema version (future-proofing)
}

Env (optional):
  BREAKER_DEFAULT_TTL_SEC=0
  BREAKER_NOTIFY_COOLDOWN_SEC=8

Approval integration (optional):
  APPROVAL_REQUIRE_CLEAR=1            # if 1, any set_off() requires friend approval
  APPROVAL_ACCOUNT_KEY=main           # label shown in approval (e.g., main, sub:<uid>)
  APPROVAL_SERVICE_URL=http://127.0.0.1:5055
  APPROVAL_SHARED_SECRET=...          # must match approval_service
  APPROVAL_TIMEOUT_SEC=180            # how long to wait for approval

Notes:
- set_on / breach still do NOT require approval (safety first).
- set_off requires approval if APPROVAL_REQUIRE_CLEAR=1; otherwise behaves as before.
"""

from __future__ import annotations
import os, json, time, pathlib, argparse, contextlib, functools
from typing import Optional, Dict, Any, Callable, TypeVar

ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "risk_state.json"
_TMP_FILE = STATE_DIR / ".risk_state.tmp"

DEFAULT_TTL = int(os.getenv("BREAKER_DEFAULT_TTL_SEC", "0") or "0")
NOTIFY_COOLDOWN = int(os.getenv("BREAKER_NOTIFY_COOLDOWN_SEC", "8") or "8")

SCHEMA_VERSION = 1

# --- approval knobs ---
APPROVAL_REQUIRE_CLEAR = (os.getenv("APPROVAL_REQUIRE_CLEAR", "0") or "0").strip() in {"1","true","yes","on"}
APPROVAL_ACCOUNT_KEY = (os.getenv("APPROVAL_ACCOUNT_KEY", "main") or "main").strip()
APPROVAL_TIMEOUT_SEC = int(os.getenv("APPROVAL_TIMEOUT_SEC", "180") or "180")

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

# optional approval client
def _approval_available() -> bool:
    try:
        from core.approval_client import require_approval  # noqa
        return True
    except Exception:
        return False

def _require_clear_approval(reason: str) -> None:
    """
    Call friend-approval before clearing breaker, if enabled.
    Raises on denial/timeout to prevent unsafe clear.
    """
    if not APPROVAL_REQUIRE_CLEAR:
        return
    if not _approval_available():
        raise RuntimeError("Approval required to clear breaker, but approval_client not available.")

    from core.approval_client import require_approval  # type: ignore
    rid = require_approval(
        action="breaker_clear",
        account_key=APPROVAL_ACCOUNT_KEY,
        reason=reason or "manual_clear",
        ttl_sec=max(60, APPROVAL_TIMEOUT_SEC),         # request validity
        timeout_sec=APPROVAL_TIMEOUT_SEC,              # how long we wait here
        poll_sec=2.5
    )
    tg_send(f"🔐 Approval OK • breaker_clear • req={rid}", priority="success")

# ---- low-level IO (atomic) ----------------------------------------------------
def _atomic_write_json(path: pathlib.Path, data: Dict[str, Any]) -> None:
    data.setdefault("version", SCHEMA_VERSION)
    _TMP_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(_TMP_FILE, path)

def _now() -> int:
    return int(time.time())

def _load_raw() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {"breach": False, "reason": "", "ts": 0, "ttl": 0, "source": "", "version": SCHEMA_VERSION}
    try:
        d = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        d.setdefault("version", SCHEMA_VERSION)
        for k in ("breach","reason","ts","ttl","source"):
            d.setdefault(k, {"breach": False, "reason": "", "ts": 0, "ttl": 0, "source": ""}[k if k in ("reason","source") else k])
        return d
    except Exception:
        return {"breach": False, "reason": "", "ts": 0, "ttl": 0, "source": "", "version": SCHEMA_VERSION}

def _save_raw(d: Dict[str, Any]) -> None:
    d.setdefault("ts", _now())
    d.setdefault("ttl", 0)
    d.setdefault("reason", "")
    d.setdefault("source", "")
    d.setdefault("version", SCHEMA_VERSION)
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

def remaining_ttl() -> int:
    """Return remaining TTL in seconds (0 if none or inactive)."""
    d = status()
    ttl = int(d.get("ttl") or 0)
    if ttl <= 0 or not d.get("breach"):
        return 0
    elapsed = max(0, _now() - int(d.get("ts") or 0))
    rem = max(0, ttl - elapsed)
    return rem

# alias
time_left = remaining_ttl

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
_last_announced: Dict[str, Any] = {"breach": None, "reason": None, "ttl": None}  # for de-dupe spam control

def _can_notify(kind: str) -> bool:
    now = _now()
    if now - _last_notify.get(kind, 0) >= NOTIFY_COOLDOWN:
        _last_notify[kind] = now
        return True
    return False

def set_on(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    """
    Turn breaker ON. Optional ttl_sec overrides env default; 0 disables expiry.
    Approval is NOT required to enable safety.
    """
    ttl = int(ttl_sec if ttl_sec is not None else DEFAULT_TTL)
    cur = status()
    new_state = {"breach": True, "reason": reason, "ts": _now(), "ttl": max(0, ttl), "source": source, "version": SCHEMA_VERSION}
    _save_raw(new_state)

    log_event("guard", "breaker_on", symbol="", account_uid="", payload={"reason": reason, "ttl": ttl, "source": source})

    changed = (not cur.get("breach")) or (int(cur.get("ttl") or 0) != ttl) or (cur.get("reason") != reason)
    sig = {"breach": True, "reason": reason, "ttl": ttl}
    if changed and (_last_announced != sig) and _can_notify("on"):
        extra = f" • ttl={ttl}s" if ttl > 0 else ""
        tg_send(f"🛑 Breaker ON • reason: {reason}{extra}", priority="error")
        _last_announced.update(sig)

def set_on_for(minutes: float, reason: str = "manual", source: str = "human") -> None:
    ttl = max(0, int(minutes * 60))
    set_on(reason=reason, ttl_sec=ttl, source=source)

def set_on_until(reason: str, until_epoch_sec: int, source: str = "human") -> None:
    ttl = max(0, int(until_epoch_sec) - _now())
    set_on(reason=reason, ttl_sec=ttl, source=source)

def extend(ttl_delta_sec: int) -> None:
    """
    If active, sets NEW ttl from NOW to ttl_delta_sec.
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
        tg_send(f"⏩ Breaker TTL set • ttl={new_ttl}s", priority="info")

def set_off(reason: str = "manual_clear", source: str = "human") -> None:
    """
    Turn breaker OFF. If APPROVAL_REQUIRE_CLEAR=1, demands friend approval.
    """
    # Approval gate (raises on denial/timeout/misconfig)
    try:
        _require_clear_approval(reason)
    except Exception as e:
        log_event("guard", "breaker_off_block", symbol="", account_uid="", payload={"error": str(e)}, level="error")
        tg_send(f"❌ Breaker OFF blocked • {e}", priority="error")
        raise

    cur_active = is_active()
    d = status()
    d.update({"breach": False, "reason": reason, "ts": _now(), "ttl": 0, "source": source, "version": SCHEMA_VERSION})
    _save_raw(d)

    log_event("guard", "breaker_off", symbol="", account_uid="", payload={"reason": reason, "source": source})
    if cur_active and _can_notify("off"):
        tg_send("✅ Breaker OFF • entries re-enabled", priority="success")

# convenient alias
def breach(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    set_on(reason=reason, ttl_sec=ttl_sec, source=source)

# ---- blocking helpers ---------------------------------------------------------
T = TypeVar("T")

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

def wait_until_clear(timeout_sec: int = 120, poll_sec: float = 1.0) -> bool:
    """
    Block until breaker clears or auto-expires.
    Returns True if cleared, False on timeout.
    Safe to use in bots that want to pause entries briefly.
    """
    deadline = _now() + int(timeout_sec)
    while _now() < deadline:
        if not is_active():
            return True
        time.sleep(max(0.05, float(poll_sec)))
    return not is_active()

def require_clear(component: str = "", block_reason: str = "breaker_active") -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to guard a function. Raises RuntimeError if breaker is ON.
    """
    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            if is_active():
                log_event("guard", "breaker_block_call", symbol="", account_uid="", payload={
                    "component": component or fn.__name__, "reason": block_reason, "state": status()
                })
                raise RuntimeError(f"Breaker active: {block_reason}")
            return fn(*args, **kwargs)
        return wrapper
    return deco

@contextlib.contextmanager
def breaker_blocking(component: str = "", why: str = "breaker_active"):
    """
    Context that converts active breaker into a no-op block (yields False) instead of raising.
    Usage:
        with breaker_blocking("signal_engine") as allowed:
            if not allowed:
                return
            ... continue ...
    """
    if is_active():
        log_event("guard", "breaker_block_silent", symbol="", account_uid="", payload={
            "component": component, "why": why, "state": status()
        })
        yield False
    else:
        yield True

# ---- CLI ----------------------------------------------------------------------
def _cli():
    ap = argparse.ArgumentParser(description="Global breaker control")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--on", action="store_true", help="Turn breaker ON")
    g.add_argument("--off", action="store_true", help="Turn breaker OFF")
    g.add_argument("--status", action="store_true", help="Print breaker status JSON")

    ap.add_argument("--reason", type=str, default=None, help="Reason for ON/OFF")
    ap.add_argument("--on-ttl", type=int, default=None, help="With --on, set TTL seconds (0 = no expiry)")
    ap.add_argument("--extend", type=int, default=None, help="Set TTL seconds from now if breaker is ON")
    ap.add_argument("--until", type=int, default=None, help="UNIX seconds; with --on set absolute expiry time")
    ap.add_argument("--for-min", type=float, default=None, help="With --on, set TTL in minutes (float ok)")
    ap.add_argument("--source", type=str, default="cli", help="Provenance tag")
    ap.add_argument("--time-left", action="store_true", help="Print remaining TTL seconds and exit")

    args = ap.parse_args()

    if args.time_left:
        print(remaining_ttl()); return
    if args.status:
        print(json.dumps(status(), indent=2)); return
    if args.on:
        if args.for_min is not None:
            set_on_for(args.for_min, reason=(args.reason or "manual"), source=args.source)
        elif args.until is not None:
            set_on_until(reason=(args.reason or "manual"), until_epoch_sec=args.until, source=args.source)
        else:
            set_on(reason=(args.reason or "manual"), ttl_sec=args.on_ttl, source=args.source)
        print(json.dumps(status(), indent=2)); return
    if args.off:
        set_off(reason=(args.reason or "manual_clear"), source=args.source)
        print(json.dumps(status(), indent=2)); return
    if args.extend is not None:
        extend(args.extend)
        print(json.dumps(status(), indent=2)); return

    ap.print_help()

def main():
    _cli()

if __name__ == "__main__":
    main()
