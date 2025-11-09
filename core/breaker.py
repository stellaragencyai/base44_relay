#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/breaker.py — global breaker with TTL, approval-gated CLEAR, auto-trip helpers, DB mirror, and CLI.

Primary state file: .state/risk_state.json
DB mirror: core.db.guard_set_breaker / core.db.guard_load

State file schema:
{
  "breach": true|false,
  "reason": "string",
  "ts": 1730820000,          # last change (unix seconds)
  "ttl": 0,                  # seconds, 0 = no expiry
  "source": "human|bot|...", # provenance
  "version": 1
}

Env (optional):
  BREAKER_DEFAULT_TTL_SEC=0
  BREAKER_NOTIFY_COOLDOWN_SEC=8

  # Auto-trip inputs (used by auto_tick or direct helpers)
  BREAKER_HEALTH_PATH=.state/health.json
  BREAKER_AUTO_ENABLE=true
  NEWS_LOCKOUT=false
  FUNDING_LOCKOUT_MIN=0
  CONNECTIVITY_LOCKOUT_SEC=0
  DD_LOCKOUT_PCT=0
  HEARTBEAT_STALE_SEC=0

Approval integration (optional):
  APPROVAL_REQUIRE_CLEAR=1
  APPROVAL_ACCOUNT_KEY=portfolio
  APPROVAL_SERVICE_URL=http://127.0.0.1:5055
  APPROVAL_SHARED_SECRET=...
  APPROVAL_TIMEOUT_SEC=180

CLI:
  python -m core.breaker --status
  python -m core.breaker --on --reason "manual" --for-min 15
  python -m core.breaker --off --reason "ok_to_trade"
  python -m core.breaker --extend 600
  python -m core.breaker --auto-tick
"""

from __future__ import annotations
import os, json, time, pathlib, argparse, contextlib, functools
from typing import Optional, Dict, Any, Callable, TypeVar, Tuple

# ---------- paths/state ----------
ROOT = pathlib.Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "risk_state.json"
_TMP_FILE = STATE_DIR / ".risk_state.tmp"

DEFAULT_TTL = int(os.getenv("BREAKER_DEFAULT_TTL_SEC", "0") or "0")
NOTIFY_COOLDOWN = int(os.getenv("BREAKER_NOTIFY_COOLDOWN_SEC", "8") or "8")

SCHEMA_VERSION = 1

# ---------- optional auto inputs ----------
HEALTH_PATH = (os.getenv("BREAKER_HEALTH_PATH") or (STATE_DIR / "health.json")).__str__()
AUTO_ENABLE = (os.getenv("BREAKER_AUTO_ENABLE", "true").strip().lower() in {"1","true","yes","on"})
NEWS_LOCKOUT = (os.getenv("NEWS_LOCKOUT", "false").strip().lower() in {"1","true","yes","on"})
FUNDING_LOCKOUT_MIN = int(os.getenv("FUNDING_LOCKOUT_MIN", "0") or "0")
CONNECTIVITY_LOCKOUT_SEC = int(os.getenv("CONNECTIVITY_LOCKOUT_SEC", "0") or "0")
DD_LOCKOUT_PCT = float(os.getenv("DD_LOCKOUT_PCT", "0") or "0")
HEARTBEAT_STALE_SEC = int(os.getenv("HEARTBEAT_STALE_SEC", "0") or "0")

# ---------- approval knobs ----------
APPROVAL_REQUIRE_CLEAR = (os.getenv("APPROVAL_REQUIRE_CLEAR", "0") or "0").strip() in {"1","true","yes","on"}
APPROVAL_ACCOUNT_KEY = (os.getenv("APPROVAL_ACCOUNT_KEY", "portfolio") or "portfolio").strip()
APPROVAL_TIMEOUT_SEC = int(os.getenv("APPROVAL_TIMEOUT_SEC", "180") or "180")

# ---------- notifier / decision log ----------
try:
    from core.notifier_bot import tg_send  # type: ignore
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):  # type: ignore
        print(f"[notify/{priority}] {msg}")

try:
    from core.decision_log import log_event  # type: ignore
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass

# ---------- DB mirror (optional; safe fallbacks) ----------
try:
    from core.db import guard_set_breaker, guard_load  # type: ignore
except Exception:
    def guard_set_breaker(active: bool, reason: str = "") -> None:  # type: ignore
        pass
    def guard_load() -> dict:  # type: ignore
        return {
            "breaker_on": False,
            "breaker_reason": "",
            "updated_ts": 0,
            "session_start_ms": 0,
            "start_equity_usd": 0.0,
            "realized_pnl_usd": 0.0,
            "breach": False,
        }

# ---------- low-level IO (atomic) ----------
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
        d.setdefault("breach", bool(d.get("breach", False)))
        d.setdefault("reason", d.get("reason", "") or "")
        d.setdefault("ts", int(d.get("ts", 0) or 0))
        d.setdefault("ttl", int(d.get("ttl", 0) or 0))
        d.setdefault("source", d.get("source", "") or "")
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

# ---------- DB mirror helpers ----------
def _touch_db_mirror(active: bool, reason: str) -> None:
    try:
        guard_set_breaker(bool(active), str(reason or ""))
    except Exception:
        pass

def _db_view() -> Tuple[bool, str]:
    try:
        gs = guard_load()
        return bool(gs.get("breaker_on", gs.get("breach", False))), str(gs.get("breaker_reason", "") or "")
    except Exception:
        return (False, "")

# ---------- semantics ----------
def _expired(d: Dict[str, Any]) -> bool:
    ttl = int(d.get("ttl") or 0)
    if ttl <= 0:
        return False
    ts = int(d.get("ts") or 0)
    return (_now() - ts) >= ttl

def _normalize(d: Dict[str, Any]) -> Dict[str, Any]:
    # respect TTL expiration
    if d.get("breach") and _expired(d):
        d = dict(d)
        d["breach"] = False
        d["reason"] = "auto_expired"
        d["ts"] = _now()
        d["ttl"] = 0
        _save_raw(d)
        _touch_db_mirror(False, d["reason"])
    return d

def status() -> Dict[str, Any]:
    """
    Returns a rich status dict that preserves legacy keys and adds DB + derived fields:
      legacy: breach, reason, ts, ttl, source, version
      derived: remaining_ttl
      local_*: perspective from file
      db_*: mirror observed in DB (if available)
    """
    local = _normalize(_load_raw())
    remaining = remaining_ttl()  # uses normalized
    db_active, db_reason = _db_view()
    # Derived local flags
    local_active = bool(local.get("breach"))
    local_reason = str(local.get("reason") or "")
    out = dict(local)  # legacy fields preserved
    out.update({
        "remaining_ttl": int(remaining),
        "local_active": local_active,
        "local_reason": local_reason,
        "db_active": bool(db_active),
        "db_reason": db_reason,
        "now": _now(),
    })
    return out

def is_active() -> bool:
    return bool(status().get("breach"))

def remaining_ttl() -> int:
    d = _normalize(_load_raw())
    ttl = int(d.get("ttl") or 0)
    if ttl <= 0 or not d.get("breach"):
        return 0
    elapsed = max(0, _now() - int(d.get("ts") or 0))
    return max(0, ttl - elapsed)

# ---------- block helpers ----------
def should_block(component: str = "", why: str = "") -> bool:
    if not is_active():
        return False
    log_event("guard", "breaker_block", symbol="", account_uid="", payload={
        "component": component, "why": why, "state": status()
    })
    return True

_last_notify = {"on": 0, "off": 0}
_last_sig: Dict[str, Any] = {"breach": None, "reason": None, "ttl": None}

def _can_notify(kind: str) -> bool:
    now = _now()
    if now - _last_notify.get(kind, 0) >= NOTIFY_COOLDOWN:
        _last_notify[kind] = now
        return True
    return False

def _emit_on(reason: str, ttl: int) -> None:
    if _can_notify("on"):
        extra = f" • ttl={ttl}s" if ttl > 0 else ""
        tg_send(f"🛑 Breaker ON • reason: {reason}{extra}", priority="error")

def _emit_off() -> None:
    if _can_notify("off"):
        tg_send("✅ Breaker OFF • entries re-enabled", priority="success")

def set_on(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    ttl = int(ttl_sec if ttl_sec is not None else DEFAULT_TTL)
    cur = _normalize(_load_raw())
    new_state = {"breach": True, "reason": reason, "ts": _now(), "ttl": max(0, ttl), "source": source, "version": SCHEMA_VERSION}
    _save_raw(new_state)
    _touch_db_mirror(True, reason)

    log_event("guard", "breaker_on", symbol="", account_uid="", payload={"reason": reason, "ttl": ttl, "source": source})

    changed = (not cur.get("breach")) or (int(cur.get("ttl") or 0) != ttl) or (cur.get("reason") != reason)
    sig = {"breach": True, "reason": reason, "ttl": ttl}
    if changed and (_last_sig != sig):
        _last_sig.update(sig)
        _emit_on(reason, ttl)

def set_on_for(minutes: float, reason: str = "manual", source: str = "human") -> None:
    ttl = max(0, int(minutes * 60))
    set_on(reason=reason, ttl_sec=ttl, source=source)

def set_on_until(reason: str, until_epoch_sec: int, source: str = "human") -> None:
    ttl = max(0, int(until_epoch_sec) - _now())
    set_on(reason=reason, ttl_sec=ttl, source=source)

def extend(ttl_delta_sec: int) -> None:
    d = _normalize(_load_raw())
    if not d.get("breach"):
        return
    new_ttl = max(0, int(ttl_delta_sec))
    d.update({"ts": _now(), "ttl": new_ttl})
    _save_raw(d)
    _touch_db_mirror(True, d.get("reason", "") or "")
    log_event("guard", "breaker_extend", symbol="", account_uid="", payload={"ttl": new_ttl})
    if _can_notify("on"):
        tg_send(f"⏩ Breaker TTL set • ttl={new_ttl}s", priority="info")

# ---------- approval client detection ----------
def _approval_available() -> bool:
    try:
        from core.approval_client import require_approval  # noqa
        return True
    except Exception:
        return False

def _require_clear_approval(reason: str) -> None:
    if not APPROVAL_REQUIRE_CLEAR:
        return
    if not _approval_available():
        raise RuntimeError("Approval required to clear breaker, but approval_client not available.")
    from core.approval_client import require_approval  # type: ignore
    rid = require_approval(
        action="breaker_clear",
        account_key=APPROVAL_ACCOUNT_KEY,
        reason=reason or "manual_clear",
        ttl_sec=max(60, APPROVAL_TIMEOUT_SEC),
        timeout_sec=APPROVAL_TIMEOUT_SEC,
        poll_sec=2.5
    )
    tg_send(f"🔐 Approval OK • breaker_clear • req={rid}", priority="success")

def set_off(reason: str = "manual_clear", source: str = "human") -> None:
    try:
        _require_clear_approval(reason)
    except Exception as e:
        log_event("guard", "breaker_off_block", symbol="", account_uid="", payload={"error": str(e)}, level="error")
        tg_send(f"❌ Breaker OFF blocked • {e}", priority="error")
        raise

    cur_active = is_active()
    d = _normalize(_load_raw())
    d.update({"breach": False, "reason": reason, "ts": _now(), "ttl": 0, "source": source, "version": SCHEMA_VERSION})
    _save_raw(d)
    _touch_db_mirror(False, reason)

    log_event("guard", "breaker_off", symbol="", account_uid="", payload={"reason": reason, "source": source})
    if cur_active:
        _emit_off()

# Alias
def breach(reason: str = "manual", ttl_sec: Optional[int] = None, source: str = "human") -> None:
    set_on(reason=reason, ttl_sec=ttl_sec, source=source)

# ---------- guarded contexts / decorators ----------
T = TypeVar("T")

@contextlib.contextmanager
def breaker_guard(component: str = "", block_reason: str = "breaker_active"):
    if is_active():
        log_event("guard", "breaker_block_enter", symbol="", account_uid="", payload={
            "component": component, "reason": block_reason, "state": status()
        })
        raise RuntimeError(f"Breaker active: {block_reason}")
    yield

def wait_until_clear(timeout_sec: int = 120, poll_sec: float = 1.0) -> bool:
    deadline = _now() + int(timeout_sec)
    while _now() < deadline:
        if not is_active():
            return True
        time.sleep(max(0.05, float(poll_sec)))
    return not is_active()

def require_clear(component: str = "", block_reason: str = "breaker_active") -> Callable[[Callable[..., T]], Callable[..., T]]:
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
    if is_active():
        log_event("guard", "breaker_block_silent", symbol="", account_uid="", payload={
            "component": component, "why": why, "state": status()
        })
        yield False
    else:
        yield True

# ---------- auto-trip helpers ----------
def _read_health() -> Dict[str, Any]:
    try:
        p = pathlib.Path(HEALTH_PATH)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def trip_for_news(ttl_min: int = 10, detail: str = "") -> None:
    if not NEWS_LOCKOUT:
        return
    set_on_for(ttl_min, reason=f"news_lockout{(':'+detail) if detail else ''}", source="auto")

def trip_for_funding(ttl_min: int) -> None:
    if ttl_min > 0:
        set_on_for(ttl_min, reason="funding_lockout", source="auto")

def trip_for_connectivity(ttl_sec: int) -> None:
    if ttl_sec > 0:
        set_on(reason="connectivity", ttl_sec=ttl_sec, source="auto")

def trip_for_drawdown(dd_pct: float, now_dd: float) -> None:
    if dd_pct > 0 and now_dd >= dd_pct:
        set_on(reason=f"drawdown_{now_dd:.2f}_pct", ttl_sec=0, source="auto")

def trip_for_heartbeat(stale_sec: int) -> None:
    if stale_sec <= 0:
        return
    h = _read_health()
    try:
        bots = (h.get("bots") or {})
        now = _now()
        for name, meta in bots.items():
            last = int(meta.get("last", 0) or 0)
            critical = bool(meta.get("critical", False))
            if not critical:
                continue
            if last <= 0 or (now - last) > stale_sec:
                set_on(reason=f"heartbeat:{name}", ttl_sec=stale_sec, source="auto")
                break
    except Exception:
        pass

def auto_tick() -> None:
    """
    Single evaluation step; call periodically from a watchdog.
    Reads health file and env knobs, then trips/extends breaker as needed.
    Never auto-clears; clearing is explicit or via TTL expiry.
    """
    if not AUTO_ENABLE:
        return
    h = _read_health()

    # news
    if NEWS_LOCKOUT and bool(h.get("news_active", False)):
        trip_for_news(ttl_min=max(1, FUNDING_LOCKOUT_MIN or 10))

    # funding window
    if FUNDING_LOCKOUT_MIN > 0 and bool(h.get("funding_window", False)):
        trip_for_funding(ttl_min=FUNDING_LOCKOUT_MIN)

    # connectivity
    unhealthy = bool(h.get("relay_unhealthy", False) or h.get("exchange_unhealthy", False))
    if unhealthy and CONNECTIVITY_LOCKOUT_SEC > 0:
        trip_for_connectivity(ttl_sec=CONNECTIVITY_LOCKOUT_SEC)

    # drawdown
    dd = float(h.get("drawdown_pct", 0.0) or 0.0)
    if DD_LOCKOUT_PCT > 0:
        trip_for_drawdown(DD_LOCKOUT_PCT, dd)

    # heartbeat stale
    if HEARTBEAT_STALE_SEC > 0:
        trip_for_heartbeat(HEARTBEAT_STALE_SEC)

# ---------- boot reconciliation ----------
def _reconcile_db_with_file() -> None:
    """
    If DB says breaker ON and file says OFF, mirror DB into file.
    If file says ON and DB says OFF, mirror file into DB.
    Keeps reason from the non-empty source.
    """
    local = _load_raw()
    db_on, db_reason = _db_view()
    local_on = bool(local.get("breach"))
    if db_on and not local_on:
        local.update({"breach": True, "reason": db_reason or local.get("reason", "") or "db_sync",
                      "ts": _now(), "ttl": int(local.get("ttl", 0) or 0)})
        _save_raw(local)
    elif local_on and not db_on:
        _touch_db_mirror(True, local.get("reason", "") or "file_sync")

# Run reconciliation at import
try:
    _reconcile_db_with_file()
except Exception:
    pass

# ---------- CLI ----------
def _cli():
    ap = argparse.ArgumentParser(description="Global breaker control")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--on", action="store_true", help="Turn breaker ON")
    g.add_argument("--off", action="store_true", help="Turn breaker OFF")
    g.add_argument("--status", action="store_true", help="Print breaker status JSON")
    g.add_argument("--auto-tick", action="store_true", help="Run one auto evaluation step")

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
    if args.auto_tick:
        auto_tick()
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
