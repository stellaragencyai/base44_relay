#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/time_lock_manager.py — Time-locked account guard + lightweight unlock token flow

Purpose
- Enforce time-based locks and milestone locks for sub-accounts (or account keys).
- Simple CLI + optional small Flask API for other bots to call.
- HMAC unlock-token generator/validator so we can later require a second party to sign/approve tokens.
- Persistent state in .state/locks.json and audit log .state/locks_audit.log

Usage (CLI)
  python -m core.time_lock_manager --lock sub:260417078 --until "2025-12-01T00:00:00Z" --reason "vesting"
  python -m core.time_lock_manager --milestone-lock sub:260417078 --milestone tier3_seed
  python -m core.time_lock_manager --unlock-token GENERATE sub:260417078 --ttl-seconds 3600
  python -m core.time_lock_manager --consume-unlock-token <token>

API (optional)
  Run server: python -m core.time_lock_manager --serve --port 5001
  GET /v1/lock/<account_key>   → { locked: true/false, reason, until, milestones }
  POST /v1/lock                 → body { account_key, until_iso, reason }
  POST /v1/unlock-token/consume → body { token }

Env
  TIMELOCK_HMAC_SECRET=some_long_secret_here
  TZ=UTC
"""
from __future__ import annotations
import os
import json
import time
import hmac
import hashlib
import base64
import argparse
import threading
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

# Config + paths
ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
LOCKS_FILE = STATE_DIR / "locks.json"
AUDIT_LOG = STATE_DIR / "locks_audit.log"

HMAC_SECRET = os.getenv("TIMELOCK_HMAC_SECRET", "").encode("utf-8")
DEFAULT_TZ = os.getenv("TZ", "UTC")

_lock = threading.Lock()

# ---------- helpers ----------
def _now_ts() -> int:
    return int(time.time())

def _iso_to_ts(s: str) -> int:
    # Accept naive ISO or with timezone. Expect UTC by default.
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # try common format fallback
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def _ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def _audit(msg: str) -> None:
    line = f"{_ts_to_iso(_now_ts())} {msg}\n"
    with open(AUDIT_LOG, "a", encoding="utf-8") as f:
        f.write(line)

def _load_state() -> Dict[str, Any]:
    try:
        if LOCKS_FILE.exists():
            return json.loads(LOCKS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"locks": {}}  # account_key -> lock obj

def _save_state(state: Dict[str, Any]) -> None:
    with _lock:
        LOCKS_FILE.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

# ---------- core functions ----------
def lock_account(account_key: str, until_ts: Optional[int] = None, reason: str = "") -> Dict[str, Any]:
    """
    Create or update a lock. If until_ts is None, lock is indefinite until manual clear.
    """
    state = _load_state()
    locks = state.setdefault("locks", {})
    locks[account_key] = {
        "locked": True,
        "reason": reason or "manual",
        "until": int(until_ts) if until_ts else 0,
        "milestones": {},  # milestone_name -> {"locked": True/False, "created":ts}
        "created": _now_ts(),
    }
    _save_state(state)
    _audit(f"LOCK_SET {account_key} reason={reason!r} until={locks[account_key]['until']}")
    return locks[account_key]

def milestone_lock(account_key: str, milestone: str, reason: str = "") -> Dict[str, Any]:
    state = _load_state()
    locks = state.setdefault("locks", {})
    obj = locks.setdefault(account_key, {"locked": False, "reason": "", "until": 0, "milestones": {}, "created": _now_ts()})
    obj["milestones"][milestone] = {"locked": True, "created": _now_ts(), "reason": reason}
    # if you want milestone locks to imply account lock, set obj["locked"]=True
    obj["locked"] = True
    _save_state(state)
    _audit(f"MILESTONE_LOCK {account_key} milestone={milestone} reason={reason!r}")
    return obj

def milestone_unlock(account_key: str, milestone: str) -> Dict[str, Any]:
    state = _load_state()
    locks = state.setdefault("locks", {})
    obj = locks.get(account_key)
    if not obj:
        raise KeyError("no lock for account")
    obj["milestones"].setdefault(milestone, {})["locked"] = False
    # If all milestones unlocked and 'until' not set/expired, clear account-level lock
    still_locked = False
    for m, meta in obj.get("milestones", {}).items():
        if meta.get("locked"):
            still_locked = True; break
    if not still_locked and (not obj.get("until") or obj.get("until") <= _now_ts()):
        obj["locked"] = False
    _save_state(state)
    _audit(f"MILESTONE_UNLOCK {account_key} milestone={milestone}")
    return obj

def unlock_account(account_key: str, reason: str = "") -> Dict[str, Any]:
    state = _load_state()
    locks = state.setdefault("locks", {})
    if account_key not in locks:
        return {}
    obj = locks[account_key]
    obj["locked"] = False
    obj["until"] = 0
    # mark milestones unlocked
    for m in obj.get("milestones", {}):
        obj["milestones"][m]["locked"] = False
    _save_state(state)
    _audit(f"UNLOCK {account_key} reason={reason!r}")
    return obj

def is_locked(account_key: str) -> Dict[str, Any]:
    state = _load_state()
    obj = state.get("locks", {}).get(account_key, None)
    if not obj:
        return {"locked": False}
    # auto-expire time-based lock
    until = int(obj.get("until", 0) or 0)
    if until and until <= _now_ts():
        # expire: clear until but leave milestones
        obj["until"] = 0
        # If no milestones locked, clear locked flag
        if not any(m.get("locked") for m in obj.get("milestones", {}).values()):
            obj["locked"] = False
        _save_state(state)
    return {
        "locked": bool(obj.get("locked", False)),
        "reason": obj.get("reason", ""),
        "until": int(obj.get("until", 0) or 0),
        "milestones": obj.get("milestones", {}),
        "created": obj.get("created", 0)
    }

# ---------- unlock token flow (HMAC) ----------
# Token format: base64url( HMAC || "." || payload_json )
# payload_json includes account_key, exp (unix ts), nonce
def _sign_payload(payload_bytes: bytes) -> bytes:
    if not HMAC_SECRET:
        raise RuntimeError("TIMELOCK_HMAC_SECRET not set in env")
    sig = hmac.new(HMAC_SECRET, payload_bytes, hashlib.sha256).digest()
    return sig

def generate_unlock_token(account_key: str, ttl_seconds: int = 3600) -> str:
    exp = _now_ts() + int(ttl_seconds)
    payload = {"a": account_key, "exp": exp, "n": base64.urlsafe_b64encode(os.urandom(6)).decode("ascii")}
    pb = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _sign_payload(pb)
    token = base64.urlsafe_b64encode(sig + b"." + pb).decode("ascii")
    _audit(f"UNLOCK_TOKEN_ISSUED {account_key} exp={exp}")
    return token

def validate_unlock_token(token: str) -> Dict[str, Any]:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        sig, sep, pb = raw.partition(b".")
        if not sep:
            raise ValueError("invalid token sep")
        if not HMAC_SECRET:
            raise RuntimeError("HMAC secret not configured")
        expected = _sign_payload(pb)
        if not hmac.compare_digest(expected, sig):
            raise ValueError("signature mismatch")
        payload = json.loads(pb.decode("utf-8"))
        if payload.get("exp", 0) < _now_ts():
            raise ValueError("token expired")
        return {"ok": True, "payload": payload}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def consume_unlock_token(token: str) -> Dict[str, Any]:
    v = validate_unlock_token(token)
    if not v.get("ok"):
        _audit(f"UNLOCK_TOKEN_INVALID {v.get('error')}")
        return v
    acct = v["payload"]["a"]
    unlock_account(acct, reason=f"token_consume")
    _audit(f"UNLOCK_TOKEN_CONSUMED {acct}")
    return {"ok": True, "account": acct}

# ---------- enforcement decorator ----------
def require_unlocked_or_raise(account_key: str, *, raise_exc=True) -> bool:
    st = is_locked(account_key)
    if st.get("locked"):
        if raise_exc:
            raise RuntimeError(f"Account {account_key} locked: reason={st.get('reason')}, until={st.get('until')}")
        return False
    return True

# ---------- CLI + (optional) Flask API ----------
def _cli():
    p = argparse.ArgumentParser()
    p.add_argument("--lock", help="Lock account_key (e.g. sub:260417078 or main)", type=str)
    p.add_argument("--until", help="ISO datetime (UTC) when lock auto-expires", type=str)
    p.add_argument("--reason", help="Reason text", type=str, default="")
    p.add_argument("--unlock", help="Unlock account_key immediately", type=str)
    p.add_argument("--milestone-lock", help="Milestone lock <acct>:<milestone>", type=str)
    p.add_argument("--milestone-unlock", help="Milestone unlock <acct>:<milestone>", type=str)
    p.add_argument("--status", help="Show lock status for account_key", type=str)
    p.add_argument("--generate-token", help="GENERATE account_key", type=str)
    p.add_argument("--ttl-seconds", help="TTL for generated token", type=int, default=3600)
    p.add_argument("--consume-unlock-token", help="Consume token string", type=str)
    p.add_argument("--serve", help="Run HTTP API", action="store_true")
    p.add_argument("--port", help="API port", type=int, default=5001)
    args = p.parse_args()

    if args.lock:
        until_ts = None
        if args.until:
            until_ts = _iso_to_ts(args.until)
        out = lock_account(args.lock, until_ts, args.reason)
        print(json.dumps(out, indent=2))
        return

    if args.milestone_lock:
        acct, _, ms = args.milestone_lock.partition(":")
        out = milestone_lock(acct, ms or "default", args.reason)
        print(json.dumps(out, indent=2)); return

    if args.milestone_unlock:
        acct, _, ms = args.milestone_unlock.partition(":")
        out = milestone_unlock(acct, ms or "default")
        print(json.dumps(out, indent=2)); return

    if args.unlock:
        out = unlock_account(args.unlock, reason=args.reason)
        print(json.dumps(out, indent=2)); return

    if args.status:
        out = is_locked(args.status)
        print(json.dumps(out, indent=2)); return

    if args.generate_token:
        token = generate_unlock_token(args.generate_token, args.ttl_seconds)
        print(token); return

    if args.consume_unlock_token:
        out = consume_unlock_token(args.consume_unlock_token)
        print(json.dumps(out, indent=2)); return

    if args.serve:
        _run_api(port=args.port); return

    p.print_help()

# Minimal HTTP API to integrate with other bots
def _run_api(port: int = 5001):
    try:
        from flask import Flask, jsonify, request
    except Exception:
        raise RuntimeError("Flask required for API. Install flask with pip install flask")

    app = Flask("time_lock_manager")

    @app.route("/v1/lock/<account_key>", methods=["GET"])
    def api_status(account_key):
        return jsonify(is_locked(account_key))

    @app.route("/v1/lock", methods=["POST"])
    def api_lock():
        body = request.get_json() or {}
        acct = body.get("account_key")
        until = body.get("until_iso")
        reason = body.get("reason", "")
        if not acct:
            return jsonify({"error": "account_key required"}), 400
        until_ts = None
        if until:
            try:
                until_ts = _iso_to_ts(until)
            except Exception:
                return jsonify({"error": "invalid until_iso"}), 400
        obj = lock_account(acct, until_ts, reason)
        return jsonify(obj)

    @app.route("/v1/unlock-token/generate", methods=["POST"])
    def api_gen_token():
        body = request.get_json() or {}
        acct = body.get("account_key")
        ttl = int(body.get("ttl_seconds", 3600))
        if not acct:
            return jsonify({"error": "account_key required"}), 400
        token = generate_unlock_token(acct, ttl)
        return jsonify({"token": token, "exp": _now_ts() + ttl})

    @app.route("/v1/unlock-token/consume", methods=["POST"])
    def api_consume_token():
        body = request.get_json() or {}
        tok = body.get("token")
        if not tok:
            return jsonify({"error": "token required"}), 400
        out = consume_unlock_token(tok)
        return jsonify(out)

    print(f"[time_lock_manager] serving on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    _cli()
