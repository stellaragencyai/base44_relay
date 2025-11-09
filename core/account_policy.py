#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/account_policy.py — per-account automation modes, permissions, and symbol gating
with hot-reload and sensible fallbacks.

Back-compat: preserves your fields (mode, entry_owner, exit_owner, defaults, rules)
and functions used elsewhere:
    - get_account(uid) -> dict
    - may_open(uid) -> bool
    - may_manage_exits(uid) -> bool
    - risk_multiplier(uid) -> float
    - symbol_allowed(uid, symbol) -> bool
    - allow_manual_main_entries() -> bool
    - reconciler_can_protect_manual() -> bool

Enhancements:
    • Hot-reload on mtime change (no restarts).
    • Graceful fallbacks if policy file missing/invalid.
    • Case-insensitive symbol allow/deny with deny-win.
    • Support for additional modes: "halt" (hard stop), "semi_auto".
    • Optional per-account symbols_allow / symbols_deny in addition to legacy "symbols".
    • Thread-safe reads via RLock.
    • CLI inspector (--uid MAIN --check BTCUSDT,ETHUSDT --reload).

Schema (JSON) example (first found file is used):
    1) env ACCOUNT_POLICY_FILE
    2) <ROOT>/config/account_policy.json
    3) ./registry/account_policy.json

{
  "defaults": {
    "mode": "auto",                      // auto | semi_auto | manual | manual_allowed | halt
    "may_open": true,                    // master boolean gate
    "entry_owner": "executor",           // executor | human | both
    "exit_owner": "b44",                 // b44 | human | both
    "risk_multiplier": 1.0,
    "symbols_allow": ["BTCUSDT","ETHUSDT"],
    "symbols_deny": []
  },
  "main": {
    "uid": "MAIN",
    "mode": "auto",
    "risk_multiplier": 1.15
  },
  "sub_accounts": [
    {
      "uid": "SUB:260417078",
      "mode": "manual",
      "may_open": false,
      "symbols_deny": ["DOGEUSDT"]
    }
  ],
  "rules": {
    "allow_manual_main_entries": true,
    "reconciler_can_protect_manual": true
  }
}
"""

from __future__ import annotations
import os
import json
import threading
from pathlib import Path
from typing import Dict, Any, Optional, List

# Optional settings to resolve project root
try:
    from core.config import settings
except Exception:
    settings = None  # type: ignore

# ---------- internals / state ----------
_lock = threading.RLock()
_policy: Dict[str, Any] = {}
_policy_mtime: float = -1.0
_policy_path: Optional[Path] = None

# ---------- path resolution ----------
def _default_paths() -> List[Path]:
    paths: List[Path] = []
    env = os.getenv("ACCOUNT_POLICY_FILE")
    if env:
        paths.append(Path(env).expanduser())
    if getattr(settings, "ROOT", None):
        paths.append(Path(settings.ROOT) / "config" / "account_policy.json")
    # legacy location
    paths.append(Path("./registry/account_policy.json"))
    return paths

def _resolve_path() -> Path:
    for p in _default_paths():
        if p.exists():
            return p
    # last resort: first candidate (even if missing) so we keep a stable path
    lst = _default_paths()
    return lst[0] if lst else Path("./registry/account_policy.json")

# ---------- io ----------
def _load_file(path: Path) -> Dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return raw
    except FileNotFoundError:
        pass
    except Exception:
        pass
    # No file or bad JSON → empty policy, we’ll use defaults
    return {}

def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except Exception:
        return -1.0

# ---------- normalization / merge ----------
_DEF = {
    "mode": "auto",               # auto | semi_auto | manual | manual_allowed | halt
    "may_open": True,
    "entry_owner": "executor",    # executor | human | both
    "exit_owner": "b44",          # b44 | human | both
    "risk_multiplier": 1.0,
    "symbols_allow": [],
    "symbols_deny": [],
    # legacy support
    "symbols": [],                # legacy allow list
}

def _norm_list(xs) -> List[str]:
    if not xs:
        return []
    if isinstance(xs, list):
        return [str(x).upper().strip() for x in xs if str(x).strip()]
    return []

def _normalize_account(node: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(_DEF)
    if not isinstance(node, dict):
        return out
    out["mode"] = str(node.get("mode", out["mode"])).strip().lower()
    out["may_open"] = bool(node.get("may_open", out["may_open"]))
    out["entry_owner"] = str(node.get("entry_owner", out["entry_owner"])).strip().lower()
    out["exit_owner"] = str(node.get("exit_owner", out["exit_owner"])).strip().lower()
    try:
        out["risk_multiplier"] = float(node.get("risk_multiplier", out["risk_multiplier"]))
    except Exception:
        pass
    # lists
    # Prefer new keys, then legacy "symbols" as allow
    out["symbols_allow"] = _norm_list(node.get("symbols_allow", node.get("symbols")))
    out["symbols_deny"] = _norm_list(node.get("symbols_deny"))
    return out

def _merge(defaults: Dict[str, Any], acct: Dict[str, Any]) -> Dict[str, Any]:
    base = _normalize_account(defaults)
    over = _normalize_account(acct)
    merged = dict(base)
    merged.update(over)
    # dedupe allow/deny
    allow = set(merged.get("symbols_allow", []))
    deny = set(merged.get("symbols_deny", []))
    # If legacy "symbols" exists in acct, we already folded it into allow above
    merged["symbols_allow"] = sorted(allow)
    merged["symbols_deny"] = sorted(deny)
    return merged

# ---------- refresh / hot-reload ----------
def _refresh(force: bool = False) -> None:
    global _policy, _policy_mtime, _policy_path
    with _lock:
        if _policy_path is None:
            _policy_path = _resolve_path()
        p = _policy_path
        mt = _mtime(p)
        if (not force) and _policy and _policy_mtime == mt:
            return
        blob = _load_file(p)
        defaults = _normalize_account(blob.get("defaults", {}))
        # main account node
        main = blob.get("main") or {}
        if not isinstance(main, dict):
            main = {}
        main_uid = str(main.get("uid", "MAIN"))
        main_norm = _merge(defaults, main)

        # sub accounts
        sub_list = blob.get("sub_accounts") or []
        subs: Dict[str, Dict[str, Any]] = {}
        if isinstance(sub_list, list):
            for sa in sub_list:
                if not isinstance(sa, dict):
                    continue
                uid = str(sa.get("uid", "")).strip() or ""
                if not uid:
                    continue
                subs[uid] = _merge(defaults, sa)

        rules = blob.get("rules") or {}
        if not isinstance(rules, dict):
            rules = {}

        _policy = {
            "defaults": defaults,
            "index": {main_uid: main_norm, **subs},
            "rules": {
                "allow_manual_main_entries": bool(rules.get("allow_manual_main_entries", True)),
                "reconciler_can_protect_manual": bool(rules.get("reconciler_can_protect_manual", True)),
            },
        }
        _policy_mtime = mt

# ---------- public api ----------
def reload_policy() -> None:
    """Force reload from disk."""
    _refresh(force=True)

def policy_path() -> str:
    """Return current policy file path (resolved)."""
    if _policy_path is None:
        _refresh(force=True)
    return str(_policy_path)

def get_account(uid: str) -> Dict[str, Any]:
    """Return merged view for uid; falls back to defaults if unknown."""
    _refresh()
    uid = str(uid or "").strip() or "MAIN"
    with _lock:
        idx = (_policy.get("index") or {})
        defaults = _policy.get("defaults") or dict(_DEF)
        return idx.get(uid, defaults)

def may_open(uid: str) -> bool:
    """
    Can bots open positions for this account?
    - mode == "halt" → False
    - mode == "manual" → False unless may_open True and entry_owner allows
    - entry_owner must be "executor" or "both"
    """
    a = get_account(uid)
    mode = a.get("mode", "auto")
    owner = a.get("entry_owner", "executor")
    may = bool(a.get("may_open", True))

    if mode == "halt":
        return False
    if owner not in ("executor", "both"):
        return False
    if mode == "manual":
        return may and owner in ("executor", "both")
    # semi_auto, manual_allowed, auto: defer to may_open flag
    return may

def may_manage_exits(uid: str) -> bool:
    """Can bots maintain exits (TP ladder / SL) for this account?"""
    a = get_account(uid)
    owner = a.get("exit_owner", "b44")
    return owner in ("b44", "both")

def risk_multiplier(uid: str) -> float:
    """Scale base risk by this factor; clamped to [0, 5]."""
    a = get_account(uid)
    try:
        v = float(a.get("risk_multiplier", 1.0))
        if v < 0:
            return 0.0
        if v > 5.0:
            return 5.0
        return v
    except Exception:
        return 1.0

def symbol_allowed(uid: str, symbol: str) -> bool:
    """
    Per-account symbol allow/deny.
    - If deny list has the symbol → False
    - If allow list empty → True (unless denied)
    - Else must be in allow
    Also supports legacy per-account "symbols" (treated as allow list).
    """
    a = get_account(uid)
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    deny = set(a.get("symbols_deny") or [])
    if sym in deny:
        return False
    allow = set(a.get("symbols_allow") or [])
    # legacy "symbols" already folded into symbols_allow in normalization
    return True if not allow else sym in allow

def allow_manual_main_entries() -> bool:
    _refresh()
    with _lock:
        rules = _policy.get("rules") or {}
        return bool(rules.get("allow_manual_main_entries", True))

def reconciler_can_protect_manual() -> bool:
    _refresh()
    with _lock:
        rules = _policy.get("rules") or {}
        return bool(rules.get("reconciler_can_protect_manual", True))

# ---------- CLI inspector ----------
def _cli():
    import argparse, json as _json
    ap = argparse.ArgumentParser(description="Account policy inspector")
    ap.add_argument("--uid", type=str, default="MAIN", help="Account UID to inspect")
    ap.add_argument("--check", type=str, default=None, help="Comma list of symbols to check allow/deny")
    ap.add_argument("--reload", action="store_true", help="Force reload policy from disk")
    args = ap.parse_args()

    if args.reload:
        reload_policy()

    pol = get_account(args.uid)
    out = {
        "path": policy_path(),
        "uid": args.uid,
        "policy": pol,
        "may_open": may_open(args.uid),
        "may_manage_exits": may_manage_exits(args.uid),
        "risk_multiplier": risk_multiplier(args.uid),
        "allow_manual_main_entries": allow_manual_main_entries(),
        "reconciler_can_protect_manual": reconciler_can_protect_manual(),
    }
    print(_json.dumps(out, indent=2))
    if args.check:
        for s in [t.strip() for t in args.check.split(",") if t.strip()]:
            print(f"{s.upper()}: {'allowed' if symbol_allowed(args.uid, s) else 'denied'}")

if __name__ == "__main__":
    _cli()
