#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/account_policy.py
Single source of truth for per-account automation modes and permissions.

Modes:
  - auto:          full automation
  - semi_auto:     automation allowed, but entries may require extra gates (unused for now)
  - manual:        bots never open/close; reconciler may protect if allowed
  - manual_allowed: like auto, but the human may place entries anytime (no bot-block)

Ownership:
  - entry_owner: "executor" | "human" | "both"
  - exit_owner:  "b44" | "human" | "both"

Env:
  ACCOUNT_POLICY_FILE=./registry/account_policy.json
"""

import os, json, threading
from typing import Dict, Any, Optional

_lock = threading.RLock()
_policy: Dict[str, Any] = {}
_policy_mtime: float = 0.0

def _path() -> str:
    return os.getenv("ACCOUNT_POLICY_FILE","./registry/account_policy.json")

def _load_file() -> Dict[str, Any]:
    path = _path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        # Try example file
        alt = path.replace(".json", ".example.json")
        with open(alt, "r", encoding="utf-8") as f:
            return json.load(f)

def _refresh():
    global _policy, _policy_mtime
    p = _path()
    try:
        m = os.path.getmtime(p)
    except FileNotFoundError:
        m = 0.0
    if m != _policy_mtime or not _policy:
        _policy = _load_file()
        _policy_mtime = m

def _idx() -> Dict[str, Dict[str, Any]]:
    """Return map of uid->config including main."""
    cfg = _policy or {}
    index = {}
    # main
    main = cfg.get("main") or {}
    index[str(main.get("uid","MAIN"))] = main
    # subs
    for sa in cfg.get("sub_accounts", []):
        index[str(sa.get("uid",""))] = sa
    return index

def _with_defaults(node: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(defaults or {})
    out.update(node or {})
    return out

def get_account(uid: str) -> Dict[str, Any]:
    """Fetch account config merged with defaults; uid='MAIN' for main account."""
    with _lock:
        _refresh()
        cfg = _policy or {}
        defaults = cfg.get("defaults", {})
        idx = _idx()
        node = idx.get(str(uid)) or {}
        return _with_defaults(node, defaults)

def may_open(uid: str) -> bool:
    """Can bots open positions on this account?"""
    a = get_account(uid)
    mode = a.get("mode","auto")
    owner = a.get("entry_owner","executor")
    if mode == "manual":
        return False
    if owner in ("executor","both"):
        return True
    return False

def may_manage_exits(uid: str) -> bool:
    """Can bots maintain exits on this account (TP ladder / SL)?"""
    a = get_account(uid)
    owner = a.get("exit_owner","b44")
    return owner in ("b44","both")

def risk_multiplier(uid: str) -> float:
    """Scale global RISK_PCT by this factor for the account."""
    a = get_account(uid)
    try:
        return float(a.get("risk_multiplier", 1.0))
    except Exception:
        return 1.0

def symbol_allowed(uid: str, symbol: str) -> bool:
    a = get_account(uid)
    syms = a.get("symbols") or []
    return True if not syms else symbol.upper() in [s.upper() for s in syms]

def allow_manual_main_entries() -> bool:
    with _lock:
        _refresh()
        rules = (_policy or {}).get("rules", {})
        return bool(rules.get("allow_manual_main_entries", True))

def reconciler_can_protect_manual() -> bool:
    with _lock:
        _refresh()
        rules = (_policy or {}).get("rules", {})
        return bool(rules.get("reconciler_can_protect_manual", True))
