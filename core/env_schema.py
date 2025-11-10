#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.env_schema — single source of truth for required configuration.

Provides:
- ENV_SPEC: declarative schema (type, required, default, allowed, min/max)
- load_env(): returns dict of parsed values
- validate_env(strict=True): raises ValueError with aggregated issues
- mask(s): utility to mask secrets in logs

This covers critical bits for Bybit, Telegram, ownership, and runtime flags.
Extend as needed, but don’t scatter config checks across the codebase again.
"""

from __future__ import annotations
import os, re, json
from typing import Any, Dict, Optional, Tuple

def mask(s: Optional[str], keep: int = 6) -> str:
    if not s:
        return "<empty>"
    s = str(s)
    if len(s) <= keep * 2:
        return s[0:2] + "…" + s[-2:]
    return s[:keep] + "…" + s[-keep:]

_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{30,}$")

def _bool(x: Any) -> bool:
    return str(x).strip().lower() in ("1","true","yes","on")

def _int(x: Any) -> int:
    return int(str(x).strip())

def _float(x: Any) -> float:
    return float(str(x).strip())

def _str(x: Any) -> str:
    return str(x).strip()

def _nonempty(s: str) -> bool:
    return bool(s and s.strip() and s.strip().lower() not in {"changeme", "<token>", "<chat_id>"})


ENV_SPEC: Dict[str, Dict[str, Any]] = {
    # Exchange
    "BYBIT_BASE_URL": {"type": _str, "required": False, "default": "https://api.bybit.com"},
    "BYBIT_API_KEY":  {"type": _str, "required": True,  "secret": True, "validate": _nonempty},
    "BYBIT_API_SECRET":{"type": _str, "required": True,  "secret": True, "validate": _nonempty},
    "EXEC_ACCOUNT_UID":{"type": _str, "required": False, "default": ""},
    "OWNERSHIP_SUB_UID":{"type": _str, "required": False, "default": ""},
    "OWNERSHIP_STRATEGY":{"type": _str, "required": False, "default": "A?"},

    # Guard & runtime
    "GUARD_STATE_FILE":{"type": _str, "required": False, "default": ".state/guard_state.json"},
    "GUARD_NOTIFY":     {"type": _bool,"required": False, "default": True},
    "SIG_DRY_RUN":      {"type": _bool,"required": False, "default": True},
    "EXEC_DRY_RUN":     {"type": _bool,"required": False, "default": True},

    # Telegram (single-bot fallback)
    "TELEGRAM_BOT_TOKEN":{"type": _str, "required": False, "secret": True},
    "TELEGRAM_CHAT_ID":  {"type": _str, "required": False},
    "TELEGRAM_CHAT_IDS": {"type": _str, "required": False},

    # Multi-bot config path
    "TG_CONFIG_PATH": {"type": _str, "required": False, "default": "cfg/tg_subaccounts.yaml"},

    # Filesystem hygiene
    "STATE_DIR": {"type": _str, "required": False, "default": "state"},
    "LOGS_DIR":  {"type": _str, "required": False, "default": "logs"},

    # Model + outcome
    "OUTCOME_PATH":      {"type": _str, "required": False, "default": "state/outcomes.jsonl"},
    "MODEL_STATE_PATH":  {"type": _str, "required": False, "default": "state/model_state.json"},
}

def load_env() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, meta in ENV_SPEC.items():
        typ = meta.get("type", _str)
        default = meta.get("default", None)
        raw = os.getenv(key, default)
        try:
            out[key] = typ(raw) if raw is not None else None
        except Exception:
            out[key] = raw
    return out

def validate_env(strict: bool = True) -> Tuple[bool, str]:
    env = load_env()
    problems = []

    for key, meta in ENV_SPEC.items():
        required = meta.get("required", False)
        val = env.get(key, None)
        if required and (val is None or (isinstance(val, str) and not val.strip())):
            problems.append(f"{key}: missing")
            continue
        validator = meta.get("validate")
        if callable(validator) and val is not None:
            try:
                ok = validator(val)
            except Exception:
                ok = False
            if not ok:
                problems.append(f"{key}: invalid value")
        # Special rule: if single-bot Telegram is used, ensure token looks sane
        if key == "TELEGRAM_BOT_TOKEN" and val:
            if not _TOKEN_RE.match(str(val).strip().replace("\ufeff","")):
                problems.append("TELEGRAM_BOT_TOKEN: bad format")

    if problems:
        msg = "Env validation failed:\n- " + "\n- ".join(problems)
        if strict:
            raise ValueError(msg)
        return False, msg
    return True, "ok"
