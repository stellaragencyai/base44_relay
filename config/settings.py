#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/config/settings.py — central env loader + repo paths

- Layered .env loading (repo root first, then OS env can override).
- Exposes common directories (ROOT, DIR_SIGNALS, DIR_LOGS, etc.).
- Provides attribute-style access to env values with sane defaults.
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Optional

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # will still function with OS env only

# ---------- repo paths ----------
# ROOT is repo root: .../Base 44/
ROOT = Path(__file__).resolve().parents[2]
DIR_STATE   = ROOT / ".state"
DIR_LOGS    = ROOT / "logs"
DIR_SIGNALS = ROOT / "signals"
DIR_REG     = ROOT / "registry"

for d in (DIR_STATE, DIR_LOGS, DIR_SIGNALS, DIR_REG):
    d.mkdir(parents=True, exist_ok=True)

# ---------- .env loading ----------
ENV_PATH = ROOT / ".env"
if load_dotenv is not None and ENV_PATH.exists():
    # load file first, allow OS env to override
    load_dotenv(ENV_PATH, override=False)

# ---------- helpers ----------
def _get(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return "" if default is None else str(default)
    return v

def _get_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}

def _get_float(name: str, default: float) -> float:
    try:
        return float(_get(name, str(default)))
    except Exception:
        return default

def _get_int(name: str, default: int) -> int:
    try:
        return int(float(_get(name, str(default))))
    except Exception:
        return default

# ---------- public "settings" surface ----------
class _Settings:
    # Paths
    ROOT: Path = ROOT
    DIR_STATE: Path = DIR_STATE
    DIR_LOGS: Path = DIR_LOGS
    DIR_SIGNALS: Path = DIR_SIGNALS
    DIR_REG: Path = DIR_REG

    # Timezone
    TZ: str = _get("TZ", "America/Phoenix")

    # Bybit creds
    BYBIT_API_KEY: str = _get("BYBIT_API_KEY", "")
    BYBIT_API_SECRET: str = _get("BYBIT_API_SECRET", "")
    BYBIT_ENV: str = _get("BYBIT_ENV", "mainnet").strip().lower()  # mainnet|testnet

    # Signal engine knobs (read by executor to keep in sync)
    SIG_MAKER_ONLY: bool  = _get_bool("SIG_MAKER_ONLY", True)
    SIG_SPREAD_MAX_BPS: float = _get_float("SIG_SPREAD_MAX_BPS", 8.0)
    SIG_TAG: str = _get("SIG_TAG", "B44")
    SIG_DRY_RUN: bool = _get_bool("SIG_DRY_RUN", True)

    # Executor-specific knobs
    EXEC_QTY_USDT: float = _get_float("EXEC_QTY_USDT", 5.0)
    EXEC_QTY_BASE: float = _get_float("EXEC_QTY_BASE", 0.0)
    EXEC_POST_ONLY: bool = _get_bool("EXEC_POST_ONLY", True)
    EXEC_SYMBOLS: str = _get("EXEC_SYMBOLS", "")
    EXEC_POLL_SEC: int = _get_int("EXEC_POLL_SEC", 2)

    # Telegram (optional; some modules use your core/notifier instead)
    TELEGRAM_BOT_TOKEN: str = _get("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = _get("TELEGRAM_CHAT_ID", "")

    # Logging
    LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")

settings = _Settings()
