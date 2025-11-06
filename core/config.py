#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Central Settings Loader

One source of truth for:
- Environment variables (.env + process env)
- Directory paths (logs, signals, state)
- Core toggles and defaults (poll rates, timezones)
- Credential presence checks (Bybit, Telegram)

Import from bots and tools like:
    from core.config import settings

This module is import-safe: it creates needed folders on first import.
"""

from __future__ import annotations
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Optional .env support
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _to_int(value: Optional[str], default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _coalesce(*vals: Optional[str], default: str = "") -> str:
    for v in vals:
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


@dataclass(frozen=True)
class Settings:
    # Root
    ROOT: Path

    # Timezone
    TZ: str

    # Directories
    DIR_LOGS: Path
    DIR_SIGNALS: Path
    DIR_STATE: Path
    DIR_CONFIG: Path

    # Bybit
    BYBIT_API_KEY: str
    BYBIT_API_SECRET: str
    BYBIT_ENV: str  # "mainnet" | "testnet"
    BYBIT_BASE_URL: str

    # Relay (optional, for internal calls if needed)
    RELAY_URL: Optional[str]
    RELAY_TOKEN: Optional[str]

    # Telegram (optional but recommended)
    TELEGRAM_BOT_TOKEN: Optional[str]
    TELEGRAM_CHAT_ID: Optional[str]

    # Signal Engine toggles/defaults
    SIG_ENABLED: bool
    SIG_DRY_RUN: bool
    SIG_SYMBOLS: str
    SIG_TIMEFRAMES: str
    SIG_BIAS_TF: str
    SIG_HEARTBEAT_MIN: int
    SIG_MIN_ADX_BIAS: int
    SIG_MIN_ATR_PCT: float
    SIG_MIN_VOL_Z: float

    # TP/SL Manager basics
    TPSL_ENABLED: bool
    TPSL_POLL_S: int
    TPSL_TARGET_RUNGS: int

    # Network
    HTTP_TIMEOUT_S: int
    PROXY_URL: Optional[str]


def _load_env(root: Path) -> None:
    """Load .env if present at project root."""
    if load_dotenv is None:
        return
    # First try project root .env, then cwd as fallback
    env_file = root / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=str(env_file), override=False)
    else:
        load_dotenv(override=False)


def _derive_bybit_url(env_name: str) -> str:
    env_name = (env_name or "mainnet").strip().lower()
    if env_name in ("testnet", "demo", "paper"):
        return "https://api-demo.bybit.com"
    return "https://api.bybit.com"


def _ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[config] failed to create dir {d}: {e}", file=sys.stderr)


def _validate_required(name: str, value: Optional[str], allow_empty: bool = False) -> None:
    if allow_empty:
        return
    if value is None or str(value).strip() == "":
        print(f"[config] WARNING: `{name}` is missing. Some features may not work.", file=sys.stderr)


def _build_settings() -> Settings:
    # Try to detect project root by walking up until we see hallmark folders
    cwd = Path.cwd()
    candidates = [cwd]
    # If imported from a submodule, likely core/config.py lives under a known root
    if "BASE44_ROOT" in os.environ:
        candidates.insert(0, Path(os.environ["BASE44_ROOT"]).expanduser())

    # Heuristic: first path that contains any of these is our root
    hallmarks = {"core", "bots", ".state"}
    root = None
    for c in candidates:
        if any((c / h).exists() for h in hallmarks):
            root = c
            break
    if root is None:
        # Fallback to cwd; user can set BASE44_ROOT to be explicit
        root = cwd

    _load_env(root)

    # Paths
    dir_logs = (root / "logs").resolve()
    dir_signals = (root / "signals").resolve()
    dir_state = (root / ".state").resolve()
    dir_config = (root / "config").resolve()

    _ensure_dirs(dir_logs, dir_signals, dir_state, dir_config)

    # Core env
    tz = _coalesce(os.getenv("TZ"), default="Europe/London")

    # Bybit creds
    bybit_key = os.getenv("BYBIT_API_KEY")
    bybit_sec = os.getenv("BYBIT_API_SECRET")
    bybit_env = _coalesce(os.getenv("BYBIT_ENV"), default="mainnet").lower()
    bybit_url = _derive_bybit_url(bybit_env)

    _validate_required("BYBIT_API_KEY", bybit_key)
    _validate_required("BYBIT_API_SECRET", bybit_sec)

    # Relay (optional)
    relay_url = os.getenv("RELAY_URL")
    relay_token = os.getenv("RELAY_TOKEN")

    # Telegram (optional)
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")

    # Signal Engine defaults
    sig_enabled = _to_bool(os.getenv("SIG_ENABLED"), default=False)
    sig_dry = _to_bool(os.getenv("SIG_DRY_RUN"), default=True)
    sig_symbols = _coalesce(os.getenv("SIG_SYMBOLS"), default="BTCUSDT,ETHUSDT")
    sig_tfs = _coalesce(os.getenv("SIG_TIMEFRAMES"), default="5,15")
    sig_bias_tf = _coalesce(os.getenv("SIG_BIAS_TF"), default="60")
    sig_hb = _to_int(os.getenv("SIG_HEARTBEAT_MIN"), default=10)
    sig_min_adx = _to_int(os.getenv("SIG_MIN_ADX_BIAS"), default=18)
    sig_min_atr_pct = float(_coalesce(os.getenv("SIG_MIN_ATR_PCT"), default="0.15"))
    sig_min_vol_z = float(_coalesce(os.getenv("SIG_MIN_VOL_Z"), default="0.5"))

    # TP/SL Manager defaults
    tpsl_enabled = _to_bool(os.getenv("TPSL_ENABLED"), default=False)
    tpsl_poll = _to_int(os.getenv("TPSL_POLL_S"), default=10)
    tpsl_rungs = _to_int(os.getenv("TPSL_TARGET_RUNGS"), default=50)

    # Network
    http_timeout = _to_int(os.getenv("HTTP_TIMEOUT_S"), default=15)
    proxy_url = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("PROXY_URL")

    return Settings(
        ROOT=root,
        TZ=tz,
        DIR_LOGS=dir_logs,
        DIR_SIGNALS=dir_signals,
        DIR_STATE=dir_state,
        DIR_CONFIG=dir_config,
        BYBIT_API_KEY=bybit_key or "",
        BYBIT_API_SECRET=bybit_sec or "",
        BYBIT_ENV=bybit_env,
        BYBIT_BASE_URL=bybit_url,
        RELAY_URL=relay_url,
        RELAY_TOKEN=relay_token,
        TELEGRAM_BOT_TOKEN=tg_token,
        TELEGRAM_CHAT_ID=tg_chat,
        SIG_ENABLED=sig_enabled,
        SIG_DRY_RUN=sig_dry,
        SIG_SYMBOLS=sig_symbols,
        SIG_TIMEFRAMES=sig_tfs,
        SIG_BIAS_TF=sig_bias_tf,
        SIG_HEARTBEAT_MIN=sig_hb,
        SIG_MIN_ADX_BIAS=sig_min_adx,
        SIG_MIN_ATR_PCT=sig_min_atr_pct,
        SIG_MIN_VOL_Z=sig_min_vol_z,
        TPSL_ENABLED=tpsl_enabled,
        TPSL_POLL_S=tpsl_poll,
        TPSL_TARGET_RUNGS=tpsl_rungs,
        HTTP_TIMEOUT_S=http_timeout,
        PROXY_URL=proxy_url,
    )


# Singleton instance exposed to the rest of the codebase
settings: Settings = _build_settings()


# Pretty print a quick diagnostic when run directly
if __name__ == "__main__":
    from pprint import pprint
    print("Base44 Settings Snapshot")
    print("------------------------")
    printable = {
        "ROOT": str(settings.ROOT),
        "TZ": settings.TZ,
        "DIR_LOGS": str(settings.DIR_LOGS),
        "DIR_SIGNALS": str(settings.DIR_SIGNALS),
        "DIR_STATE": str(settings.DIR_STATE),
        "BYBIT_ENV": settings.BYBIT_ENV,
        "BYBIT_BASE_URL": settings.BYBIT_BASE_URL,
        "RELAY_URL": settings.RELAY_URL or "(none)",
        "TELEGRAM_CONFIGURED": bool(settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID),
        "SIG_ENABLED": settings.SIG_ENABLED,
        "SIG_DRY_RUN": settings.SIG_DRY_RUN,
        "SIG_SYMBOLS": settings.SIG_SYMBOLS,
        "SIG_TIMEFRAMES": settings.SIG_TIMEFRAMES,
        "SIG_BIAS_TF": settings.SIG_BIAS_TF,
        "SIG_HEARTBEAT_MIN": settings.SIG_HEARTBEAT_MIN,
        "TPSL_ENABLED": settings.TPSL_ENABLED,
        "TPSL_TARGET_RUNGS": settings.TPSL_TARGET_RUNGS,
        "HTTP_TIMEOUT_S": settings.HTTP_TIMEOUT_S,
        "PROXY_URL_SET": bool(settings.PROXY_URL),
    }
    pprint(printable)
