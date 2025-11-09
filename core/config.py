#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/config.py — Central Settings Loader for Base44

One source of truth for:
- Environment variables (.env + process env)
- Directory paths (logs, signals, state, registry)
- Core toggles and defaults (poll rates, timezones, logging)
- Network + exchange endpoints (Bybit base, timeouts)
- Optional integrations (Relay, Telegram)

Import from bots/tools like:
    from core.config import settings, ROOT, DIR_LOGS, DIR_SIGNALS

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


# -------------------------------
# Helpers
# -------------------------------

def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")

def _to_int(value: Optional[str], default: int) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default

def _to_float(value: Optional[str], default: float) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default

def _coalesce(*vals: Optional[str], default: str = "") -> str:
    for v in vals:
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

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


# -------------------------------
# Settings dataclass
# -------------------------------

@dataclass(frozen=True)
class Settings:
    # Root and dirs
    ROOT: Path
    DIR_LOGS: Path
    DIR_SIGNALS: Path
    DIR_STATE: Path
    DIR_CONFIG: Path
    DIR_REGISTRY: Path

    # Time / logging
    TZ: str
    LOG_LEVEL: str
    LOG_JSON: bool

    # Network
    HTTP_TIMEOUT_S: int
    PROXY_URL: Optional[str]

    # Bybit
    BYBIT_API_KEY: str
    BYBIT_API_SECRET: str
    BYBIT_ENV: str                 # mainnet | testnet
    BYBIT_BASE_URL: str

    # Relay (optional)
    RELAY_URL: Optional[str]
    RELAY_TOKEN: Optional[str]

    # Telegram (optional)
    TELEGRAM_BOT_TOKEN: Optional[str]
    TELEGRAM_CHAT_ID: Optional[str]

    # Signal Engine knobs (used by signal_engine.py)
    SIG_ENABLED: bool
    SIG_DRY_RUN: bool
    SIG_SYMBOLS: str
    SIG_TIMEFRAMES: str
    SIG_BIAS_TF: int
    SIG_HEARTBEAT_MIN: int

    SIG_POLL_SEC: int
    SIG_ADX_LEN: int
    SIG_ATR_LEN: int
    SIG_VOL_Z_WIN: int
    SIG_MIN_ADX: float                 # note: we normalize from SIG_MIN_ADX_BIAS if present
    SIG_MIN_ATR_PCT: float
    SIG_NOTIFY_COOLDOWN_SEC: int

    SIG_TAG: str
    SIG_MAKER_ONLY: bool
    SIG_SPREAD_MAX_BPS: float

    SIG_STOP_DIST_MODE: str            # auto|atr_mult|pct
    SIG_STOP_ATR_MULT: float
    SIG_STOP_PCT: float

    # TP/SL Manager defaults (tp_sl_manager.py uses getattr with fallbacks; included for completeness)
    TP_ADOPT_EXISTING: bool
    TP_CANCEL_NON_B44: bool
    TP_DRY_RUN: bool
    TP_STARTUP_GRACE_SEC: int
    TP_MANAGED_TAG: str
    TP_PERIODIC_SWEEP_SEC: int
    TP_RUNGS: int
    TP_EQUAL_R_START: float
    TP_EQUAL_R_STEP: float
    TP_SL_ATR_MULT_FALLBACK: float
    TP_SL_ATR_BUFFER: float
    TP_SL_TF: str
    TP_SL_LOOKBACK: int
    TP_SL_SWING_WIN: int
    TP_POST_ONLY: bool
    TP_SPREAD_OFFSET_RATIO: float
    TP_MAX_MAKER_OFFSET_TICKS: int
    TP_FALLBACK_OFFSET_TICKS: int
    TP_SYMBOL_WHITELIST: str


# -------------------------------
# Load .env and build settings
# -------------------------------

def _load_env(root: Path) -> None:
    if load_dotenv is None:
        return
    env_file = root / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=str(env_file), override=False)
    else:
        load_dotenv(override=False)

def _detect_root() -> Path:
    # Prefer explicit
    if "BASE44_ROOT" in os.environ:
        r = Path(os.environ["BASE44_ROOT"]).expanduser()
        return r if r.exists() else Path.cwd()
    # Heuristic
    cwd = Path.cwd()
    hallmarks = {"core", "bots", ".state"}
    for p in (cwd, cwd.parent, cwd.parent.parent):
        try:
            if any((p / h).exists() for h in hallmarks):
                return p
        except Exception:
            continue
    return cwd

def _build_settings() -> Settings:
    root = _detect_root()
    _load_env(root)

    # Dirs
    dir_logs = (root / "logs").resolve()
    dir_signals = Path(_coalesce(os.getenv("SIGNAL_DIR"), default=str(root / "signals"))).resolve()
    dir_state = (root / ".state").resolve()
    dir_config = (root / "config").resolve()
    dir_registry = (root / "registry").resolve()
    _ensure_dirs(dir_logs, dir_signals, dir_state, dir_config, dir_registry)

    # Time / logging
    tz = _coalesce(os.getenv("TZ"), os.getenv("TIMEZONE"), default="America/Phoenix")
    log_level = _coalesce(os.getenv("LOG_LEVEL"), default="INFO").upper()
    log_json = _to_bool(os.getenv("LOG_JSON"), default=False)

    # Network
    http_timeout = _to_int(os.getenv("HTTP_TIMEOUT_S"), default=12)
    proxy_url = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("PROXY_URL")

    # Bybit
    bybit_key = os.getenv("BYBIT_API_KEY")
    bybit_sec = os.getenv("BYBIT_API_SECRET")
    bybit_env = _coalesce(os.getenv("BYBIT_ENV"), default="mainnet").lower()
    bybit_url = _coalesce(os.getenv("BYBIT_BASE_URL"), default=_derive_bybit_url(bybit_env))
    _validate_required("BYBIT_API_KEY", bybit_key)
    _validate_required("BYBIT_API_SECRET", bybit_sec)

    # Relay
    relay_url = os.getenv("RELAY_URL") or os.getenv("DASHBOARD_RELAY_BASE")
    relay_token = os.getenv("RELAY_TOKEN") or os.getenv("RELAY_SECRET")

    # Telegram
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")

    # Signal Engine knobs
    sig_enabled = _to_bool(os.getenv("SIG_ENABLED"), default=True)
    sig_dry = _to_bool(os.getenv("SIG_DRY_RUN"), default=True)
    sig_symbols = _coalesce(os.getenv("SIG_SYMBOLS"), default="BTCUSDT,ETHUSDT")
    sig_tfs = _coalesce(os.getenv("SIG_TIMEFRAMES"), default="5,15")
    sig_bias_tf = _to_int(os.getenv("SIG_BIAS_TF"), default=60)
    sig_hb = _to_int(os.getenv("SIG_HEARTBEAT_MIN"), default=10)

    sig_poll = _to_int(os.getenv("SIG_POLL_SEC"), default=30)
    sig_adx_len = _to_int(os.getenv("SIG_ADX_LEN"), default=14)
    sig_atr_len = _to_int(os.getenv("SIG_ATR_LEN"), default=14)
    sig_vol_z_win = _to_int(os.getenv("SIG_VOL_Z_WIN"), default=60)

    # Back-compat: accept SIG_MIN_ADX_BIAS if present, else SIG_MIN_ADX
    sig_min_adx = _to_float(os.getenv("SIG_MIN_ADX") or os.getenv("SIG_MIN_ADX_BIAS"), default=18.0)
    sig_min_atr_pct = _to_float(os.getenv("SIG_MIN_ATR_PCT"), default=0.25)
    sig_cd_sec = _to_int(os.getenv("SIG_NOTIFY_COOLDOWN_SEC"), default=300)

    sig_tag = _coalesce(os.getenv("SIG_TAG"), default="B44")
    sig_maker_only = _to_bool(os.getenv("SIG_MAKER_ONLY"), default=True)
    sig_spread_max_bps = _to_float(os.getenv("SIG_SPREAD_MAX_BPS"), default=8.0)

    sig_stop_mode = _coalesce(os.getenv("SIG_STOP_DIST_MODE"), default="auto").lower()
    sig_stop_atr_mult = _to_float(os.getenv("SIG_STOP_ATR_MULT"), default=3.0)
    sig_stop_pct = _to_float(os.getenv("SIG_STOP_PCT"), default=1.2)

    # TP/SL defaults (kept here so tools can read centralized config if desired)
    tp_adopt = _to_bool(os.getenv("TP_ADOPT_EXISTING"), default=True)
    tp_cancel_non_b44 = _to_bool(os.getenv("TP_CANCEL_NON_B44"), default=False)
    tp_dry = _to_bool(os.getenv("TP_DRY_RUN"), default=True)
    tp_grace = _to_int(os.getenv("TP_STARTUP_GRACE_SEC"), default=20)
    tp_tag = _coalesce(os.getenv("TP_MANAGED_TAG"), default="B44")
    tp_sweep = _to_int(os.getenv("TP_PERIODIC_SWEEP_SEC"), default=12)

    tp_rungs = _to_int(os.getenv("TP_RUNGS"), default=5)
    tp_r_start = _to_float(os.getenv("TP_EQUAL_R_START"), default=0.5)
    tp_r_step = _to_float(os.getenv("TP_EQUAL_R_STEP"), default=0.5)

    tp_sl_atr_fb = _to_float(os.getenv("TP_SL_ATR_MULT_FALLBACK"), default=0.45)
    tp_sl_atr_buf = _to_float(os.getenv("TP_SL_ATR_BUFFER"), default=0.08)
    tp_sl_tf = _coalesce(os.getenv("TP_SL_TF"), default="5")
    tp_sl_lookback = _to_int(os.getenv("TP_SL_LOOKBACK"), default=120)
    tp_sl_swing_win = _to_int(os.getenv("TP_SL_SWING_WIN"), default=20)

    tp_post_only = _to_bool(os.getenv("TP_POST_ONLY"), default=True)
    tp_spread_ratio = _to_float(os.getenv("TP_SPREAD_OFFSET_RATIO"), default=0.35)
    tp_max_ticks = _to_int(os.getenv("TP_MAX_MAKER_OFFSET_TICKS"), default=5)
    tp_fb_ticks = _to_int(os.getenv("TP_FALLBACK_OFFSET_TICKS"), default=2)

    tp_sym_whitelist = _coalesce(os.getenv("TP_SYMBOL_WHITELIST"), default="")

    return Settings(
        ROOT=root,
        DIR_LOGS=dir_logs,
        DIR_SIGNALS=dir_signals,
        DIR_STATE=dir_state,
        DIR_CONFIG=dir_config,
        DIR_REGISTRY=dir_registry,

        TZ=tz,
        LOG_LEVEL=log_level,
        LOG_JSON=log_json,

        HTTP_TIMEOUT_S=http_timeout,
        PROXY_URL=proxy_url,

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

        SIG_POLL_SEC=sig_poll,
        SIG_ADX_LEN=sig_adx_len,
        SIG_ATR_LEN=sig_atr_len,
        SIG_VOL_Z_WIN=sig_vol_z_win,
        SIG_MIN_ADX=sig_min_adx,
        SIG_MIN_ATR_PCT=sig_min_atr_pct,
        SIG_NOTIFY_COOLDOWN_SEC=sig_cd_sec,

        SIG_TAG=sig_tag,
        SIG_MAKER_ONLY=sig_maker_only,
        SIG_SPREAD_MAX_BPS=sig_spread_max_bps,

        SIG_STOP_DIST_MODE=sig_stop_mode,
        SIG_STOP_ATR_MULT=sig_stop_atr_mult,
        SIG_STOP_PCT=sig_stop_pct,

        TP_ADOPT_EXISTING=tp_adopt,
        TP_CANCEL_NON_B44=tp_cancel_non_b44,
        TP_DRY_RUN=tp_dry,
        TP_STARTUP_GRACE_SEC=tp_grace,
        TP_MANAGED_TAG=tp_tag,
        TP_PERIODIC_SWEEP_SEC=tp_sweep,
        TP_RUNGS=tp_rungs,
        TP_EQUAL_R_START=tp_r_start,
        TP_EQUAL_R_STEP=tp_r_step,
        TP_SL_ATR_MULT_FALLBACK=tp_sl_atr_fb,
        TP_SL_ATR_BUFFER=tp_sl_atr_buf,
        TP_SL_TF=tp_sl_tf,
        TP_SL_LOOKBACK=tp_sl_lookback,
        TP_SL_SWING_WIN=tp_sl_swing_win,
        TP_POST_ONLY=tp_post_only,
        TP_SPREAD_OFFSET_RATIO=tp_spread_ratio,
        TP_MAX_MAKER_OFFSET_TICKS=tp_max_ticks,
        TP_FALLBACK_OFFSET_TICKS=tp_fb_ticks,
        TP_SYMBOL_WHITELIST=tp_sym_whitelist,
    )


# Singleton instance exposed to the rest of the codebase
settings: Settings = _build_settings()

# Convenience exports for common imports
ROOT = settings.ROOT
DIR_LOGS = settings.DIR_LOGS
DIR_SIGNALS = settings.DIR_SIGNALS
DIR_STATE = settings.DIR_STATE
DIR_REGISTRY = settings.DIR_REGISTRY

# Pretty print a quick diagnostic when run directly
if __name__ == "__main__":
    from pprint import pprint
    print("Base44 Settings Snapshot")
    print("------------------------")
    printable = {
        "ROOT": str(settings.ROOT),
        "TZ": settings.TZ,
        "LOG_LEVEL": settings.LOG_LEVEL,
        "LOG_JSON": settings.LOG_JSON,
        "DIR_LOGS": str(settings.DIR_LOGS),
        "DIR_SIGNALS": str(settings.DIR_SIGNALS),
        "DIR_STATE": str(settings.DIR_STATE),
        "DIR_REGISTRY": str(settings.DIR_REGISTRY),
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
        "HTTP_TIMEOUT_S": settings.HTTP_TIMEOUT_S,
    }
    pprint(printable)
