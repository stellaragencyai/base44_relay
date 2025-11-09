#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/logger.py — unified, structured logging with TZ-aware timestamps,
optional JSON output, rotating files, and bindable context.

Features
- Console handler:
    • JSON when LOG_JSON=true
    • Colored pretty text otherwise
- File handler:
    • logs/base44.log, RotatingFileHandler (5 x 5MB)
    • Always JSON for reliable parsing downstream
    • Optional per-logger file split (LOG_FILE_PER_LOGGER=true)
- TZ-aware timestamps using settings.TZ (ZoneInfo)
- Quiet common noisy deps (urllib3/httpx/etc.)
- get_logger(name) and bind_context(logger, **extra)

Env (via your .env or process):
  LOG_LEVEL=DEBUG|INFO|WARNING|ERROR|CRITICAL
  LOG_JSON=true|false
  LOG_FILE_PER_LOGGER=true|false
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# --------------------------------------------------------------------
# Settings (prefer core.config; fall back to env-only bootstrap)
# --------------------------------------------------------------------
try:
    from core.config import settings  # type: ignore
except Exception:
    @dataclass
    class _Bootstrap:
        TZ: str = os.getenv("TZ", "Europe/London")
        DIR_LOGS: Path = Path(os.getenv("BASE44_ROOT", ".")).resolve() / "logs"
        LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
        LOG_JSON: bool = os.getenv("LOG_JSON", "0").strip().lower() in {"1", "true", "yes", "on"}
    settings = _Bootstrap()  # type: ignore

# Ensure logs directory
try:
    Path(settings.DIR_LOGS).mkdir(parents=True, exist_ok=True)
except Exception:
    pass

# --------------------------------------------------------------------
# TZ helpers (zoneinfo over pytz)
# --------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

def _tz() -> timezone:
    if ZoneInfo:
        try:
            return ZoneInfo(getattr(settings, "TZ", "Europe/London") or "Europe/London")  # type: ignore
        except Exception:
            return timezone.utc
    return timezone.utc

_TZINFO = _tz()

# --------------------------------------------------------------------
# Formatters
# --------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=_TZINFO).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        # Include extras (JSON-serializable best-effort)
        skip = {
            "args","asctime","created","exc_info","exc_text","filename","funcName","levelname",
            "levelno","lineno","module","msecs","message","msg","name","pathname","process",
            "processName","relativeCreated","stack_info","thread","threadName"
        }
        for k, v in record.__dict__.items():
            if k in skip:
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except Exception:
                payload[k] = repr(v)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

class PrettyFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",    # cyan
        "INFO": "\033[32m",     # green
        "WARNING": "\033[33m",  # yellow
        "ERROR": "\033[31m",    # red
        "CRITICAL": "\033[35m", # magenta
    }
    RESET = "\033[0m"

    def __init__(self, tzinfo: timezone, with_color: bool = True):
        super().__init__("%(message)s")
        self.tzinfo = tzinfo
        self.with_color = with_color and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=self.tzinfo).strftime("%Y-%m-%d %H:%M:%S")
        lvl = record.levelname
        name = record.name
        base = f"{ts} | {lvl:8s} | {name} | {record.getMessage()}"

        # extras inline preview
        skip = {
            "args","asctime","created","exc_info","exc_text","filename","funcName","levelname",
            "levelno","lineno","module","msecs","message","msg","name","pathname","process",
            "processName","relativeCreated","stack_info","thread","threadName"
        }
        extras = []
        for k, v in record.__dict__.items():
            if k in skip:
                continue
            if k == "comp":
                base += f" | {k}={v}"
                continue
            extras.append(f"{k}={v}")
        if extras:
            base += " | " + " ".join(extras)

        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)

        if self.with_color:
            color = self.COLORS.get(lvl, "")
            reset = self.RESET if color else ""
            return f"{color}{base}{reset}"
        return base

# --------------------------------------------------------------------
# Handlers (configured once per-process)
# --------------------------------------------------------------------
_configured = False
_console_handler: Optional[logging.Handler] = None
_file_handler: Optional[logging.Handler] = None

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    return default if v is None else str(v).strip().lower() in {"1","true","yes","on","y"}

def _env_level(default: str = "INFO") -> int:
    txt = (os.getenv("LOG_LEVEL", default) or default).upper().strip()
    return getattr(logging, txt, logging.INFO)

def _build_console_handler(json_mode: bool) -> logging.Handler:
    h = logging.StreamHandler(sys.stderr)
    if json_mode:
        h.setFormatter(JsonFormatter())
    else:
        h.setFormatter(PrettyFormatter(_TZINFO, with_color=True))
    return h

def _build_file_handler(filename: Path) -> logging.Handler:
    h = logging.handlers.RotatingFileHandler(
        filename=str(filename),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    # File is always JSON for machine-readability
    h.setFormatter(JsonFormatter())
    return h

def _ensure_root_handlers():
    global _configured, _console_handler, _file_handler
    if _configured:
        return

    level = _env_level("INFO")
    json_mode = _env_bool("LOG_JSON", False)

    root = logging.getLogger()
    root.setLevel(level)

    # Remove any preexisting handlers (avoid dupes on reloads)
    for h in list(root.handlers):
        root.removeHandler(h)

    _console_handler = _build_console_handler(json_mode)
    _console_handler.setLevel(level)
    root.addHandler(_console_handler)

    main_log = Path(getattr(settings, "DIR_LOGS", Path("logs"))) / "base44.log"
    _file_handler = _build_file_handler(main_log)
    _file_handler.setLevel(level)
    root.addHandler(_file_handler)

    # Quiet noisy libs
    for noisy in ("urllib3", "httpx", "websockets", "asyncio", "botocore"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))

    _configured = True

# --------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger wired to Base44 handlers. If LOG_FILE_PER_LOGGER=true,
    a per-logger rotating file handler is attached once.
    """
    _ensure_root_handlers()
    logger = logging.getLogger(name)
    # Ensure level inheritance from root
    logger.setLevel(logging.getLogger().level)
    logger.propagate = False

    # Attach root handlers only once per logger
    have_console = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    have_rot = any(isinstance(h, logging.handlers.RotatingFileHandler) for h in logger.handlers)
    if not have_console and _console_handler:
        logger.addHandler(_console_handler)
    if not have_rot and _file_handler:
        logger.addHandler(_file_handler)

    # Optional per-logger file split
    if _env_bool("LOG_FILE_PER_LOGGER", False):
        key = "__base44_per_file_attached__"
        if not getattr(logger, key, False):
            per_path = Path(getattr(settings, "DIR_LOGS", Path("logs"))) / (name.replace("/", ".").replace("\\", ".") + ".log")
            per_h = _build_file_handler(per_path)
            per_h.setLevel(logging.getLogger().level)
            logger.addHandler(per_h)
            setattr(logger, key, True)

    return logger

class _BoundLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter injecting fixed context into every record."""
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {})
        merged = dict(self.extra)
        merged.update(extra or {})
        kwargs["extra"] = merged
        return msg, kwargs

def bind_context(logger: logging.Logger, **extra) -> logging.Logger:
    """
    Return a context-bound logger without mutating the base logger.
    Usage:
        log = get_logger("bots.tp_sl_manager")
        log = bind_context(log, comp="tpsl", account="MAIN")
        log.info("ladder synced", symbol="BTCUSDT")
    """
    if isinstance(logger, _BoundLoggerAdapter):
        # extend existing adapter
        logger.extra.update(extra)
        return logger
    return _BoundLoggerAdapter(logger, extra)  # type: ignore

# Convenience default logger
_default = get_logger("base44")

def debug(msg: str, **kw): _default.debug(msg, extra=kw)
def info(msg: str, **kw): _default.info(msg, extra=kw)
def warn(msg: str, **kw): _default.warning(msg, extra=kw)
def error(msg: str, **kw): _default.error(msg, extra=kw)
def exception(msg: str, **kw): _default.exception(msg, extra=kw)

# --------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------
if __name__ == "__main__":
    log = get_logger("core.logger.selftest")
    log.info("hello from logger", comp="selftest", foo=123)
    log = bind_context(log, comp="ctx", uid="MAIN")
    try:
        raise ValueError("boom")
    except Exception:
        log.exception("sample exception")
