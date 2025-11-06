#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Unified Logger

Features:
- Single setup for console + rotating file output under logs/.
- Timezone-aware timestamps using settings.TZ (default Europe/London).
- Structured fields support via LoggerAdapter (bind context once).
- Optional JSON output via LOG_JSON=true.
- Log level via LOG_LEVEL (DEBUG, INFO, WARNING, ERROR).
- Quiet noisy deps (urllib3, httpx) by default.
- Per-logger file split optional via LOG_FILE_PER_LOGGER=true.

Usage:
    from core.logger import get_logger, bind_context

    log = get_logger("bots.tp_sl_manager")
    log.info("starting bot", extra={"rungs": 50})

    # or bind extra fields once:
    log = bind_context(log, account="MAIN", sub_uid="302355261")
    log.info("heartbeat")

"""

from __future__ import annotations
import json
import logging
import os
import sys
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from core.config import settings

# ---------------------------
# Utilities
# ---------------------------

def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return str(v).strip() if v is not None else default

def _env_level(default: str = "INFO") -> int:
    txt = _env_str("LOG_LEVEL", default).upper()
    return {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }.get(txt, logging.INFO)

# ---------------------------
# TZ-aware time formatter
# ---------------------------

try:
    import pytz  # type: ignore
except Exception:
    pytz = None

import time
import datetime as _dt

class TZFormatter(logging.Formatter):
    """
    Logging formatter that forces timestamps to settings.TZ (or UTC fallback).
    """

    def __init__(self, fmt: str, datefmt: Optional[str] = None, tz_name: Optional[str] = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.tz_name = tz_name or settings.TZ or "Europe/London"

        # Resolve timezone
        if pytz:
            try:
                self.tz = pytz.timezone(self.tz_name)
            except Exception:
                self.tz = pytz.timezone("UTC")
        else:
            self.tz = None  # fallback: naive localtime

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = _dt.datetime.fromtimestamp(record.created, _dt.timezone.utc)
        if self.tz:
            # convert utc to target tz using pytz
            dt = _dt.datetime.utcfromtimestamp(record.created).replace(tzinfo=_dt.timezone.utc)
            dt = _dt.datetime.fromtimestamp(dt.timestamp(), tz=_dt.timezone.utc)  # stable
            dt = _dt.datetime.fromtimestamp(record.created, _dt.timezone.utc)
            # pytz conversion with localize/astimezone
            dt = _dt.datetime.fromtimestamp(record.created, _dt.timezone.utc).astimezone(_dt.timezone.utc)
            # If pytz exists, convert via pytz for named tz
            try:
                if pytz:
                    dt = pytz.utc.localize(_dt.datetime.utcfromtimestamp(record.created)).astimezone(self.tz)  # type: ignore
            except Exception:
                pass
        else:
            # fallback: localtime
            dt = _dt.datetime.fromtimestamp(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        # ISO-like default with offset
        return dt.strftime("%Y-%m-%d %H:%M:%S%z")

# ---------------------------
# JSON formatter (optional)
# ---------------------------

class JSONFormatter(logging.Formatter):
    """
    Structured JSON logs. Keeps message, level, name, ts, and merges extras.
    """

    def __init__(self, tz_name: Optional[str] = None):
        super().__init__()
        self.tz_name = tz_name or settings.TZ or "Europe/London"
        if pytz:
            try:
                self.tz = pytz.timezone(self.tz_name)
            except Exception:
                self.tz = pytz.timezone("UTC")
        else:
            self.tz = None

    def formatTime(self, record: logging.LogRecord) -> str:
        dt = _dt.datetime.fromtimestamp(record.created, _dt.timezone.utc)
        if self.tz and pytz:
            dt = pytz.utc.localize(_dt.datetime.utcfromtimestamp(record.created)).astimezone(self.tz)  # type: ignore
        return dt.isoformat()

    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Merge 'extra' fields: anything not in default LogRecord dict
        default_keys = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys())
        for k, v in record.__dict__.items():
            if k not in default_keys and k not in ("args", "message"):
                base[k] = v
        return json.dumps(base, ensure_ascii=False)

# ---------------------------
# Console colorizer (TTY only)
# ---------------------------

class ColorFormatter(TZFormatter):
    COLORS = {
        "DEBUG": "\033[36m",    # cyan
        "INFO": "\033[32m",     # green
        "WARNING": "\033[33m",  # yellow
        "ERROR": "\033[31m",    # red
        "CRITICAL": "\033[41m", # red background
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        if sys.stderr.isatty():
            color = self.COLORS.get(record.levelname, "")
            reset = self.RESET if color else ""
            return f"{color}{base}{reset}"
        return base

# ---------------------------
# Context binding
# ---------------------------

class ContextAdapter(logging.LoggerAdapter):
    """
    Attach a dict of context fields to every record emitted by this logger.
    """

    def __init__(self, logger: Logger, extra: Optional[Dict[str, Any]] = None):
        super().__init__(logger, extra or {})

    def process(self, msg, kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        # Shallow merge: bound context is base, call-site extra overlays it
        merged = dict(self.extra)
        merged.update(kwargs["extra"])
        kwargs["extra"] = merged
        return msg, kwargs

def bind_context(logger: Logger, **ctx: Any) -> ContextAdapter:
    """
    Return a LoggerAdapter with context bound.
    """
    if isinstance(logger, ContextAdapter):
        # merge onto existing
        logger.extra.update(ctx)
        return logger
    return ContextAdapter(logger, ctx)

# ---------------------------
# Setup internals
# ---------------------------

_INITIALIZED = False

def _build_console_handler(level: int, json_mode: bool) -> logging.Handler:
    ch = logging.StreamHandler(stream=sys.stderr)
    ch.setLevel(level)
    if json_mode:
        ch.setFormatter(JSONFormatter(tz_name=settings.TZ))
    else:
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        ch.setFormatter(ColorFormatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S", tz_name=settings.TZ))
    return ch

def _build_file_handler(level: int, logger_name: str, json_mode: bool) -> logging.Handler:
    logs_dir: Path = settings.DIR_LOGS
    logs_dir.mkdir(parents=True, exist_ok=True)

    file_per = _env_bool("LOG_FILE_PER_LOGGER", default=False)
    fname = "app.log"
    if file_per and logger_name:
        # sanitize name to a filesystem-friendly short path
        safe = logger_name.replace("/", ".").replace("\\", ".")
        fname = f"{safe}.log"

    path = logs_dir / fname

    fh = TimedRotatingFileHandler(
        filename=str(path),
        when="midnight",
        interval=1,
        backupCount=14,
        encoding="utf-8",
        delay=True,
        utc=True,  # rotate by UTC midnight; timestamps still shown in settings.TZ
    )
    fh.setLevel(level)
    if json_mode:
        fh.setFormatter(JSONFormatter(tz_name=settings.TZ))
    else:
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        fh.setFormatter(TZFormatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S", tz_name=settings.TZ))
    return fh

def setup_logging(level: Optional[int] = None) -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    lvl = level if level is not None else _env_level("INFO")
    json_mode = _env_bool("LOG_JSON", default=False)

    root = logging.getLogger()
    root.setLevel(lvl)

    # Remove pre-existing handlers (avoid duplicates on reloads)
    for h in list(root.handlers):
        root.removeHandler(h)

    # Console + file
    root.addHandler(_build_console_handler(lvl, json_mode))
    # Root file handler uses "app.log"
    root.addHandler(_build_file_handler(lvl, logger_name="", json_mode=json_mode))

    # Quiet noisy libraries
    for noisy in ("urllib3", "httpx", "websockets", "asyncio", "botocore"):
        logging.getLogger(noisy).setLevel(max(lvl, logging.WARNING))

    _INITIALIZED = True

def get_logger(name: str) -> Logger:
    """
    Get a logger with Base44 defaults. First call initializes logging.
    If LOG_FILE_PER_LOGGER=true, a per-logger file handler is added.
    """
    setup_logging()
    log = logging.getLogger(name)

    # Optionally add a per-logger file handler
    if _env_bool("LOG_FILE_PER_LOGGER", default=False):
        # Prevent multiple additions for the same logger across imports
        key = "__base44_per_file_attached__"
        if not getattr(log, key, False):
            per_file = _build_file_handler(logging.getLogger().level, logger_name=name, json_mode=_env_bool("LOG_JSON", False))
            log.addHandler(per_file)
            setattr(log, key, True)

    return log

# ---------------------------
# Self-test
# ---------------------------

if __name__ == "__main__":
    # Minimal self-check
    log = get_logger("core.logger.selftest")
    log = bind_context(log, test="yes", tz=settings.TZ)
    log.debug("debug line")
    log.info("hello from logger")
    log.warning("careful now", extra={"hint": "rotation@midnight"})
    log.error("something went wrong", extra={"code": 123})
