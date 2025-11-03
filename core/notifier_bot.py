# core/notifier_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notifier Bot (core)

Goals:
- One import for all bots to send consistent console + Telegram notifications.
- Safe by default: never crash callers, chunk long messages, respect rate limits.
- Flexible: multiple chat targets, threads, parse modes, quiet/silent operation.

Env vars (all optional unless noted):
- TELEGRAM_BOT_TOKEN           : Bot token (required to send to Telegram)
- TELEGRAM_CHAT_ID             : Single chat ID, OR
- TELEGRAM_CHAT_IDS            : Comma-separated chat IDs (overrides TELEGRAM_CHAT_ID)
- TELEGRAM_THREAD_ID           : Default thread/topic ID for forums (integer)
- TELEGRAM_SILENT              : "1" to disable Telegram sends (console only)
- TELEGRAM_DISABLE_PREVIEW     : "1" to disable web page previews (default on)
- TELEGRAM_DEFAULT_PARSE_MODE  : "Markdown", "HTML", or "" for plain (default "")
- TELEGRAM_NOTIFY              : "0" to disable notification (silent messages)
- TELEGRAM_MAX_RETRIES         : e.g., "3" (default 3)
- TELEGRAM_BACKOFF_BASE_MS     : e.g., "400" (default 400ms)
- TELEGRAM_RATE_LIMIT_TPS      : max sends per second per process (default 1.5)
- TZ                           : IANA tz for console stamps (e.g., "Atlantic/Canary"); defaults to UTC.
"""

from __future__ import annotations
import os
import time
import json
import math
import threading
import datetime
from typing import Any, Iterable, Optional, List

import requests

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
_TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
_TELEGRAM_CHAT_IDS = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
if not _TELEGRAM_CHAT_IDS and _TELEGRAM_CHAT_ID:
    _TELEGRAM_CHAT_IDS = [_TELEGRAM_CHAT_ID]

_TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID", "").strip()
try:
    _TELEGRAM_THREAD_ID = int(_TELEGRAM_THREAD_ID) if _TELEGRAM_THREAD_ID else None
except Exception:
    _TELEGRAM_THREAD_ID = None

_TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "0") == "1"
_DISABLE_PREVIEW = os.getenv("TELEGRAM_DISABLE_PREVIEW", "1") != "0"
_DEFAULT_PARSE_MODE = os.getenv("TELEGRAM_DEFAULT_PARSE_MODE", "").strip()  # "", "Markdown", "HTML"
_DEFAULT_NOTIFY = os.getenv("TELEGRAM_NOTIFY", "1") != "0"
_MAX_RETRIES = max(0, int(os.getenv("TELEGRAM_MAX_RETRIES", "3")))
_BACKOFF_BASE_MS = max(50, int(os.getenv("TELEGRAM_BACKOFF_BASE_MS", "400")))  # ms
_RATE_LIMIT_TPS = float(os.getenv("TELEGRAM_RATE_LIMIT_TPS", "1.5"))

_TZ = os.getenv("TZ", "UTC")

# ------------------------------------------------------------------------------
# Tiny rate limiter (token bucket)
# ------------------------------------------------------------------------------
_bucket_lock = threading.Lock()
_bucket_tokens = _RATE_LIMIT_TPS
_bucket_last = time.monotonic()

def _rate_limit_consume(cost: float = 1.0):
    global _bucket_tokens, _bucket_last
    with _bucket_lock:
        now = time.monotonic()
        elapsed = now - _bucket_last
        _bucket_last = now
        _bucket_tokens = min(_RATE_LIMIT_TPS, _bucket_tokens + elapsed * _RATE_LIMIT_TPS)
        if _bucket_tokens >= cost:
            _bucket_tokens -= cost
            return 0.0
        wait = (cost - _bucket_tokens) / _RATE_LIMIT_TPS
    # Sleep outside lock
    if wait > 0:
        time.sleep(wait)
    return wait

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def _now_iso() -> str:
    try:
        # Prefer TZ if system has it, fall back to UTC
        tzinfo = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo if _TZ == "" else datetime.timezone.utc
        if _TZ and _TZ.upper() != "UTC":
            # zoneinfo is available in 3.9+; if missing, UTC is fine
            try:
                from zoneinfo import ZoneInfo
                tzinfo = ZoneInfo(_TZ)
            except Exception:
                tzinfo = datetime.timezone.utc
        return datetime.datetime.now(tz=tzinfo).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def _console_print(prefix: str, msg: str):
    flat = " ".join((msg or "").split())
    print(f"[{_now_iso()}] {prefix} {flat}")

def _split_message(msg: str, limit: int = 4096) -> List[str]:
    if msg is None:
        return [""]
    if len(msg) <= limit:
        return [msg]
    parts, cur = [], []
    length = 0
    for line in msg.splitlines(keepends=True):
        l = len(line)
        if length + l > limit and cur:
            parts.append("".join(cur))
            cur, length = [], 0
        if l > limit:
            # brutal split for very long single line
            for i in range(0, l, limit):
                chunk = line[i:i+limit]
                if length + len(chunk) > limit and cur:
                    parts.append("".join(cur))
                    cur, length = [], 0
                cur.append(chunk)
                length += len(chunk)
        else:
            cur.append(line)
            length += l
    if cur:
        parts.append("".join(cur))
    # add index headers if multiple parts
    if len(parts) > 1:
        total = len(parts)
        parts = [f"{p.strip()}\n({i}/{total})" for i, p in enumerate(parts, 1)]
    return parts

def _telegram_enabled() -> bool:
    return bool(_TELEGRAM_BOT_TOKEN and _TELEGRAM_CHAT_IDS) and not _TELEGRAM_SILENT

def _post_telegram(chat_id: str, text: str, *,
                   parse_mode: Optional[str],
                   disable_preview: bool,
                   notify: bool,
                   thread_id: Optional[int],
                   reply_to: Optional[int]) -> Optional[requests.Response]:
    url = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_preview,
        "disable_notification": not notify,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if thread_id:
        payload["message_thread_id"] = thread_id
    if reply_to:
        payload["reply_to_message_id"] = reply_to

    # naive retry with exponential backoff
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.ok:
                return resp
            # Retry only on transient-ish statuses
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt)
                time.sleep(min(8.0, delay))
                continue
            # Hard fail on other status codes
            _console_print("notifier/telegram/error:", f"HTTP {resp.status_code} {resp.text[:200]}")
            return resp
        except Exception as e:
            last_exc = e
            delay = (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt)
            time.sleep(min(8.0, delay))
            continue
    if last_exc:
        _console_print("notifier/telegram/error:", f"{last_exc}")
    return None

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def tg_send(msg: str,
            *,
            parse_mode: Optional[str] = None,
            disable_preview: Optional[bool] = None,
            notify: Optional[bool] = None,
            thread_id: Optional[int] = None,
            reply_to: Optional[int] = None,
            priority: str = "info") -> None:
    """
    Send a message to console and Telegram (if configured).
    Safe to call from anywhere; never raises upstream.

    parse_mode: "Markdown" | "HTML" | None
    disable_preview: True to suppress URL previews (default from env)
    notify: False for silent push (default from env)
    thread_id: Telegram forum topic id
    reply_to: message id to reply to
    priority: "info"|"warn"|"error"|"success"
    """
    try:
        prefix = {
            "error": "❌",
            "warn": "⚠️",
            "success": "✅",
            "info": "ℹ️",
        }.get(priority, "ℹ️")
        _console_print(f"notifier/{priority}:", msg)

        if not _telegram_enabled():
            return

        use_parse = parse_mode if parse_mode is not None else (_DEFAULT_PARSE_MODE or None)
        use_preview = _DISABLE_PREVIEW if disable_preview is None else disable_preview
        use_notify = _DEFAULT_NOTIFY if notify is None else notify
        use_thread = thread_id if thread_id is not None else _TELEGRAM_THREAD_ID

        parts = _split_message(msg)
        for chat_id in _TELEGRAM_CHAT_IDS:
            for part in parts:
                _post_telegram(
                    chat_id=str(chat_id),
                    text=part,
                    parse_mode=use_parse,
                    disable_preview=use_preview,
                    notify=use_notify,
                    thread_id=use_thread,
                    reply_to=reply_to,
                )
    except Exception as e:
        _console_print("notifier/soft-fail:", f"{e}")

def tg_send_json(obj: Any,
                 title: Optional[str] = None,
                 *,
                 indent: int = 2,
                 priority: str = "info",
                 **kwargs) -> None:
    """
    Pretty-print JSON to console and Telegram. Applies chunking automatically.
    """
    try:
        body = json.dumps(obj, indent=indent, ensure_ascii=False)
    except Exception:
        body = str(obj)
    msg = f"{title}\n{body}" if title else body
    tg_send(msg, priority=priority, parse_mode=None, **kwargs)

def tg_send_code(text: str,
                 language: str = "",
                 *,
                 priority: str = "info",
                 **kwargs) -> None:
    """
    Sends a code block using Markdown triple backticks. If your message includes
    backticks, we try to escape them to avoid parse issues.
    """
    # Escape triple backticks inside the text
    safe = text.replace("```", "`​``")  # sneaky zero-width joiner to break fence
    msg = f"```{language}\n{safe}\n```"
    # If parse mode not set, default to Markdown for this helper
    kwargs.setdefault("parse_mode", "Markdown")
    tg_send(msg, priority=priority, **kwargs)

def tg_healthcheck(name: str = "notifier") -> None:
    """
    Tiny heartbeat; useful for cron or long-running bots.
    """
    tg_send(f"✅ {name} alive @ { _now_iso() }", priority="success")

# Backward compatibility alias
send = tg_send
