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

# dotenv: load root .env reliably, regardless of CWD
try:
    from dotenv import load_dotenv, find_dotenv
    _ENV_PATH = find_dotenv(filename=".env", usecwd=True)
    if _ENV_PATH:
        load_dotenv(_ENV_PATH)
        # basic print without using our timestamp helper yet
        print(f"[notifier] Loaded .env: {_ENV_PATH}")
    else:
        load_dotenv()  # fallback: environment/system
        print("[notifier/warn] No project .env found via find_dotenv; relying on process env.")
except Exception:
    # If python-dotenv isn't installed, we just rely on OS env
    print("[notifier/warn] python-dotenv not available; relying on process env.")
    _ENV_PATH = None

# ------------------------------------------------------------------------------
# Config (module-level; can be updated via set_chat_ids or by re-import)
# ------------------------------------------------------------------------------
_TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# Chat IDs from TELEGRAM_CHAT_IDS or fallback TELEGRAM_CHAT_ID
_TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
_TELEGRAM_CHAT_IDS = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
if not _TELEGRAM_CHAT_IDS and _TELEGRAM_CHAT_ID:
    _TELEGRAM_CHAT_IDS = [_TELEGRAM_CHAT_ID]

# Optional thread/topic id
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
_RATE_LIMIT_TPS = max(0.1, float(os.getenv("TELEGRAM_RATE_LIMIT_TPS", "1.5")))

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
    if wait > 0:
        time.sleep(wait)
    return wait

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def _tzinfo():
    """Return tzinfo based on TZ env, default UTC, with zoneinfo if available."""
    if not _TZ or _TZ.upper() == "UTC":
        return datetime.timezone.utc
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(_TZ)
    except Exception:
        return datetime.timezone.utc

def _now_iso() -> str:
    try:
        return datetime.datetime.now(tz=_tzinfo()).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        # Still timezone-aware fallback
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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

    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.ok:
                return resp
            # Transient server/limit errors → retry with capped backoff
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(8.0, (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt))
                _console_print("notifier/telegram/retry:", f"HTTP {resp.status_code}; retrying in {delay:.2f}s")
                time.sleep(delay)
                continue
            # Hard failures print body
            _console_print("notifier/telegram/error:", f"HTTP {resp.status_code} {resp.text[:300]}")
            return resp
        except Exception as e:
            last_exc = e
            delay = min(8.0, (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt))
            _console_print("notifier/telegram/except:", f"{e}; retrying in {delay:.2f}s")
            time.sleep(delay)
            continue
    if last_exc:
        _console_print("notifier/telegram/error:", f"{last_exc}")
    return None

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def set_chat_ids(ids: Iterable[str | int]) -> None:
    """
    Override destination chat IDs at runtime.
    """
    global _TELEGRAM_CHAT_IDS
    _TELEGRAM_CHAT_IDS = [str(x).strip() for x in ids if str(x).strip()]

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
        _console_print(f"notifier/{priority}:", f"{prefix} {msg}")

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
    safe = text.replace("```", "`\u200d``")  # zero-width joiner to break fence
    msg = f"```{language}\n{safe}\n```"
    kwargs.setdefault("parse_mode", "Markdown")
    tg_send(msg, priority=priority, **kwargs)

def tg_send_exception(e: Exception,
                      title: str = "Exception",
                      *,
                      priority: str = "error",
                      include_type: bool = True) -> None:
    """
    Convenience: format and send an exception.
    """
    name = type(e).__name__ if include_type else ""
    msg = f"{title}: {name}: {e}"
    tg_send(msg, priority=priority, parse_mode=None)

def tg_healthcheck(name: str = "notifier") -> None:
    """
    Tiny heartbeat; useful for cron or long-running bots.
    """
    tg_send(f"✅ {name} alive @ { _now_iso() }", priority="success")

def tg_validate() -> bool:
    """
    Send a one-time test message to all configured chats, printing helpful hints
    for common failure modes. Returns True if at least one chat succeeded.
    """
    if not _TELEGRAM_BOT_TOKEN:
        _console_print("notifier/error:", "Missing TELEGRAM_BOT_TOKEN in environment.")
        return False
    if not _TELEGRAM_CHAT_IDS:
        _console_print("notifier/error:", "No TELEGRAM_CHAT_ID(S) configured.")
        return False

    ok_any = False
    test_msg = f"🟢 Notifier validate @ {_now_iso()}"
    parts = _split_message(test_msg)
    for chat_id in _TELEGRAM_CHAT_IDS:
        resp = _post_telegram(
            chat_id=str(chat_id),
            text=parts[0],
            parse_mode=None,
            disable_preview=True,
            notify=True,
            thread_id=_TELEGRAM_THREAD_ID,
            reply_to=None,
        )
        if resp and resp.ok:
            ok_any = True
        else:
            # Print extra hints based on typical API replies
            hint = ""
            try:
                if resp is not None:
                    data = resp.json()
                    desc = str(data.get("description", "")).lower()
                    code = data.get("error_code")
                    if "unauthorized" in desc or code == 401:
                        hint = "Bot token invalid. Create a new token with BotFather."
                    elif "chat not found" in desc:
                        hint = "CHAT_ID invalid or you never pressed Start with this bot."
                    elif "forbidden" in desc:
                        hint = "Bot is blocked or not a member of the chat/group."
            except Exception:
                pass
            _console_print("notifier/help:", hint or "See Telegram API response above for details.")
    return ok_any

# Backward compatibility alias
send = tg_send
