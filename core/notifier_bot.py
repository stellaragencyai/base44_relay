# core/notifier_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notifier Bot (core) — quiet on import, layered env, retries, rate limit, media helpers.

What you get:
- Loads env via core/env_bootstrap (layered .env, repo-safe paths).
- Token format validation with masked previews (logs only on CLI).
- Multi-recipient via TELEGRAM_CHAT_IDS (overrides TELEGRAM_CHAT_ID).
- Optional thread routing (forum topics) via TELEGRAM_THREAD_ID.
- Rate limiting (token bucket), retries with backoff + jitter.
- Chunking for >4096 char messages.
- Media helpers: tg_send_photo(), tg_send_document().
- Console mirror for every message; Telegram send is optional and never raises.
- CLI: --validate, --updates, --ping [name], --echo "msg".

Env (in .env or process):
  TELEGRAM_BOT_TOKEN=123456:ABC...
  TELEGRAM_CHAT_ID=7776809236
  TELEGRAM_CHAT_IDS=777,888,-100123...
  TELEGRAM_THREAD_ID=123
  TELEGRAM_SILENT=0|1
  TELEGRAM_DISABLE_PREVIEW=1|0
  TELEGRAM_DEFAULT_PARSE_MODE=Markdown | HTML
  TELEGRAM_NOTIFY=1|0
  TELEGRAM_MAX_RETRIES=3
  TELEGRAM_BACKOFF_BASE_MS=400
  TELEGRAM_RATE_LIMIT_TPS=1.5
  TELEGRAM_DEBUG=0|1       # extra console notes
  TZ=America/Phoenix
"""

from __future__ import annotations
import os
import re
import io
import time
import json
import random
import threading
import datetime
from typing import Any, Iterable, Optional, List, Union

# Layered env + safe paths
try:
    from core.env_bootstrap import LOGS_DIR  # ensures layered .env already loaded
except Exception:
    LOGS_DIR = None

import requests

# --------------------------------------------------------------------------------------
# Config helpers
# --------------------------------------------------------------------------------------
def _is_placeholder(s: str) -> bool:
    try:
        val = (s or "").strip().lower()
    except Exception:
        return True
    return val in {"", "<chat_id>", "<chat_ids>", "<token>", "changeme", "todo"}

def _getenv(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default).strip()

_TELEGRAM_BOT_TOKEN = _getenv("TELEGRAM_BOT_TOKEN")
_TELEGRAM_CHAT_ID = _getenv("TELEGRAM_CHAT_ID")
_TELEGRAM_CHAT_IDS = [c.strip() for c in (_getenv("TELEGRAM_CHAT_IDS")).split(",") if c.strip()]
# Sanitize placeholders
_TELEGRAM_CHAT_IDS = [c for c in _TELEGRAM_CHAT_IDS if not _is_placeholder(c)]
if not _TELEGRAM_CHAT_IDS and not _is_placeholder(_TELEGRAM_CHAT_ID):
    _TELEGRAM_CHAT_IDS = [_TELEGRAM_CHAT_ID]

# Optional thread routing
try:
    _TELEGRAM_THREAD_ID: Optional[int] = int(_getenv("TELEGRAM_THREAD_ID")) if _getenv("TELEGRAM_THREAD_ID") else None
except Exception:
    _TELEGRAM_THREAD_ID = None

_TELEGRAM_SILENT = (_getenv("TELEGRAM_SILENT", "0") or "0") == "1"
_DISABLE_PREVIEW = (_getenv("TELEGRAM_DISABLE_PREVIEW", "1") or "1") != "0"
_DEFAULT_PARSE_MODE = _getenv("TELEGRAM_DEFAULT_PARSE_MODE")
_DEFAULT_NOTIFY = (_getenv("TELEGRAM_NOTIFY", "1") or "1") != "0"
_MAX_RETRIES = max(0, int(_getenv("TELEGRAM_MAX_RETRIES", "3") or "3"))
_BACKOFF_BASE_MS = max(50, int(_getenv("TELEGRAM_BACKOFF_BASE_MS", "400") or "400"))
_RATE_LIMIT_TPS = max(0.1, float(_getenv("TELEGRAM_RATE_LIMIT_TPS", "1.5") or "1.5"))
_TELEGRAM_DEBUG = (_getenv("TELEGRAM_DEBUG", "0") or "0") == "1"
_TZ = (_getenv("TZ", "America/Phoenix") or "America/Phoenix")

_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{30,}$")

def _mask_token(tok: str) -> str:
    if not tok: return "<empty>"
    if len(tok) < 12: return tok[:2] + "…" + tok[-2:]
    return tok[:6] + "…" + tok[-6:]

def _valid_token(tok: str) -> bool:
    # Defensive trim for stray BOM/whitespace
    s = tok.strip().replace("\ufeff", "")
    return bool(_TOKEN_RE.match(s))

_API_BASE = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}" if _TELEGRAM_BOT_TOKEN else "https://api.telegram.org/bot"

# --------------------------------------------------------------------------------------
# Rate limiter
# --------------------------------------------------------------------------------------
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

# --------------------------------------------------------------------------------------
# Time utilities
# --------------------------------------------------------------------------------------
def _tzinfo():
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
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def _console_print(prefix: str, msg: str):
    flat = " ".join((msg or "").split())
    print(f"[{_now_iso()}] {prefix} {flat}")

# --------------------------------------------------------------------------------------
# Message chunking
# --------------------------------------------------------------------------------------
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
            parts.append("".join(cur)); cur, length = [], 0
        if l > limit:
            for i in range(0, l, limit):
                chunk = line[i:i+limit]
                if length + len(chunk) > limit and cur:
                    parts.append("".join(cur)); cur, length = [], 0
                cur.append(chunk); length += len(chunk)
        else:
            cur.append(line); length += l
    if cur:
        parts.append("".join(cur))
    if len(parts) > 1:
        total = len(parts)
        parts = [f"{p.strip()}\n({i}/{total})" for i, p in enumerate(parts, 1)]
    return parts

def _telegram_enabled() -> bool:
    return bool(_TELEGRAM_BOT_TOKEN and _TELEGRAM_CHAT_IDS) and not _TELEGRAM_SILENT

# --------------------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------------------
def _retry_delay(attempt: int) -> float:
    # Exponential backoff with small jitter
    base = (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt)
    return min(8.0, base) + random.uniform(0, 0.15)

def _post_telegram_json(path: str, payload: dict) -> Optional[requests.Response]:
    url = f"{_API_BASE}/{path.lstrip('/')}"
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.ok:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = _retry_delay(attempt)
                if _TELEGRAM_DEBUG:
                    _console_print("notifier/telegram/retry:", f"HTTP {resp.status_code}; retry in {delay:.2f}s")
                time.sleep(delay)
                continue
            _console_print("notifier/telegram/error:", f"HTTP {resp.status_code} {resp.text[:300]}")
            return resp
        except Exception as e:
            last_exc = e
            delay = _retry_delay(attempt)
            if _TELEGRAM_DEBUG:
                _console_print("notifier/telegram/except:", f"{e}; retry in {delay:.2f}s")
            time.sleep(delay)
            continue
    if last_exc:
        _console_print("notifier/telegram/error:", f"{last_exc}")
    return None

def _post_telegram_multipart(path: str, fields: dict, files: dict) -> Optional[requests.Response]:
    url = f"{_API_BASE}/{path.lstrip('/')}"
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, data=fields, files=files, timeout=30)
            if resp.ok:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = _retry_delay(attempt)
                if _TELEGRAM_DEBUG:
                    _console_print("notifier/telegram/retry:", f"HTTP {resp.status_code}; retry in {delay:.2f}s")
                time.sleep(delay)
                continue
            _console_print("notifier/telegram/error:", f"HTTP {resp.status_code} {resp.text[:300]}")
            return resp
        except Exception as e:
            last_exc = e
            delay = _retry_delay(attempt)
            if _TELEGRAM_DEBUG:
                _console_print("notifier/telegram/except:", f"{e}; retry in {delay:.2f}s")
            time.sleep(delay)
            continue
    if last_exc:
        _console_print("notifier/telegram/error:", f"{last_exc}")
    return None

def _get_telegram(path: str, *, params: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        url = f"{_API_BASE}/{path.lstrip('/')}"
        return requests.get(url, params=params or {}, timeout=15)
    except Exception as e:
        _console_print("notifier/telegram/error:", f"GET {path}: {e}")
        return None

# --------------------------------------------------------------------------------------
# Diagnostics
# --------------------------------------------------------------------------------------
def _bom_prefix_debug(tok: str) -> str:
    if not tok:
        return "len=0"
    prv = [f"{ord(c):#06x}" for c in tok[:3]]
    nxt = [f"{ord(c):#06x}" for c in tok[-3:]]
    return f"len={len(tok)} first={prv} last={nxt}"

def tg_diag(verbose_updates: bool = False) -> bool:
    """Validate token with getMe and optionally fetch getUpdates. Logs more under CLI."""
    if _TELEGRAM_DEBUG:
        _console_print("notifier/info:", f"Token preview: {_mask_token(_TELEGRAM_BOT_TOKEN)} ({_bom_prefix_debug(_TELEGRAM_BOT_TOKEN)})")
    if not _valid_token(_TELEGRAM_BOT_TOKEN):
        _console_print("notifier/error:", "Bot token format is invalid. Get a fresh token from @BotFather.")
        _console_print("notifier/help:", "Ensure .env is UTF-8 (no BOM) and line is TELEGRAM_BOT_TOKEN=<token>")
        return False

    r = _get_telegram("getMe")
    if not r:
        _console_print("notifier/error:", "No response from Telegram. Network/DNS issue?")
        return False
    try:
        data = r.json()
    except Exception:
        _console_print("notifier/error:", f"getMe non-JSON response: HTTP {r.status_code}")
        return False

    if not data.get("ok", False):
        code = data.get("error_code")
        desc = data.get("description", "")
        _console_print("notifier/telegram/error:", f"getMe → {code} {desc}")
        return False

    if _TELEGRAM_DEBUG:
        me = data.get("result", {})
        _console_print("notifier/success:", f"getMe ok. Bot: @{me.get('username')} id={me.get('id')}")
        if _TELEGRAM_CHAT_IDS:
            _console_print("notifier/info:", f"Destinations: {', '.join(map(str, _TELEGRAM_CHAT_IDS))}")
        else:
            _console_print("notifier/warn:", "No TELEGRAM_CHAT_ID(S) configured.")

    if verbose_updates:
        up = _get_telegram("getUpdates")
        if up and up.ok:
            try:
                payload = up.json()
                tg_send_json(payload, title="getUpdates payload (trimmed)", indent=2, priority="info")
            except Exception:
                _console_print("notifier/info:", "getUpdates returned non-JSON; skipping print.")
        else:
            _console_print("notifier/info:", "getUpdates empty. Send /start to your bot, then try again.")
    return True

# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------
def set_chat_ids(ids: Iterable[str | int]) -> None:
    """Override destination chat IDs at runtime."""
    global _TELEGRAM_CHAT_IDS
    _TELEGRAM_CHAT_IDS = [str(x).strip() for x in ids if str(x).strip() and not _is_placeholder(x)]

def tg_send(msg: str,
            *,
            parse_mode: Optional[str] = None,
            disable_preview: Optional[bool] = None,
            notify: Optional[bool] = None,
            thread_id: Optional[int] = None,
            reply_to: Optional[int] = None,
            priority: str = "info") -> None:
    """Send a message to console and Telegram (if configured). Never raises upstream."""
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
                payload = {
                    "chat_id": str(chat_id),
                    "text": part,
                    "disable_web_page_preview": use_preview,
                    "disable_notification": not use_notify,
                }
                if use_parse:
                    payload["parse_mode"] = use_parse
                if use_thread:
                    payload["message_thread_id"] = use_thread
                if reply_to:
                    payload["reply_to_message_id"] = reply_to
                _post_telegram_json("sendMessage", payload)
    except Exception as e:
        _console_print("notifier/soft-fail:", f"{e}")

def tg_send_json(obj: Any,
                 title: Optional[str] = None,
                 *,
                 indent: int = 2,
                 priority: str = "info",
                 **kwargs) -> None:
    """Pretty-print JSON to console and Telegram. Applies chunking automatically."""
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
    """Send a code block as Markdown with triple backticks."""
    safe = text.replace("```", "`\u200d``")
    msg = f"```{language}\n{safe}\n```"
    kwargs.setdefault("parse_mode", "Markdown")
    tg_send(msg, priority=priority, **kwargs)

def tg_send_exception(e: Exception,
                      title: str = "Exception",
                      *,
                      priority: str = "error",
                      include_type: bool = True) -> None:
    """Format and send an exception."""
    name = type(e).__name__ if include_type else ""
    msg = f"{title}: {name}: {e}"
    tg_send(msg, priority=priority, parse_mode=None)

def tg_healthcheck(name: str = "notifier") -> None:
    """Tiny heartbeat; useful for cron or long-running bots."""
    tg_send(f"✅ {name} alive @ { _now_iso() }", priority="success")

def tg_validate(verbose_updates: bool = False) -> bool:
    """Validate token & send a test message. Returns True if at least one chat succeeded."""
    if not tg_diag(verbose_updates=verbose_updates):
        return False

    if not _TELEGRAM_CHAT_IDS:
        _console_print("notifier/error:", "No TELEGRAM_CHAT_ID(S) configured; cannot send test.")
        return False

    ok_any = False
    test_msg = f"🟢 Notifier validate @ {_now_iso()}"
    parts = _split_message(test_msg)
    for chat_id in _TELEGRAM_CHAT_IDS:
        payload = {
            "chat_id": str(chat_id),
            "text": parts[0],
            "disable_web_page_preview": True,
            "disable_notification": False,
        }
        if _TELEGRAM_THREAD_ID:
            payload["message_thread_id"] = _TELEGRAM_THREAD_ID
        resp = _post_telegram_json("sendMessage", payload)
        if resp and resp.ok:
            ok_any = True
        else:
            try:
                data = resp.json() if resp is not None else {}
                desc = str(data.get("description", "")).lower()
                code = data.get("error_code")
                if code == 401 or "unauthorized" in desc:
                    _console_print("notifier/help:", "Unauthorized: token is wrong or revoked.")
                elif "chat not found" in desc:
                    _console_print("notifier/help:", "CHAT_ID invalid or you never pressed Start with this bot.")
                elif "forbidden" in desc:
                    _console_print("notifier/help:", "Bot is blocked or not a member of the group/channel.")
                elif code == 404 or "not found" in desc:
                    _console_print("notifier/help:", "Not Found: malformed/empty token or BOM at start of line.")
                else:
                    _console_print("notifier/help:", "See Telegram API response above for details.")
            except Exception:
                _console_print("notifier/help:", "Non-JSON error; check network/mitm.")
    return ok_any

# --------------------------------------------------------------------------------------
# Media helpers
# --------------------------------------------------------------------------------------
def tg_send_photo(photo: Union[str, bytes, io.BytesIO],
                  caption: Optional[str] = None,
                  *,
                  notify: Optional[bool] = None,
                  thread_id: Optional[int] = None,
                  reply_to: Optional[int] = None,
                  priority: str = "info") -> None:
    """Send a photo to all configured chats. Accepts file path, bytes, or BytesIO."""
    try:
        _console_print(f"notifier/{priority}:", f"🖼️ photo {('with caption' if caption else '')}")
        if not _telegram_enabled():
            return
        use_notify = _DEFAULT_NOTIFY if notify is None else notify
        use_thread = thread_id if thread_id is not None else _TELEGRAM_THREAD_ID

        def _to_file_tuple(p: Union[str, bytes, io.BytesIO]):
            if isinstance(p, str):
                return ("photo.jpg", open(p, "rb"), "image/jpeg")
            if isinstance(p, bytes):
                return ("photo.jpg", io.BytesIO(p), "image/jpeg")
            if isinstance(p, io.BytesIO):
                return ("photo.jpg", p, "image/jpeg")
            raise TypeError("Unsupported photo type")

        for chat_id in _TELEGRAM_CHAT_IDS:
            fields = {
                "chat_id": str(chat_id),
                "disable_notification": "true" if not use_notify else "false",
            }
            if caption:
                fields["caption"] = caption
                # Telegram ignores parse_mode here unless specified in fields
            if use_thread:
                fields["message_thread_id"] = str(use_thread)
            files = {"photo": _to_file_tuple(photo)}
            _post_telegram_multipart("sendPhoto", fields, files)
    except Exception as e:
        _console_print("notifier/soft-fail:", f"{e}")

def tg_send_document(document: Union[str, bytes, io.BytesIO],
                     filename: Optional[str] = None,
                     caption: Optional[str] = None,
                     *,
                     notify: Optional[bool] = None,
                     thread_id: Optional[int] = None,
                     reply_to: Optional[int] = None,
                     priority: str = "info") -> None:
    """Send a document to all configured chats."""
    try:
        _console_print(f"notifier/{priority}:", f"📎 document {filename or ''}".strip())
        if not _telegram_enabled():
            return
        use_notify = _DEFAULT_NOTIFY if notify is None else notify
        use_thread = thread_id if thread_id is not None else _TELEGRAM_THREAD_ID

        def _to_file_tuple(d: Union[str, bytes, io.BytesIO], fname: Optional[str]):
            if isinstance(d, str):
                path = d
                name = fname or os.path.basename(path) or "file.bin"
                return (name, open(path, "rb"))
            if isinstance(d, bytes):
                name = fname or "file.bin"
                return (name, io.BytesIO(d))
            if isinstance(d, io.BytesIO):
                name = fname or "file.bin"
                return (name, d)
            raise TypeError("Unsupported document type")

        for chat_id in _TELEGRAM_CHAT_IDS:
            fields = {
                "chat_id": str(chat_id),
                "disable_notification": "true" if not use_notify else "false",
            }
            if caption:
                fields["caption"] = caption
            if use_thread:
                fields["message_thread_id"] = str(use_thread)
            files = {"document": _to_file_tuple(document, filename)}
            _post_telegram_multipart("sendDocument", fields, files)
    except Exception as e:
        _console_print("notifier/soft-fail:", f"{e}")

# Backward compatibility alias
send = tg_send

# --------------------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Base44 Notifier Bot (Telegram)")
    parser.add_argument("--validate", action="store_true", help="Run token check + send a test message")
    parser.add_argument("--updates", action="store_true", help="Also print getUpdates payload during --validate")
    parser.add_argument("--ping", nargs="?", const="notifier", help="Send a heartbeat message (optional name)")
    parser.add_argument("--echo", type=str, help="Send a custom message to configured chat(s)")
    args = parser.parse_args()

    # Verbose only on CLI
    _console_print("notifier/info:", f".env layered via env_bootstrap; token ok: {bool(_valid_token(_TELEGRAM_BOT_TOKEN))}  chats: {', '.join(_TELEGRAM_CHAT_IDS) if _TELEGRAM_CHAT_IDS else '<none>'}")

    ran = False

    if args.validate:
        ran = True
        ok = tg_validate(verbose_updates=args.updates)
        _console_print("notifier/result:", f"validate={'ok' if ok else 'fail'}")

    if args.ping is not None:
        ran = True
        tg_healthcheck(args.ping)

    if args.echo:
        ran = True
        tg_send(args.echo, priority="info")

    if not ran:
        tg_diag(verbose_updates=False)
        _console_print("notifier/info:", "Nothing else to do. Try --validate, --ping, or --echo.")
