# core/notifier_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notifier Bot (core) — DC-safe, token-validated, env-safe

Adds:
- Root .env discovery via find_dotenv
- Token regex validation + masked preview
- getMe probe with clear diagnostics
- Optional getUpdates fetch to help discover chat_id
- Timezone-aware timestamps (no utcnow deprecation)
"""

from __future__ import annotations
import os
import re
import time
import json
import threading
import datetime
from typing import Any, Iterable, Optional, List

import requests

# dotenv: load root .env reliably
try:
    from dotenv import load_dotenv, find_dotenv
    _ENV_PATH = find_dotenv(filename=".env", usecwd=True)
    if _ENV_PATH:
        load_dotenv(_ENV_PATH)
        print(f"[notifier] Loaded .env: {_ENV_PATH}")
    else:
        load_dotenv()
        print("[notifier/warn] No project .env found via find_dotenv; relying on process env.")
except Exception:
    _ENV_PATH = None
    print("[notifier/warn] python-dotenv not available; relying on process env.")

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
_TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
_TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
_TELEGRAM_CHAT_IDS = [c.strip() for c in (os.getenv("TELEGRAM_CHAT_IDS", "") or "").split(",") if c.strip()]
if not _TELEGRAM_CHAT_IDS and _TELEGRAM_CHAT_ID:
    _TELEGRAM_CHAT_IDS = [_TELEGRAM_CHAT_ID]

_TELEGRAM_THREAD_ID = (os.getenv("TELEGRAM_THREAD_ID", "") or "").strip()
try:
    _TELEGRAM_THREAD_ID = int(_TELEGRAM_THREAD_ID) if _TELEGRAM_THREAD_ID else None
except Exception:
    _TELEGRAM_THREAD_ID = None

_TELEGRAM_SILENT = (os.getenv("TELEGRAM_SILENT", "0") or "0") == "1"
_DISABLE_PREVIEW = (os.getenv("TELEGRAM_DISABLE_PREVIEW", "1") or "1") != "0"
_DEFAULT_PARSE_MODE = (os.getenv("TELEGRAM_DEFAULT_PARSE_MODE", "") or "").strip()
_DEFAULT_NOTIFY = (os.getenv("TELEGRAM_NOTIFY", "1") or "1") != "0"
_MAX_RETRIES = max(0, int((os.getenv("TELEGRAM_MAX_RETRIES", "3") or "3")))
_BACKOFF_BASE_MS = max(50, int((os.getenv("TELEGRAM_BACKOFF_BASE_MS", "400") or "400")))
_RATE_LIMIT_TPS = max(0.1, float((os.getenv("TELEGRAM_RATE_LIMIT_TPS", "1.5") or "1.5")))

_TZ = (os.getenv("TZ", "UTC") or "UTC")

# ------------------------------------------------------------------------------
# Token validation and API base
# ------------------------------------------------------------------------------
_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{30,}$")
def _mask_token(tok: str) -> str:
    if not tok:
        return "<empty>"
    if len(tok) < 12:
        return tok[0:2] + "…" + tok[-2:]
    return tok[:6] + "…" + tok[-6:]

def _valid_token(tok: str) -> bool:
    return bool(_TOKEN_RE.match(tok))

_API_BASE = f"https://api.telegram.org/bot{_TELEGRAM_BOT_TOKEN}" if _TELEGRAM_BOT_TOKEN else "https://api.telegram.org/bot"

# ------------------------------------------------------------------------------
# Rate limiter
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

# ------------------------------------------------------------------------------
# HTTP helpers
# ------------------------------------------------------------------------------
def _post_telegram(chat_id: str, text: str, *,
                   parse_mode: Optional[str],
                   disable_preview: bool,
                   notify: bool,
                   thread_id: Optional[int],
                   reply_to: Optional[int]) -> Optional[requests.Response]:
    url = f"{_API_BASE}/sendMessage"
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
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(8.0, (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt))
                _console_print("notifier/telegram/retry:", f"HTTP {resp.status_code}; retry in {delay:.2f}s")
                time.sleep(delay); continue
            _console_print("notifier/telegram/error:", f"HTTP {resp.status_code} {resp.text[:300]}")
            return resp
        except Exception as e:
            last_exc = e
            delay = min(8.0, (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt))
            _console_print("notifier/telegram/except:", f"{e}; retry in {delay:.2f}s")
            time.sleep(delay); continue
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

# ------------------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------------------
def tg_diag(verbose_updates: bool = False) -> bool:
    """
    Validate token with getMe and optionally fetch getUpdates.
    Returns True if token looks valid (getMe ok).
    """
    masked = _mask_token(_TELEGRAM_BOT_TOKEN)
    _console_print("notifier/info:", f"Token preview: {masked}")
    if not _valid_token(_TELEGRAM_BOT_TOKEN):
        _console_print("notifier/error:", "Bot token format is invalid. Get a fresh token from @BotFather.")
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
        if code == 401:
            _console_print("notifier/help:", "Unauthorized: token is wrong or revoked.")
        elif code == 404:
            _console_print("notifier/help:", "Not Found: endpoint didn’t recognize your token. Usually bad/mistyped token.")
        else:
            _console_print("notifier/help:", "Fix token in .env (TELEGRAM_BOT_TOKEN) and try again.")
        return False

    me = data.get("result", {})
    _console_print("notifier/success:", f"getMe ok. Bot: @{me.get('username')} id={me.get('id')}")

    # Show chat ids we will use
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
            _console_print("notifier/info:", "getUpdates not available or empty (send /start to your bot, then try again).")
    return True

# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------
def set_chat_ids(ids: Iterable[str | int]) -> None:
    """Override destination chat IDs at runtime."""
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
    Send a message to console and Telegram (if configured). Never raises upstream.
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
    safe = text.replace("```", "`\u200d``")  # zero-width joiner to break fence
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
    """
    Validate token & send a test message. Returns True if at least one chat succeeded.
    """
    if not tg_diag(verbose_updates=verbose_updates):
        return False

    if not _TELEGRAM_CHAT_IDS:
        _console_print("notifier/error:", "No TELEGRAM_CHAT_ID(S) configured; cannot send test.")
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
                    _console_print("notifier/help:", "Not Found: usually a malformed token hitting the wrong endpoint.")
                else:
                    _console_print("notifier/help:", "See Telegram API response above for details.")
            except Exception:
                _console_print("notifier/help:", "Non-JSON error; check network/mitm.")
    return ok_any

# Backward compatibility alias
send = tg_send
