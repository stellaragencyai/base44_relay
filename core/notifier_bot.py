# core/notifier_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notifier Bot (core) — quiet on import, layered env, retries, rate limit, media helpers.
Now with multi-bot routing per subaccount (optional cfg/tg_subaccounts.yaml) and hard mute.

What you get:
- Loads env via core/env_bootstrap (layered .env, repo-safe paths).
- Token format validation with masked previews (logs only on CLI).
- Multi-recipient via TELEGRAM_CHAT_IDS (overrides TELEGRAM_CHAT_ID).
- Optional per-subaccount routing using cfg/tg_subaccounts.yaml:
    * tg_send(..., sub_uid="260417071") targets that bot's token/chat_id
    * If sub_uid omitted, tries EXEC_ACCOUNT_UID / OWNERSHIP_SUB_UID, else first enabled bot
    * Falls back to legacy single-bot env if no YAML or no match
- Optional thread routing (forum topics) via TELEGRAM_THREAD_ID.
- Global rate limiting (token bucket) and per-bot de-duplication.
- Retries with backoff + jitter + 401 squelch (prevents log spam on bad tokens).
- Chunking for >4096 char messages.
- Media helpers: tg_send_photo(), tg_send_document() (accept sub_uid too).
- Console mirror for every message; Telegram send is optional and never raises.
- CLI: --validate, --updates, --ping [name], --echo "msg", --probe-yaml.

Env (in .env or process):
  TELEGRAM_BOT_TOKEN=123456:ABC...
  TELEGRAM_CHAT_ID=7776809236
  TELEGRAM_CHAT_IDS=777,888,-100123...
  TELEGRAM_THREAD_ID=123
  TELEGRAM_SILENT=0|1                # hard mute: never call Telegram APIs when "1"
  TELEGRAM_DISABLE_PREVIEW=1|0
  TELEGRAM_DEFAULT_PARSE_MODE=Markdown | HTML
  TELEGRAM_NOTIFY=1|0
  TELEGRAM_MAX_RETRIES=3
  TELEGRAM_BACKOFF_BASE_MS=400
  TELEGRAM_RATE_LIMIT_TPS=1.5
  TELEGRAM_DEBUG=0|1
  TZ=America/Phoenix

Multi-bot config (optional YAML; env TG_CONFIG_PATH can point elsewhere):
  cfg/tg_subaccounts.yaml
    bots:
      - name: main-alpha
        sub_uid: "260417071"
        token: "123456:AAABBB..."   # BotFather token
        chat_id: -1001234567890
        parse_mode: "MarkdownV2"
        rate_limit_per_min: 40
        dedupe_sec: 5
        enabled: true
    defaults:
      parse_mode: "MarkdownV2"
      rate_limit_per_min: 40
      dedupe_sec: 5
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
from typing import Any, Iterable, Optional, List, Union, Dict

from pathlib import Path

# Layered env + safe paths (not strictly required here)
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
    if not tok:
        return "<empty>"
    if len(tok) < 12:
        return tok[:2] + "…" + tok[-2:]
    return tok[:6] + "…" + tok[-6:]

def _valid_token(tok: str) -> bool:
    # Defensive trim for stray BOM/whitespace
    s = (tok or "").strip().replace("\ufeff", "")
    return bool(_TOKEN_RE.match(s))

# Optional settings read for sub_uid resolution
try:
    from core.config import settings
except Exception:
    class _S:
        EXEC_ACCOUNT_UID = None
        OWNERSHIP_SUB_UID = None
    settings = _S()  # type: ignore

# Multi-bot YAML (optional)
_TG_CFG_PATH = Path(os.getenv("TG_CONFIG_PATH", "cfg/tg_subaccounts.yaml"))

def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except Exception:
        return {}
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

_cfg_cache: Dict[str, Any] = {}
_bot_state: Dict[str, Dict[str, Any]] = {}  # per-sub_uid runtime (dedupe/rl)
_unauth_squelch: Dict[str, float] = {}      # token -> last 401 ts

def _reload_cfg() -> Dict[str, Any]:
    global _cfg_cache
    _cfg_cache = _load_yaml(_TG_CFG_PATH)
    return _cfg_cache

def _cfg() -> Dict[str, Any]:
    return _cfg_cache or _reload_cfg()

def _resolve_sub_uid(explicit: Optional[str]) -> Optional[str]:
    if explicit:
        return str(explicit)
    # Try runtime/common envs
    uid = (os.getenv("EXEC_ACCOUNT_UID") or os.getenv("OWNERSHIP_SUB_UID")
           or getattr(settings, "EXEC_ACCOUNT_UID", None) or getattr(settings, "OWNERSHIP_SUB_UID", None))
    return str(uid) if uid else None

def _pick_bot(sub_uid: Optional[str]) -> Optional[Dict[str, Any]]:
    cfg = _cfg()
    bots = cfg.get("bots") or []
    defaults = cfg.get("defaults") or {}
    if bots:
        if sub_uid:
            for b in bots:
                if not b.get("enabled", True):
                    continue
                if str(b.get("sub_uid")) == str(sub_uid):
                    out = dict(defaults); out.update(b); return out
        # fallback: first enabled
        for b in bots:
            if b.get("enabled", True):
                out = dict(defaults); out.update(b); return out
    return None

# --------------------------------------------------------------------------------------
# Rate limiter (global token bucket) + per-bot dedupe
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

def _bot_runtime(uid: str, per_min_default: int = 40, dedupe_default: int = 5) -> Dict[str, Any]:
    st = _bot_state.setdefault(uid, {"bucket": [], "last_text": "", "last_ts": 0.0,
                                     "per_min": per_min_default, "dedupe": dedupe_default})
    return st

def _allow_send_bot(rt: Dict[str, Any], per_min: int, dedupe_sec: int, text: str) -> bool:
    now = time.time()
    # dedupe identical spam
    if text == rt.get("last_text") and (now - float(rt.get("last_ts", 0))) < dedupe_sec:
        return False
    # simple per-minute leaky bucket per bot
    bucket = [t for t in rt.get("bucket", []) if now - t < 60.0]
    if len(bucket) >= per_min:
        return False
    bucket.append(now)
    rt["bucket"] = bucket
    rt["last_text"] = text
    rt["last_ts"] = now
    return True

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

# --------------------------------------------------------------------------------------
# Enablement checks
# --------------------------------------------------------------------------------------
def _telegram_enabled_env() -> bool:
    # require non-silent and valid token+dest
    return (not _TELEGRAM_SILENT) and _valid_token(_TELEGRAM_BOT_TOKEN) and bool(_TELEGRAM_CHAT_IDS)

def _telegram_enabled_any() -> bool:
    if _TELEGRAM_SILENT:
        return False
    # either multi-bot config has at least one enabled bot with valid token+chat
    cfg = _cfg()
    bots = cfg.get("bots") or []
    if bots:
        for b in bots:
            if not b.get("enabled", True):
                continue
            tok = str(b.get("token") or "")
            cid = b.get("chat_id", None)
            if _valid_token(tok) and (cid is not None):
                return True
    # or legacy env single bot
    return _telegram_enabled_env()

# --------------------------------------------------------------------------------------
# HTTP helpers (per-token)
# --------------------------------------------------------------------------------------
def _api_base_for_token(tok: str) -> str:
    return f"https://api.telegram.org/bot{tok}"

def _retry_delay(attempt: int) -> float:
    # Exponential backoff with small jitter
    base = (_BACKOFF_BASE_MS / 1000.0) * (2 ** attempt)
    return min(8.0, base) + random.uniform(0, 0.15)

def _squelch_unauthorized(token: str) -> bool:
    """Return True if we should suppress logging another 401 for this token right now."""
    now = time.time()
    last = _unauth_squelch.get(token, 0.0)
    if now - last < 60.0:
        return True
    _unauth_squelch[token] = now
    return False

def _post_telegram_json_token(token: str, path: str, payload: dict) -> Optional[requests.Response]:
    if _TELEGRAM_SILENT or not _valid_token(token):
        return None
    url = f"{_api_base_for_token(token)}/{path.lstrip('/')}"
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, json=payload, timeout=20)
            if resp.ok:
                return resp
            if resp.status_code == 401:
                if not _squelch_unauthorized(token):
                    _console_print("notifier/telegram/error:", f"HTTP 401 Unauthorized (token={_mask_token(token)})")
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

def _post_telegram_multipart_token(token: str, path: str, fields: dict, files: dict) -> Optional[requests.Response]:
    if _TELEGRAM_SILENT or not _valid_token(token):
        return None
    url = f"{_api_base_for_token(token)}/{path.lstrip('/')}"
    last_exc = None
    for attempt in range(_MAX_RETRIES + 1):
        _rate_limit_consume(1.0)
        try:
            resp = requests.post(url, data=fields, files=files, timeout=30)
            if resp.ok:
                return resp
            if resp.status_code == 401:
                if not _squelch_unauthorized(token):
                    _console_print("notifier/telegram/error:", f"HTTP 401 Unauthorized (token={_mask_token(token)})")
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

# Legacy single-bot helpers remain for CLI validate/diag
def _get_telegram_env(path: str, *, params: Optional[dict] = None) -> Optional[requests.Response]:
    try:
        token = _TELEGRAM_BOT_TOKEN
        if _TELEGRAM_SILENT or not _valid_token(token):
            return None
        base = _api_base_for_token(token)
        url = f"{base}/{path.lstrip('/')}"
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
    """Validate token with getMe (legacy single-bot) and optionally fetch getUpdates."""
    if _TELEGRAM_DEBUG:
        _console_print("notifier/info:", f"Token preview: {_mask_token(_TELEGRAM_BOT_TOKEN)} ({_bom_prefix_debug(_TELEGRAM_BOT_TOKEN)})")
    if _TELEGRAM_SILENT:
        _console_print("notifier/warn:", "Silent mode is ON; skipping Telegram validation.")
        return True
    if not _valid_token(_TELEGRAM_BOT_TOKEN):
        _console_print("notifier/error:", "Bot token format is invalid (env TELEGRAM_BOT_TOKEN).")
        _console_print("notifier/help:", "Ensure .env is UTF-8 (no BOM) and line TELEGRAM_BOT_TOKEN=<token>")
        return False

    r = _get_telegram_env("getMe")
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
        up = _get_telegram_env("getUpdates")
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
    """Override destination chat IDs at runtime (legacy env mode)."""
    global _TELEGRAM_CHAT_IDS
    _TELEGRAM_CHAT_IDS = [str(x).strip() for x in ids if str(x).strip() and not _is_placeholder(x)]

def _sanitize_links(text: str) -> str:
    """Avoid leaking full client link IDs; keep a short preview after 'link=' tokens."""
    try:
        if "link=" in (text or ""):
            parts = text.split("link=")
            head, tail = parts[0], "link=".join(parts[1:])
            first_token = tail.split()[0]
            short = first_token[:32] + "…" if len(first_token) > 32 else first_token
            rest = tail[len(first_token):]
            return head + "link=" + short + rest
    except Exception:
        pass
    return text

def tg_send(msg: str,
            *,
            parse_mode: Optional[str] = None,
            disable_preview: Optional[bool] = None,
            notify: Optional[bool] = None,
            thread_id: Optional[int] = None,
            reply_to: Optional[int] = None,
            priority: str = "info",
            sub_uid: Optional[str] = None) -> None:
    """Send a message to console and Telegram (multi-bot aware). Never raises upstream."""
    try:
        prefix = {
            "error": "❌",
            "warn": "⚠️",
            "success": "✅",
            "info": "ℹ️",
        }.get(priority, "ℹ️")
        _console_print(f"notifier/{priority}:", f"{prefix} {msg}")

        if not _telegram_enabled_any():
            return

        use_parse = parse_mode if parse_mode is not None else (_DEFAULT_PARSE_MODE or None)
        use_preview = _DISABLE_PREVIEW if disable_preview is None else disable_preview
        use_notify = _DEFAULT_NOTIFY if notify is None else notify
        use_thread = thread_id if thread_id is not None else _TELEGRAM_THREAD_ID

        body = _sanitize_links(msg)
        parts = _split_message(body)

        # Try multi-bot config first
        target_uid = _resolve_sub_uid(sub_uid)
        bot_cfg = _pick_bot(target_uid)

        if bot_cfg and _valid_token(str(bot_cfg.get("token") or "")) and (bot_cfg.get("chat_id") is not None):
            token = str(bot_cfg["token"]).strip()
            chat_id = bot_cfg["chat_id"]
            per_min = int(bot_cfg.get("rate_limit_per_min", 40))
            dedupe = int(bot_cfg.get("dedupe_sec", 5))
            parse_for_bot = use_parse or bot_cfg.get("parse_mode")

            # per-bot dedupe/rl
            rt = _bot_runtime(str(bot_cfg.get("sub_uid") or "default"), per_min, dedupe)
            for part in parts:
                decorated = f"{prefix} {part}"
                if not _allow_send_bot(rt, per_min, dedupe, decorated):
                    continue
                payload = {
                    "chat_id": str(chat_id),
                    "text": decorated,
                    "disable_web_page_preview": use_preview,
                    "disable_notification": not use_notify,
                }
                if parse_for_bot:
                    payload["parse_mode"] = parse_for_bot
                if use_thread:
                    payload["message_thread_id"] = use_thread
                if reply_to:
                    payload["reply_to_message_id"] = reply_to
                _post_telegram_json_token(token, "sendMessage", payload)
            return

        # Fallback: legacy single-bot env mode (multi-chat support)
        if not _telegram_enabled_env():
            return
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
                _post_telegram_json_token(_TELEGRAM_BOT_TOKEN, "sendMessage", payload)
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
                      include_type: bool = True,
                      **kwargs) -> None:
    """Format and send an exception."""
    name = type(e).__name__ if include_type else ""
    msg = f"{title}: {name}: {e}"
    tg_send(msg, priority=priority, parse_mode=None, **kwargs)

def tg_healthcheck(name: str = "notifier", **kwargs) -> None:
    """Tiny heartbeat; useful for cron or long-running bots."""
    tg_send(f"✅ {name} alive @ { _now_iso() }", priority="success", **kwargs)

def tg_validate(verbose_updates: bool = False) -> bool:
    """
    Validate in legacy env mode. If you’re using multi-bot routing, just send a test via tg_send(..., sub_uid="...").
    Returns True if at least one chat succeeded.
    """
    if _TELEGRAM_SILENT:
        _console_print("notifier/warn:", "Silent mode is ON; skipping Telegram validation.")
        return True

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
        resp = _post_telegram_json_token(_TELEGRAM_BOT_TOKEN, "sendMessage", payload)
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
# Media helpers (multi-bot aware)
# --------------------------------------------------------------------------------------
def tg_send_photo(photo: Union[str, bytes, io.BytesIO],
                  caption: Optional[str] = None,
                  *,
                  notify: Optional[bool] = None,
                  thread_id: Optional[int] = None,
                  reply_to: Optional[int] = None,
                  priority: str = "info",
                  sub_uid: Optional[str] = None) -> None:
    """Send a photo. Accepts file path, bytes, or BytesIO. Multi-bot aware via sub_uid."""
    try:
        _console_print(f"notifier/{priority}:", f"🖼️ photo {('with caption' if caption else '')}")
        if not _telegram_enabled_any():
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

        # route
        target_uid = _resolve_sub_uid(sub_uid)
        bot_cfg = _pick_bot(target_uid)

        if bot_cfg and _valid_token(str(bot_cfg.get("token") or "")) and (bot_cfg.get("chat_id") is not None):
            token = str(bot_cfg["token"]).strip()
            chat_id = bot_cfg["chat_id"]
            fields = {
                "chat_id": str(chat_id),
                "disable_notification": "true" if not use_notify else "false",
            }
            if caption:
                fields["caption"] = caption
            if use_thread:
                fields["message_thread_id"] = str(use_thread)
            files = {"photo": _to_file_tuple(photo)}
            _post_telegram_multipart_token(token, "sendPhoto", fields, files)
            return

        # fallback env mode
        if not _telegram_enabled_env():
            return
        for chat_id in _TELEGRAM_CHAT_IDS:
            fields = {
                "chat_id": str(chat_id),
                "disable_notification": "true" if not use_notify else "false",
            }
            if caption:
                fields["caption"] = caption
            if use_thread:
                fields["message_thread_id"] = str(use_thread)
            files = {"photo": _to_file_tuple(photo)}
            _post_telegram_multipart_token(_TELEGRAM_BOT_TOKEN, "sendPhoto", fields, files)
    except Exception as e:
        _console_print("notifier/soft-fail:", f"{e}")

def tg_send_document(document: Union[str, bytes, io.BytesIO],
                     filename: Optional[str] = None,
                     caption: Optional[str] = None,
                     *,
                     notify: Optional[bool] = None,
                     thread_id: Optional[int] = None,
                     reply_to: Optional[int] = None,
                     priority: str = "info",
                     sub_uid: Optional[str] = None) -> None:
    """Send a document to configured chat(s). Multi-bot aware via sub_uid."""
    try:
        _console_print(f"notifier/{priority}:", f"📎 document {filename or ''}".strip())
        if not _telegram_enabled_any():
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

        # route
        target_uid = _resolve_sub_uid(sub_uid)
        bot_cfg = _pick_bot(target_uid)

        if bot_cfg and _valid_token(str(bot_cfg.get("token") or "")) and (bot_cfg.get("chat_id") is not None):
            token = str(bot_cfg["token"]).strip()
            chat_id = bot_cfg["chat_id"]
            fields = {
                "chat_id": str(chat_id),
                "disable_notification": "true" if not use_notify else "false",
            }
            if caption:
                fields["caption"] = caption
            if use_thread:
                fields["message_thread_id"] = str(use_thread)
            files = {"document": _to_file_tuple(document, filename)}
            _post_telegram_multipart_token(token, "sendDocument", fields, files)
            return

        # fallback env mode
        if not _telegram_enabled_env():
            return
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
            _post_telegram_multipart_token(_TELEGRAM_BOT_TOKEN, "sendDocument", fields, files)
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
    parser.add_argument("--validate", action="store_true", help="Run token check + send a test message (legacy env)")
    parser.add_argument("--updates", action="store_true", help="Also print getUpdates payload during --validate")
    parser.add_argument("--ping", nargs="?", const="notifier", help="Send a heartbeat message (optional name)")
    parser.add_argument("--echo", type=str, help="Send a custom message to configured chat(s)")
    parser.add_argument("--probe-yaml", action="store_true", help="Print parsed tg_subaccounts.yaml and first target")
    args = parser.parse_args()

    # Verbose only on CLI
    _console_print("notifier/info:", f".env layered via env_bootstrap; token ok: {bool(_valid_token(_TELEGRAM_BOT_TOKEN))}  chats: {', '.join(_TELEGRAM_CHAT_IDS) if _TELEGRAM_CHAT_IDS else '<none>'}")

    ran = False

    if args.probe_yaml:
        ran = True
        c = _cfg()
        bots = c.get("bots") or []
        defaults = c.get("defaults") or {}
        _console_print("notifier/info:", f"yaml bots={len(bots)} defaults={bool(defaults)} path={_TG_CFG_PATH}")
        if bots:
            # show first enabled bot masked
            b = next((x for x in bots if x.get("enabled", True)), bots[0])
            tok = str(b.get("token", "")); cid = b.get("chat_id")
            _console_print("notifier/info:", f"first enabled: sub_uid={b.get('sub_uid')} chat_id={cid} token={_mask_token(tok)}")

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
        # Light diag for env path; multi-bot users can do --probe-yaml
        tg_diag(verbose_updates=False)
        _console_print("notifier/info:", "Nothing else to do. Try --validate, --ping, --echo, or --probe-yaml.")
