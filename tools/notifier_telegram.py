#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Telegram Notifier (tools/notifier_telegram.py)

What this module does:
- Centralizes Telegram messaging for all bots (text, markdown, html, photo, document).
- Uses settings from core.config (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).
- Graceful error handling: retries, chunking, optional quiet failure.
- Detects common misconfig problems ("chat not found", "bot was blocked").
- CLI self-test: `python -m tools.notifier_telegram "hello from base44"`

Env keys (already handled by core.config):
  TELEGRAM_BOT_TOKEN=123456:abcdef...
  TELEGRAM_CHAT_ID=7776809236

Usage (from another module):
    from tools.notifier_telegram import tg

    tg.send_text("TP/SL Manager online ✅", parse_mode="Markdown")
    tg.send_markdown("*Filled:* 3/50  •  PnL est: $12.34")
    tg.safe_text("something that may fail", quiet=True)
"""

from __future__ import annotations
import json
import os
import time
import mimetypes
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, Union
import urllib.request
import urllib.error

from core.config import settings
from core.logger import get_logger

log = get_logger("tools.notifier_telegram")

TELEGRAM_API_BASE = "https://api.telegram.org"

# Telegram limits
MAX_TEXT_LEN = 4096        # per message
MAX_CAPTION_LEN = 1024     # photos/doc captions

# -----------------------------
# Low-level HTTP helper
# -----------------------------

def _http_post_json(url: str, payload: Dict[str, Any], timeout: int = 15) -> Tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        return e.code, body
    except Exception as e:
        return 0, str(e)


def _http_post_multipart(url: str, fields: Dict[str, Any], files: Dict[str, Path], timeout: int = 30) -> Tuple[int, str]:
    boundary = f"----Base44Form{int(time.time()*1000)}"
    body = bytearray()

    def add_field(name: str, value: str) -> None:
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body.extend(value.encode("utf-8"))
        body.extend(b"\r\n")

    def add_file(name: str, file_path: Path) -> None:
        mime, _ = mimetypes.guess_type(str(file_path))
        mime = mime or "application/octet-stream"
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(
            f'Content-Disposition: form-data; name="{name}"; filename="{file_path.name}"\r\n'.encode()
        )
        body.extend(f"Content-Type: {mime}\r\n\r\n".encode())
        with open(file_path, "rb") as f:
            body.extend(f.read())
        body.extend(b"\r\n")

    for k, v in fields.items():
        add_field(k, str(v))

    for k, p in files.items():
        add_file(k, p)

    body.extend(f"--{boundary}--\r\n".encode())

    req = urllib.request.Request(url, data=bytes(body), headers={
        "Content-Type": f"multipart/form-data; boundary={boundary}"
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        return e.code, body
    except Exception as e:
        return 0, str(e)

# -----------------------------
# Core notifier
# -----------------------------

class TelegramNotifier:
    def __init__(self, bot_token: Optional[str], chat_id: Optional[str]):
        self.bot_token = (bot_token or "").strip()
        self.chat_id = (chat_id or "").strip()

    # Basic health
    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def api_url(self, method: str) -> str:
        return f"{TELEGRAM_API_BASE}/bot{self.bot_token}/{method}"

    # --------------- text helpers ---------------

    def _chunk(self, text: str, max_len: int) -> Iterable[str]:
        text = text or ""
        if len(text) <= max_len:
            yield text
            return
        # Try to split on newlines first
        start = 0
        while start < len(text):
            end = min(start + max_len, len(text))
            # backtrack to last newline if possible
            nl = text.rfind("\n", start, end)
            if nl == -1 or nl <= start + 100:  # avoid microscopic chunks
                yield text[start:end]
                start = end
            else:
                yield text[start:nl]
                start = nl + 1

    def _post_sendMessage(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        code, body = _http_post_json(self.api_url("sendMessage"), payload, timeout=settings.HTTP_TIMEOUT_S)
        ok, err = self._interpret_response(code, body)
        return ok, err

    def _post_sendPhoto(self, fields: Dict[str, Any], file: Optional[Path]) -> Tuple[bool, str]:
        if file:
            code, body = _http_post_multipart(self.api_url("sendPhoto"), fields, {"photo": file})
        else:
            code, body = _http_post_json(self.api_url("sendPhoto"), fields, timeout=settings.HTTP_TIMEOUT_S)
        ok, err = self._interpret_response(code, body)
        return ok, err

    def _post_sendDocument(self, fields: Dict[str, Any], file: Optional[Path]) -> Tuple[bool, str]:
        if file:
            code, body = _http_post_multipart(self.api_url("sendDocument"), fields, {"document": file})
        else:
            code, body = _http_post_json(self.api_url("sendDocument"), fields, timeout=settings.HTTP_TIMEOUT_S)
        ok, err = self._interpret_response(code, body)
        return ok, err

    # --------------- public text API ---------------

    def send_text(self, text: str, parse_mode: Optional[str] = None, disable_web_page_preview: bool = True) -> bool:
        """
        Send plain text. Automatically chunks >4096 chars.
        Returns True if all chunks succeeded.
        """
        if not self.is_configured():
            log.warning("[tg] not configured (missing token or chat id)")
            return False

        all_ok = True
        for chunk in self._chunk(text, MAX_TEXT_LEN):
            payload = {
                "chat_id": self.chat_id,
                "text": chunk,
                "disable_web_page_preview": disable_web_page_preview,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            ok, err = self._retry(lambda: self._post_sendMessage(payload))
            if not ok:
                log.error("[tg] send_text failed: %s", err)
                all_ok = False
        return all_ok

    def send_markdown(self, text: str) -> bool:
        return self.send_text(text, parse_mode="Markdown")

    def send_html(self, text: str) -> bool:
        return self.send_text(text, parse_mode="HTML")

    def safe_text(self, text: str, parse_mode: Optional[str] = None, quiet: bool = False) -> bool:
        try:
            return self.send_text(text, parse_mode=parse_mode)
        except Exception as e:
            if not quiet:
                log.error("[tg] safe_text exception: %s", e)
            return False

    # --------------- media API ---------------

    def send_photo(self, photo: Union[str, Path], caption: Optional[str] = None, parse_mode: Optional[str] = None) -> bool:
        """
        photo can be a local Path or a URL string.
        """
        if not self.is_configured():
            log.warning("[tg] not configured (missing token or chat id)")
            return False

        fields: Dict[str, Any] = {"chat_id": self.chat_id}
        if caption:
            fields["caption"] = caption[:MAX_CAPTION_LEN]
        if parse_mode:
            fields["parse_mode"] = parse_mode

        file_path: Optional[Path] = None
        if isinstance(photo, Path) or (isinstance(photo, str) and Path(photo).exists()):
            file_path = Path(photo) if not isinstance(photo, Path) else photo
        else:
            fields["photo"] = str(photo)

        ok, err = self._retry(lambda: self._post_sendPhoto(fields, file_path))
        if not ok:
            log.error("[tg] send_photo failed: %s", err)
        return ok

    def send_document(self, doc: Union[str, Path], caption: Optional[str] = None) -> bool:
        """
        doc can be a local Path only (Telegram expects the file upload for local).
        """
        if not self.is_configured():
            log.warning("[tg] not configured (missing token or chat id)")
            return False

        path = Path(doc) if not isinstance(doc, Path) else doc
        if not path.exists():
            log.error("[tg] document path not found: %s", path)
            return False

        fields: Dict[str, Any] = {"chat_id": self.chat_id}
        if caption:
            fields["caption"] = caption[:MAX_CAPTION_LEN]

        ok, err = self._retry(lambda: self._post_sendDocument(fields, path))
        if not ok:
            log.error("[tg] send_document failed: %s", err)
        return ok

    # --------------- utils ---------------

    def _retry(self, fn, attempts: int = 3, backoff: float = 0.8) -> Tuple[bool, str]:
        last_err = ""
        for i in range(attempts):
            ok, err = fn()
            if ok:
                return True, ""
            last_err = err
            # For certain errors, retrying is pointless
            if self._is_perm_error(err):
                break
            time.sleep(backoff * (2 ** i))
        return False, last_err

    @staticmethod
    def _is_perm_error(err: str) -> bool:
        e = err.lower()
        if "chat not found" in e:
            return True
        if "bot was blocked by the user" in e:
            return True
        if "unauthorized" in e and "bot token" in e:
            return True
        return False

    @staticmethod
    def _interpret_response(code: int, body: str) -> Tuple[bool, str]:
        # Network fail
        if code == 0:
            return False, body or "network error"

        # Parse Telegram response
        try:
            data = json.loads(body)
        except Exception:
            data = None

        if code == 200 and isinstance(data, dict) and data.get("ok") is True:
            return True, ""
        # Telegram standard error shape
        if isinstance(data, dict) and "description" in data:
            return False, f"HTTP {code} {data.get('description')}"
        # Fallback
        return False, f"HTTP {code} body={body[:300]}"

# Singleton for convenience
tg = TelegramNotifier(settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID)

# ---------------------------------
# CLI self-test
# ---------------------------------

def _cli_selftest(msg: str) -> int:
    if not tg.is_configured():
        log.error("Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in your .env")
        return 2
    ok = tg.send_text(f"Base44 notifier self-test:\n{msg}")
    return 0 if ok else 1

if __name__ == "__main__":
    import sys
    test_msg = "hello from Base44 ✅"
    if len(sys.argv) > 1:
        test_msg = " ".join(sys.argv[1:])
    exit(_cli_selftest(test_msg))
