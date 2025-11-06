#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Tunnel Watchdog (automation-ready)

What it does
- Polls a public endpoint for your relay (default /health, optional /diag/time).
- Uses HEAD first, falls back to GET, follows redirects if allowed.
- Sends Telegram alerts on UP/DOWN transitions, host or IP changes, and mismatch with RELAY_URL.
- Throttles alerts so you don't get rage-quit pings.
- Persists live state under .state/tunnel.json for dashboards/other bots.
- Exponential backoff while DOWN, small jitter while UP.

Env (.env)
  PUBLIC_URL=https://<your-ngrok>.ngrok-free.dev
  RELAY_URL=https://<same-as-above-please>          # optional, warns if mismatch
  RELAY_TOKEN=...                                   # optional, sent as Bearer + x-relay-token
  TW_POLL_SEC=30
  TW_ALERT_COOLDOWN_SEC=120
  TW_EXPECT_PATH=/health                            # or /diag/time
  TW_ALLOW_REDIRECTS=true
  TW_HEAD_FIRST=true
  TW_BACKOFF_SEQ=2,4,8,16,32
  TW_JITTER_SEC=3
  TW_IP_RESOLVE=true
  STATE_DIR=.state
  LOG_LEVEL=INFO
"""

from __future__ import annotations
import os, sys, time, json, socket, urllib.parse, random, logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from dotenv import load_dotenv

# --- robust project import path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.argv:
    sys.path.insert(0, str(PROJECT_ROOT))

# notifier (soft)
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):
        print(f"[notify/{priority}] {msg}")

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tunnel_watchdog")

PUBLIC_URL   = (os.getenv("PUBLIC_URL", "") or "").rstrip("/")
RELAY_URL    = (os.getenv("RELAY_URL", "") or os.getenv("DASHBOARD_RELAY_BASE", "") or "").rstrip("/")
RELAY_TOKEN  = os.getenv("RELAY_TOKEN", "") or os.getenv("RELAY_SECRET", "")

POLL_SEC     = int(os.getenv("TW_POLL_SEC", "30"))
COOLDOWN_SEC = int(os.getenv("TW_ALERT_COOLDOWN_SEC", "120"))
EXPECT_PATH  = os.getenv("TW_EXPECT_PATH", "/health").strip() or "/health"
ALLOW_REDIR  = (os.getenv("TW_ALLOW_REDIRECTS", "true").strip().lower() in {"1","true","yes","on"})
HEAD_FIRST   = (os.getenv("TW_HEAD_FIRST", "true").strip().lower() in {"1","true","yes","on"})
BACKOFF_SEQ  = [int(x) for x in (os.getenv("TW_BACKOFF_SEQ", "2,4,8,16,32").split(",")) if x.strip()]
JITTER_SEC   = int(os.getenv("TW_JITTER_SEC", "3"))
RESOLVE_IP   = (os.getenv("TW_IP_RESOLVE", "true").strip().lower() in {"1","true","yes","on"})

STATE_DIR    = Path(os.getenv("STATE_DIR", ".state"))
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE   = STATE_DIR / "tunnel.json"

def _host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""

def _resolve_ip(host: str) -> Optional[str]:
    if not host or not RESOLVE_IP:
        return None
    try:
        infos = socket.getaddrinfo(host, None)
        for fam, _, _, _, sockaddr in infos:
            ip = sockaddr[0]
            if ip:
                return ip
    except Exception:
        return None
    return None

def _headers() -> Dict[str, str]:
    h = {"User-Agent": "Base44TunnelWatchdog/1.0"}
    if RELAY_TOKEN:
        h["Authorization"] = f"Bearer {RELAY_TOKEN}"
        h["x-relay-token"] = RELAY_TOKEN
    return h

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# alert cooldown memory
_last_alert: Dict[str, float] = {}

def _cool_ok(key: str) -> bool:
    t = _last_alert.get(key, 0.0)
    now = time.time()
    if now - t >= COOLDOWN_SEC:
        _last_alert[key] = now
        return True
    return False

def _write_state(obj: Dict[str, Any]):
    try:
        STATE_FILE.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass

def _probe(url_base: str) -> requests.Response:
    url = f"{url_base}{EXPECT_PATH}"
    # HEAD first to avoid unnecessary bodies
    if HEAD_FIRST:
        try:
            r = requests.head(url, headers=_headers(), timeout=8, allow_redirects=ALLOW_REDIR)
            if r.status_code == 405:  # some servers dislike HEAD
                raise requests.RequestException("HEAD not allowed")
            return r
        except Exception:
            pass
    # fallback to GET
    r = requests.get(url, headers=_headers(), timeout=10, allow_redirects=ALLOW_REDIR)
    return r

def main():
    if not PUBLIC_URL:
        print("Set PUBLIC_URL in .env to use Tunnel Watchdog.")
        return

    last_host = _host_of(PUBLIC_URL)
    last_ip   = _resolve_ip(last_host)
    was_down  = None  # tri-state
    fail_streak = 0

    log.info(f"Tunnel Watchdog → poll {PUBLIC_URL}{EXPECT_PATH} every {POLL_SEC}s (host={last_host} ip={last_ip or '-'})")

    # mismatch warning once at start
    if RELAY_URL and RELAY_URL != PUBLIC_URL and _cool_ok("mismatch"):
        tg_send(f"⚠️ PUBLIC_URL and RELAY_URL differ.\nPUBLIC_URL={PUBLIC_URL}\nRELAY_URL={RELAY_URL}", priority="warn")

    while True:
        delay = POLL_SEC
        try:
            cur_host = _host_of(PUBLIC_URL)
            cur_ip   = _resolve_ip(cur_host)

            if cur_host != last_host and _cool_ok("host-change"):
                tg_send(f"🔁 Tunnel host changed:\n{last_host or 'unknown'} → {cur_host}", priority="info")
                last_host = cur_host

            if cur_ip and cur_ip != last_ip and _cool_ok("ip-change"):
                tg_send(f"🛰️ Tunnel IP changed: {last_ip or 'unknown'} → {cur_ip}", priority="info")
                last_ip = cur_ip

            resp = _probe(PUBLIC_URL)
            ok = False
            status = resp.status_code
            body_ok = False

            # consider 200 OK healthy; if JSON with {"ok": true} even better
            if status == 200:
                ok = True
                try:
                    js = resp.json()
                    body_ok = bool(js.get("ok", False)) if isinstance(js, dict) else False
                except Exception:
                    body_ok = False

            # transitions
            if ok:
                if was_down is True and _cool_ok("recovered"):
                    tg_send("✅ Public tunnel recovered", priority="success")
                was_down = False
                fail_streak = 0
            else:
                fail_streak += 1
                if was_down is not True and _cool_ok("down"):
                    reason = f"status={status}"
                    tg_send(f"❌ Public tunnel DOWN ({reason})", priority="error")
                elif fail_streak in (3, 6, 10) and _cool_ok(f"down-{fail_streak}"):
                    # escalating nudges, not a firehose
                    tg_send(f"❌ Public tunnel still down ({fail_streak} consecutive failures).", priority="error")
                was_down = True

            # persist state for dashboards
            _write_state({
                "ts": _now_utc_iso(),
                "public_url": PUBLIC_URL,
                "expect_path": EXPECT_PATH,
                "host": cur_host,
                "ip": cur_ip,
                "status": status,
                "healthy": bool(ok),
                "body_ok": bool(body_ok),
                "fail_streak": fail_streak,
            })

            # console heartbeat
            stamp = datetime.now().strftime("%H:%M:%S")
            log.info(f"[{stamp}] {status} healthy={ok} host={cur_host} ip={cur_ip or '-'}")

            # minor jitter so we don't synchronize with other cron-ish bots
            delay = POLL_SEC + (random.randint(0, max(JITTER_SEC, 0)) if not was_down else 0)

        except KeyboardInterrupt:
            log.info("Tunnel Watchdog stopped by user.")
            break
        except Exception as e:
            fail_streak += 1
            if was_down is not True and _cool_ok("down-ex"):
                tg_send(f"❌ Public tunnel unreachable: {e}", priority="error")
            was_down = True
            log.warning(f"probe error: {e}")
            # exponential backoff while down
            step = BACKOFF_SEQ[min(fail_streak-1, len(BACKOFF_SEQ)-1)] if BACKOFF_SEQ else POLL_SEC
            delay = step

            # state still updates on exception
            try:
                _write_state({
                    "ts": _now_utc_iso(),
                    "public_url": PUBLIC_URL,
                    "expect_path": EXPECT_PATH,
                    "host": _host_of(PUBLIC_URL),
                    "ip": _resolve_ip(_host_of(PUBLIC_URL)) if RESOLVE_IP else None,
                    "status": None,
                    "healthy": False,
                    "body_ok": False,
                    "fail_streak": fail_streak,
                    "error": str(e),
                })
            except Exception:
                pass

        time.sleep(max(1, int(delay)))

if __name__ == "__main__":
    main()
