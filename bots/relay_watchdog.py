#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Relay Watchdog (automation-ready)

What it monitors:
- Relay /health via core.base44_client (auth, same path the bots use)
- Bybit proxy signing via relay: GET /v5/position/list (category + optional settleCoin)
- Optional public tunnel direct check (RELAYS_PUBLIC_URL) for DNS/tunnel issues
- Latency for each check, with warn/crit thresholds

What it can do (all optional, .env-driven):
- Telegram alerts on UP/DOWN transitions (throttled)
- Periodic DOWN reminders (not every poll)
- Flip the global breaker file to pause automation while relay is DOWN
- Call a restart webhook when DOWN persists past an escalation window
- Decision-log events for later diagnostics if core.decision_log exists

ENV (.env):
  # cadence
  HEALTH_POLL_SEC=20

  # relay + bybit proxy
  RISK_CATEGORY=linear
  PNL_SETTLE_COIN=USDT

  # optional public tunnel direct probe (bypasses base44_client)
  RELAY_PUBLIC_URL=                    # e.g. https://xxxx.ngrok-free.dev
  RELAY_PUBLIC_TIMEOUT_SEC=3

  # latency thresholds (milliseconds)
  WD_LATENCY_WARN_MS=450
  WD_LATENCY_CRIT_MS=1200

  # breaker + escalation
  WD_SET_BREAKER=true                  # write .state/risk_state.json breach=true on DOWN
  WD_BREAKER_FILE=.state/risk_state.json
  WD_ESCALATE_AFTER_SEC=180            # after this much continuous DOWN, escalate
  WD_REMIND_EVERY_MIN=5                # DOWN reminder cadence

  # optional restart webhook you control (supervisor/systemd/pm2/etc)
  WD_RESTART_URL=                      # POST to this when escalating
  WD_RESTART_BEARER=                   # optional bearer token for the restart call
  WD_RESTART_JSON={"service":"base44_relay"}  # JSON payload to send

  # Telegram (via core.notifier_bot)
  TELEGRAM_BOT_TOKEN=
  TELEGRAM_CHAT_ID=

Notes:
- Uses core.base44_client.{relay_get, proxy, tg_send}.
- Gracefully no-ops features you don’t configure.
- Never spams: alerts only on transitions + spaced reminders while DOWN.
"""

from __future__ import annotations
import os
import sys
import time
import json
from pathlib import Path
from typing import Optional

# ── import path & soft deps
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.base44_client import relay_get, proxy, tg_send  # type: ignore

try:
    import requests
except Exception:
    requests = None  # public probe becomes a no-op without requests

# decision log is optional
try:
    from core.decision_log import log_event
except Exception:  # pragma: no cover
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        print(f"[DECLOG/{component}/{event}] {payload or {}}")

# ── env
POLL_SEC            = int(os.getenv("HEALTH_POLL_SEC", "20"))
CATEGORY            = os.getenv("RISK_CATEGORY", "linear")
SETTLE_COIN         = os.getenv("PNL_SETTLE_COIN", "USDT")

RELAY_PUBLIC_URL    = (os.getenv("RELAY_PUBLIC_URL") or "").strip()
RELAY_PUBLIC_TIMEOUT= float(os.getenv("RELAY_PUBLIC_TIMEOUT_SEC", "3") or 3)

LAT_WARN_MS         = int(os.getenv("WD_LATENCY_WARN_MS", "450"))
LAT_CRIT_MS         = int(os.getenv("WD_LATENCY_CRIT_MS", "1200"))

SET_BREAKER         = (os.getenv("WD_SET_BREAKER", "true").strip().lower() in {"1","true","yes","on"})
BREAKER_FILE        = Path(os.getenv("WD_BREAKER_FILE", ".state/risk_state.json"))
ESCALATE_AFTER_SEC  = int(os.getenv("WD_ESCALATE_AFTER_SEC", "180"))
REMIND_EVERY_MIN    = int(os.getenv("WD_REMIND_EVERY_MIN", "5"))

RESTART_URL         = (os.getenv("WD_RESTART_URL") or "").strip()
RESTART_BEARER      = (os.getenv("WD_RESTART_BEARER") or "").strip()
RESTART_JSON_RAW    = (os.getenv("WD_RESTART_JSON") or "").strip()

try:
    RESTART_JSON = json.loads(RESTART_JSON_RAW) if RESTART_JSON_RAW else {"service": "base44_relay"}
except Exception:
    RESTART_JSON = {"service": "base44_relay"}

# ── helpers
def _ms_since(t0: float) -> int:
    return int((time.perf_counter() - t0) * 1000)

def _grade_latency(ms: int) -> str:
    if ms >= LAT_CRIT_MS:
        return "crit"
    if ms >= LAT_WARN_MS:
        return "warn"
    return "ok"

def _now_ts() -> int:
    return int(time.time())

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _read_breaker() -> dict:
    try:
        if not BREAKER_FILE.exists():
            return {}
        return json.loads(BREAKER_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_breaker(breach: bool, reason: str):
    if not SET_BREAKER:
        return
    _ensure_parent(BREAKER_FILE)
    data = _read_breaker()
    data.update({
        "breach": bool(breach),
        "source": "relay_watchdog",
        "reason": reason,
        "ts": _now_ts()
    })
    try:
        BREAKER_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[relay_watchdog] breaker write failed: {e}")

def _restart_hook():
    if not RESTART_URL or requests is None:
        return False, "restart not configured"
    headers = {"Content-Type": "application/json"}
    if RESTART_BEARER:
        headers["Authorization"] = f"Bearer {RESTART_BEARER}"
    try:
        r = requests.post(RESTART_URL, json=RESTART_JSON, headers=headers, timeout=5)
        ok = (200 <= r.status_code < 300)
        return ok, f"status={r.status_code}"
    except Exception as e:
        return False, f"ex={e}"

# ── checks
def check_relay_health() -> tuple[bool, int]:
    """
    Calls relay /health; returns (ok, latency_ms)
    """
    t0 = time.perf_counter()
    try:
        resp = relay_get("/health")
        ok = bool(resp.get("ok", False))
    except Exception:
        ok = False
    return ok, _ms_since(t0)

def check_bybit_proxy() -> tuple[bool, int]:
    """
    Calls Bybit positions through relay proxy; returns (ok, latency_ms)
    """
    t0 = time.perf_counter()
    try:
        params = {"category": CATEGORY}
        if CATEGORY.lower() == "linear" and SETTLE_COIN:
            params["settleCoin"] = SETTLE_COIN
        body = proxy("GET", "/v5/position/list", params=params)
        ok = (body or {}).get("retCode") in (0, "0")
    except Exception:
        ok = False
    return ok, _ms_since(t0)

def check_public_direct() -> tuple[Optional[bool], Optional[int]]:
    """
    Optional: probe RELAY_PUBLIC_URL/health directly (bypasses base44_client).
    Returns (ok, latency_ms) or (None, None) if not configured/unavailable.
    """
    if not RELAY_PUBLIC_URL or requests is None:
        return None, None
    url = RELAY_PUBLIC_URL.rstrip("/") + "/health"
    t0 = time.perf_counter()
    try:
        r = requests.get(url, timeout=RELAY_PUBLIC_TIMEOUT)
        ok = r.ok and bool((r.json() if "application/json" in r.headers.get("Content-Type","") else {}).get("ok", False))
    except Exception:
        ok = False
    return ok, _ms_since(t0)

# ── main
def main():
    print(f"Relay Watchdog running • poll {POLL_SEC}s • cat={CATEGORY} • settleCoin={SETTLE_COIN} • breaker={'on' if SET_BREAKER else 'off'}")

    last_state: Optional[bool] = None   # None unknown, True up, False down
    down_since: Optional[float] = None
    last_reminder: float = 0.0
    escalated: bool = False

    while True:
        try:
            ok_health, lat_health = check_relay_health()
            ok_proxy, lat_proxy   = check_bybit_proxy()
            ok_public, lat_public = check_public_direct()

            healthy = ok_health and ok_proxy
            lat_grade = max(_grade_latency(lat_health), _grade_latency(lat_proxy), key=lambda g: {"ok":0,"warn":1,"crit":2}[g])

            # console heartbeat
            pub_str = "-" if ok_public is None else ("OK" if ok_public else "FAIL")
            pub_lat = "-" if lat_public is None else f"{lat_public}ms"
            print(f"[relay_watchdog] relay={ 'OK' if ok_health else 'FAIL' }({lat_health}ms) "
                  f"proxy={ 'OK' if ok_proxy else 'FAIL' }({lat_proxy}ms) "
                  f"public={pub_str}({pub_lat}) grade={lat_grade}")

            # decision log sample
            log_event("watchdog", "probe", "", "MAIN", {
                "relay_ok": ok_health, "proxy_ok": ok_proxy, "public_ok": ok_public,
                "lat_ms": {"health": lat_health, "proxy": lat_proxy, "public": lat_public},
                "grade": lat_grade
            })

            now = time.time()

            # transitions
            if last_state is None:
                # first observation: set breaker if needed but don't spam start message
                if not healthy and SET_BREAKER:
                    _write_breaker(True, "relay_down_init")
                    down_since = now
                last_state = healthy

            elif healthy and last_state is False:
                # recovery
                tg_send("✅ Relay Watchdog: recovered — relay and Bybit proxy healthy.")
                log_event("watchdog", "recovered", "", "MAIN", {"down_for_sec": int(now - (down_since or now))})
                if SET_BREAKER:
                    _write_breaker(False, "recovered")
                last_state = True
                down_since = None
                last_reminder = 0.0
                escalated = False

            elif (not healthy) and last_state is True:
                # just went down
                reason = "relay down" if not ok_health else "bybit proxy failing"
                tg_send(f"❌ Relay Watchdog: DOWN — {reason}.")
                log_event("watchdog", "down", "", "MAIN", {"reason": reason})
                if SET_BREAKER:
                    _write_breaker(True, reason)
                last_state = False
                down_since = now
                last_reminder = 0.0
                escalated = False

            # persistent DOWN path: reminders + escalation
            if last_state is False:
                # spaced reminder
                if REMIND_EVERY_MIN > 0 and (now - last_reminder) >= REMIND_EVERY_MIN * 60:
                    elapsed = int(now - (down_since or now))
                    tg_send(f"⏳ Relay Watchdog: still DOWN ({elapsed}s). Breaker active.", priority="warn")
                    last_reminder = now

                # escalate once after threshold
                if (not escalated) and ESCALATE_AFTER_SEC > 0 and (now - (down_since or now)) >= ESCALATE_AFTER_SEC:
                    ok, info = _restart_hook()
                    if ok:
                        tg_send("🛠️ Relay Watchdog: escalation triggered — restart hook called successfully.")
                        log_event("watchdog", "restart_hook_ok", "", "MAIN", {"info": info})
                    else:
                        tg_send(f"🛠️ Relay Watchdog: escalation attempted — restart hook failed ({info}).", priority="warn")
                        log_event("watchdog", "restart_hook_fail", "", "MAIN", {"info": info})
                    escalated = True

        except Exception as e:
            print(f"[relay_watchdog] error: {e}")
            try:
                tg_send(f"⚠️ Relay Watchdog exception: {e}")
            except Exception:
                pass
            # keep breaker asserted on exceptions if we were already down
            # no state flip here

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
