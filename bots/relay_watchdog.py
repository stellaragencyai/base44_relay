# bots/relay_watchdog.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relay Watchdog (monitor-only)
- Verifies the Base44 Relay is reachable via /health.
- Verifies the Bybit proxy path is working by calling /v5/position/list through the relay.
- Sends Telegram alerts on DOWN → UP and UP → DOWN transitions.
- Prints status every poll.

ENV (in .env):
  HEALTH_POLL_SEC=20       # seconds between checks
  RISK_CATEGORY=linear     # category for positions call
  PNL_SETTLE_COIN=USDT     # settle coin for linear positions call
  RELAY_BASE=...           # already used by core/base44_client
  RELAY_TOKEN=...          # already used by core/base44_client
  TELEGRAM_BOT_TOKEN=...   # optional, for alerts
  TELEGRAM_CHAT_ID=...     # optional, for alerts
"""

import os
import sys
import time
from pathlib import Path

# ── Robust import path: add project root, then import from core package
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.base44_client import relay_get, proxy, tg_send  # type: ignore

POLL_SEC    = int(os.getenv("HEALTH_POLL_SEC", "20"))
CATEGORY    = os.getenv("RISK_CATEGORY", "linear")
SETTLE_COIN = os.getenv("PNL_SETTLE_COIN", "USDT")

def check_relay_health() -> bool:
    """
    Calls the relay /health endpoint.
    Expects a JSON with {"ok": True, ...} from base44_relay.py.
    """
    try:
        resp = relay_get("/health")
        return bool(resp.get("ok", False))
    except Exception:
        return False

def check_bybit_proxy() -> bool:
    """
    Calls Bybit positions through the relay proxy to ensure signing + upstream works.
    Uses category + (for linear) settleCoin parameter.
    """
    try:
        params = {"category": CATEGORY}
        if CATEGORY.lower() == "linear" and SETTLE_COIN:
            params["settleCoin"] = SETTLE_COIN
        body = proxy("GET", "/v5/position/list", params=params)
        # Expect a Bybit-like body with retCode == 0 on success
        return (body or {}).get("retCode") == 0
    except Exception:
        return False

def main():
    print(f"Relay Watchdog running • poll {POLL_SEC}s • category={CATEGORY} • settleCoin={SETTLE_COIN}")
    last_ok = None  # tri-state: None (unknown), True (healthy), False (down)

    while True:
        try:
            ok_relay = check_relay_health()
            ok_bybit = check_bybit_proxy()
            healthy  = ok_relay and ok_bybit

            # Console heartbeat every poll
            print(f"[relay_watchdog] relay_ok={ok_relay} bybit_proxy_ok={ok_bybit}")

            # Transition alerts
            if last_ok is None:
                # First observation: don't spam Telegram, just set baseline.
                pass
            elif healthy and last_ok is False:
                tg_send("✅ Relay Watchdog: recovered — relay and Bybit proxy are healthy.")
            elif (not healthy) and last_ok is True:
                reason = "relay down" if not ok_relay else "bybit proxy failing"
                tg_send(f"❌ Relay Watchdog: DOWN — {reason}.")

            last_ok = healthy
        except Exception as e:
            # If our own code crashes, report and keep going.
            print(f"[relay_watchdog] error: {e}")
            try:
                tg_send(f"⚠️ Relay Watchdog exception: {e}")
            except Exception:
                pass

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
