#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tunnel Watchdog
- Polls a known public endpoint (e.g. your ngrok URL /health)
- Warns if hostname changes or goes offline, prints current public URL
Env:
  PUBLIC_URL=https://<your-ngrok>.ngrok-free.dev
  TW_POLL_SEC=30
"""

import os, time, urllib.parse, requests
from datetime import datetime
from dotenv import load_dotenv
from base44_client import tg_send

load_dotenv()
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
POLL = int(os.getenv("TW_POLL_SEC", "30"))

def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""

def main():
    if not PUBLIC_URL:
        print("Set PUBLIC_URL in .env to use Tunnel Watchdog.")
        return
    last_host = host_of(PUBLIC_URL)
    print(f"Tunnel Watchdog → {PUBLIC_URL}/health every {POLL}s (host: {last_host})")

    down = False
    while True:
        try:
            r = requests.get(f"{PUBLIC_URL}/health", timeout=10)
            cur_host = host_of(PUBLIC_URL)
            if cur_host != last_host:
                tg_send(f"🔁 Public tunnel host changed:\n{last_host} → {cur_host}")
                last_host = cur_host
            if r.status_code != 200:
                if not down:
                    tg_send(f"❌ Public /health not 200 (got {r.status_code})")
                down = True
            else:
                if down:
                    tg_send("✅ Public tunnel recovered")
                down = False
            ts = datetime.utcnow().strftime("%H:%M:%S")
            print(f"[{ts}] {r.status_code} host={cur_host}")
        except Exception as e:
            if not down:
                tg_send(f"❌ Public tunnel unreachable: {e}")
            down = True
            print(f"ERROR: {e}")
        time.sleep(POLL)

if __name__ == "__main__":
    main()
