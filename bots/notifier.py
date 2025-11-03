#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Notifier Bot — Telegram Heartbeat + Relay Status (UTC-safe, loud errors)
"""

import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
RELAY_BASE = os.getenv("RELAY_BASE", "http://127.0.0.1:8080").strip()

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_telegram(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("[notifier/error] Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env")
        return False
    try:
        url = f"{API_BASE}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, data=payload, timeout=15)
        if r.status_code != 200:
            print(f"[notifier/error] Telegram HTTP {r.status_code}: {r.text}")
            return False
        data = r.json()
        if not data.get("ok"):
            print(f"[notifier/error] Telegram responded ok=false: {data}")
            return False
        return True
    except Exception as e:
        print(f"[notifier/error] Telegram send failed: {e}")
        return False

def check_relay():
    try:
        r = requests.get(f"{RELAY_BASE}/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def validate_telegram():
    print("[notifier] Validating Telegram credentials with a test message...")
    ok = send_telegram("🟢 <b>Base44 Notifier online.</b> (credential test)")
    if not ok:
        print(
            "[notifier/help] If you see 'Unauthorized', your bot token is wrong.\n"
            "[notifier/help] If you see 'chat not found' or 'Forbidden', your CHAT_ID is wrong or you never pressed Start.\n"
            "[notifier/help] For groups, CHAT_ID must be negative like -100xxxxxxxxxx.\n"
            "[notifier/help] Fix .env then restart this script."
        )
    return ok

def main():
    if not validate_telegram():
        # Don’t loop forever if creds are bad
        return
    # Heartbeat loop
    while True:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        alive = check_relay()
        if alive:
            print(f"[{ts}] notifier/heartbeat: Relay OK")
        else:
            print(f"[{ts}] notifier/error: Relay unreachable")
            send_telegram("🔴 <b>Relay not responding!</b>")
        time.sleep(30)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        send_telegram("⚫ Base44 Notifier stopped manually.")
        print("Notifier stopped.")
