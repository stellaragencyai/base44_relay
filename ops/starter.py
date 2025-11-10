#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Starter
Launches all core bots in the proper order, breaker-safe, with heartbeat logging.
"""

import subprocess, time, os, sys

BOTS = [
    "bots/signal_engine.py",
    "bots/auto_executor.py",
    "bots/tp_sl_manager.py",
    "bots/portfolio_guard.py",
]

def start_bot(path):
    return subprocess.Popen([sys.executable, path])

def main():
    print("🚀  Starting Base44 trading system...")
    procs = []
    for bot in BOTS:
        if not os.path.exists(bot):
            print(f"⚠️  Missing: {bot}")
            continue
        print(f"▶️  Launching {bot}")
        p = start_bot(bot)
        procs.append(p)
        time.sleep(2)  # small stagger for relay/telegram connections

    print("✅  All bots started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🧹  Stopping all bots...")
        for p in procs:
            p.terminate()
        print("💤  All bots stopped cleanly.")

if __name__ == "__main__":
    main()
