#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots/alloc_daemon.py — Periodic profit allocator runner with heartbeats.
"""

from __future__ import annotations
import os, time
from core.logger import get_logger
from core.decision_log import log_event
from tools.notifier_telegram import tg
from core.profit_allocator import run_once

log = get_logger("bots.alloc_daemon")
INTERVAL = int(os.getenv("PROFIT_ALLOC_INTERVAL_SEC", "300") or "300")

def main():
    tg.safe_text(f"🟢 Profit Allocator online • every {INTERVAL}s", quiet=True)
    log_event("allocator","startup","", "", {"interval": INTERVAL})
    while True:
        t0 = time.time()
        try:
            run_once()
            log_event("allocator","tick_done","", "", {})
        except Exception as e:
            log.exception("allocation error: %s", e)
            tg.safe_text(f"❌ Allocator error: {e}", quiet=True)
            log_event("allocator","tick_error","", "", {"err": str(e)}, level="error")
        slept = max(1, INTERVAL - int(time.time() - t0))
        time.sleep(slept)

if __name__ == "__main__":
    main()
