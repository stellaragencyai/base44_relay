#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/guardctl.py — tiny CLI for breaker control

Usage:
  python tools/guardctl.py status
  python tools/guardctl.py trip "reason text"
  python tools/guardctl.py clear
  python tools/guardctl.py reset [start_equity_usd]
"""
import sys
from core.guard import guard_blocking_reason, guard_trip, guard_clear, guard_reset_session
from core.db import guard_load

def main():
    if len(sys.argv) < 2:
        print(__doc__); return
    cmd = sys.argv[1].lower()
    if cmd == "status":
        st = guard_load()
        blocked, why = guard_blocking_reason()
        print(f"breaker_on={bool(st.get('breaker_on'))} reason='{st.get('breaker_reason','')}' blocked_now={blocked} why='{why}'")
    elif cmd == "trip":
        reason = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else "manual"
        guard_trip(reason)
        print(f"tripped: {reason}")
    elif cmd == "clear":
        guard_clear()
        print("cleared")
    elif cmd == "reset":
        start_eq = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0
        guard_reset_session(start_eq)
        print(f"session reset; start_equity_usd={start_eq}")
    else:
        print(__doc__)

if __name__ == "__main__":
    main()
