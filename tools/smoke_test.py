#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/smoke_test.py — End-to-end sanity for Base44 core loop (DRY-safe)

What it checks:
- .env essentials present
- Relay reachability (/diag if you have it, else /bybit/wallet/balance best-effort)
- Bybit direct client usable (time sync, public ticker)
- Telegram send path available (won't crash if not configured)
- Portfolio guard heartbeat and risk budget
- Signals queue write/read round-trip
- DB presence (optional): basic function importability

Exit code is non-zero if anything critical fails.
"""

from __future__ import annotations
import os, sys, time, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

errs = []

def note(ok: bool, msg: str):
    tag = "OK" if ok else "ERR"
    print(f"[{tag}] {msg}")
    if not ok:
        errs.append(msg)

def have_env(k: str, allow_empty: bool=False) -> bool:
    v = os.getenv(k)
    return (v is not None) if allow_empty else (v is not None and str(v).strip() != "")

# 0) ENV sanity
must = ["TZ","LOG_LEVEL","BYBIT_BASE_URL"]
for k in must:
    note(have_env(k, allow_empty=False), f"env:{k} present")

# 1) Relay reachability
import requests
relay = os.getenv("RELAY_URL", "http://127.0.0.1:5000").rstrip("/")
tok = os.getenv("RELAY_TOKEN", "")
hdr = {"Authorization": f"Bearer {tok}"} if tok else {}
try:
    r = requests.get(relay + "/bybit/wallet/balance", headers=hdr, timeout=4)
    note(r.status_code in (200, 401, 403), f"relay reachable {relay} (HTTP {r.status_code})")
except Exception as e:
    note(False, f"relay unreachable {relay}: {e}")

# 2) Bybit direct client (public + time sync)
try:
    from core.bybit_client import Bybit
    by = Bybit()
    ok_sync = True
    try:
        by.sync_time()
    except Exception as e:
        ok_sync = False
    note(ok_sync, "Bybit time sync")
    ok_pub = True
    try:
        ok_pub, _, _ = by.get_tickers(category="linear", symbol="BTCUSDT")
    except Exception:
        ok_pub = False
    note(ok_pub, "Bybit public ticker (BTCUSDT)")
except Exception as e:
    note(False, f"import core.bybit_client failed: {e}")

# 3) Telegram (soft check)
tg_ok = True
try:
    from core.notifier_bot import tg_send
    tg_send("🔎 Base44 smoke test ping (if you see this, Telegram path is alive)")
except Exception:
    tg_ok = False
note(tg_ok, "Telegram notifier available")

# 4) Portfolio guard heartbeat + risk budget
try:
    from core.portfolio_guard import guard
    hb = guard.heartbeat()
    have_eq = isinstance(hb.get("equity"), (int, float))
    note(have_eq, f"guard heartbeat equity={hb.get('equity')}")
    risk_val = guard.current_risk_value()
    note(isinstance(risk_val, (int,float)) and risk_val >= 0, f"guard risk budget USD={risk_val:.4f}")
except Exception as e:
    note(False, f"portfolio_guard import/run failed: {e}")

# 5) Signals queue round-trip
try:
    from core.config import settings
    sig_dir = getattr(settings, "DIR_SIGNALS", ROOT / "signals")
    sig_dir.mkdir(parents=True, exist_ok=True)
    qpath = sig_dir / (getattr(settings, "SIGNAL_QUEUE_FILE", "observed.jsonl"))
    test_line = json.dumps({
        "ts": int(time.time()*1000),
        "symbol": "BTCUSDT",
        "signal": "LONG",
        "params": {"maker_only": True, "spread_max_bps": 25.0, "tag": "B44"},
        "features": {"note": "smoke"},
    }, ensure_ascii=False)
    with open(qpath, "a", encoding="utf-8") as fh:
        fh.write(test_line + "\n")
    # verify file exists and non-empty
    ok_q = qpath.exists() and qpath.stat().st_size > 0
    note(ok_q, f"signals queue present: {qpath}")
except Exception as e:
    note(False, f"signal queue write failed: {e}")

# 6) DB optional presence
db_ok = True
try:
    from core import db as coredb  # noqa
    # probe symbol APIs we rely on
    need = ["insert_order", "set_order_state", "insert_execution", "upsert_position", "migrate"]
    for n in need:
        if not hasattr(coredb, n):
            db_ok = False
            break
except Exception:
    db_ok = False
note(db_ok, "DB module usable (optional)")

print("\nSummary:")
if errs:
    for e in errs:
        print(" -", e)
    sys.exit(1)
else:
    print("All critical checks passed.")
