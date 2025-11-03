# bots/pnl_logger.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from core.env_bootstrap import *  # loads config/.env automatically

"""
Daily PnL Logger (monitor-only)

What it does:
- Polls wallet balance and open positions to log equity & PnL over time.
- Writes CSV to /logs/pnl/ by default. Can rotate daily files.
- Safe to run with open trades (read-only).

Upgrades:
- Bootstrap import so env is loaded and core path is fixed.
- Optional daily rotation: PNL_ROTATE_DAILY=1 → logs/pnl/YYYY-MM-DD.csv
- Telegram alerts (cooldown) on API errors; quiet during startup grace.
- Signature-safe calls to base44_client so minor refactors don’t break it.
- Proper UTC timestamps, resilient float parsing, stable console logs.
"""

import os
import csv
import time
import sys
import importlib
from pathlib import Path
from datetime import datetime, timezone

# --------------------------------------------------------------------------------------
# Locate and import base44_client & preferred tg_send
# --------------------------------------------------------------------------------------
def _import_base44_client():
    env_hint = os.getenv("BASE44_CORE_DIR")
    here = Path(__file__).resolve()
    candidates = []

    if env_hint:
        candidates.append(Path(env_hint))

    repo_root = here.parents[1]   # project root
    bots_dir  = here.parent
    candidates += [repo_root / "core", bots_dir / "core", repo_root, bots_dir]

    tried = []
    for p in candidates:
        try:
            if p and p.exists():
                if str(p) not in sys.path:
                    sys.path.insert(0, str(p))
                return importlib.import_module("base44_client")
            tried.append(str(p))
        except Exception:
            tried.append(str(p))
            continue
    raise ImportError("Unable to import base44_client. Tried:\n  - " + "\n  - ".join(tried))

def _import_tg_send(client_mod):
    # Prefer notifier_bot if present (chunking, retries); otherwise use base44_client.tg_send; else console only.
    try:
        nb = importlib.import_module("notifier_bot")
        if hasattr(nb, "tg_send"):
            return nb.tg_send
    except Exception:
        pass
    if hasattr(client_mod, "tg_send"):
        return client_mod.tg_send

    def _console_only(msg: str, **_):
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{stamp}] {msg}")
    return _console_only

_client = _import_base44_client()
tg_send = _import_tg_send(_client)

# --------------------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------------------
POLL = int(os.getenv("PNL_POLL_SEC", "30"))
ACCOUNT_TYPE = os.getenv("PNL_ACCOUNT_TYPE", "UNIFIED")
CATEGORY = os.getenv("RISK_CATEGORY", "linear")
SETTLE_COIN = os.getenv("PNL_SETTLE_COIN", "USDT")

# Logging paths
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = Path(os.getenv("PNL_LOG_DIR", str(BASE_DIR / "logs" / "pnl")))
LOG_DIR.mkdir(parents=True, exist_ok=True)
ROTATE_DAILY = os.getenv("PNL_ROTATE_DAILY", "0") == "1"
CSV_PATH = LOG_DIR / "daily_pnl_log.csv"  # used if ROTATE_DAILY=0

# Alerts
STARTUP_GRACE_SEC = int(os.getenv("PNL_STARTUP_GRACE_SEC", "10"))
ALERT_COOLDOWN_SEC = int(os.getenv("PNL_ALERT_COOLDOWN_SEC", "120"))

FIELDS = [
    "timestamp", "date", "accountType", "equity", "walletBalance",
    "unrealizedPnl", "realizedPnl", "openSymbols", "positionCount"
]

# --------------------------------------------------------------------------------------
# Safe wrappers around client calls so different signatures won't explode
# --------------------------------------------------------------------------------------
def _safe_get_wallet_balance(account_type: str, coin: str):
    fn = getattr(_client, "get_wallet_balance", None)
    if fn is None:
        raise RuntimeError("base44_client.get_wallet_balance not found")

    for attempt in (
        lambda: fn(accountType=account_type, coin=coin),
        lambda: fn(accountType=account_type),
        lambda: fn(account_type),
        lambda: fn(),
    ):
        try:
            return attempt()
        except TypeError:
            continue
    raise RuntimeError("get_wallet_balance signature mismatch. Update base44_client or adjust logger.")

def _safe_get_positions(category: str, settle_coin: str):
    fn = getattr(_client, "get_positions", None)
    if fn is None:
        raise RuntimeError("base44_client.get_positions not found")

    for attempt in (
        lambda: fn(category=category, settleCoin=settle_coin),
        lambda: fn(category=category),
        lambda: fn(settleCoin=settle_coin),
        lambda: fn(),
    ):
        try:
            return attempt()
        except TypeError:
            continue
    raise RuntimeError("get_positions signature mismatch. Update base44_client or adjust logger.")

# --------------------------------------------------------------------------------------
# Core helpers
# --------------------------------------------------------------------------------------
def _as_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def _get_equity_tuple():
    """
    Returns (total_equity, wallet_balance, unrealized_pnl)
    """
    body = _safe_get_wallet_balance(ACCOUNT_TYPE, SETTLE_COIN)
    if not isinstance(body, dict) or (body.get("retCode") not in (0, "0")):
        rc = None if not isinstance(body, dict) else body.get("retCode")
        msg = None if not isinstance(body, dict) else body.get("retMsg")
        raise RuntimeError(f"Bybit retCode={rc} retMsg={msg}")

    lst = ((body.get("result") or {}).get("list")) or []
    if not lst:
        return (0.0, 0.0, 0.0)

    r = lst[0]
    total_equity  = _as_float(r.get("totalEquity"))
    wallet_bal    = _as_float(r.get("walletBalance"))
    unrealized    = _as_float(r.get("unrealisedPnl"))
    return (total_equity, wallet_bal, unrealized)

def _get_open_symbols():
    body = _safe_get_positions(CATEGORY, SETTLE_COIN)
    if not isinstance(body, dict) or (body.get("retCode") not in (0, "0")):
        rc = None if not isinstance(body, dict) else body.get("retCode")
        msg = None if not isinstance(body, dict) else body.get("retMsg")
        raise RuntimeError(f"Bybit retCode={rc} retMsg={msg}")

    lst = ((body.get("result") or {}).get("list")) or []
    syms = []
    for p in lst:
        try:
            if _as_float(p.get("size")) > 0:
                sym = p.get("symbol") or ""
                if sym:
                    syms.append(sym)
        except Exception:
            continue
    return syms

def _csv_path_for(ts: datetime):
    if not ROTATE_DAILY:
        return CSV_PATH
    return LOG_DIR / f"{ts.strftime('%Y-%m-%d')}.csv"

def _append_row(path: Path, row):
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            w.writeheader()
        w.writerow(row)
        f.flush()

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    print(f"PnL Logger running • poll {POLL}s • dir → {LOG_DIR} • rotate_daily={int(ROTATE_DAILY)}")

    boot_ts = time.time()
    last_alert = 0.0

    while True:
        try:
            ts = datetime.now(timezone.utc)  # timezone-aware UTC
            date = ts.strftime("%Y-%m-%d")

            eq, bal, unrl = _get_equity_tuple()
            syms = _get_open_symbols()

            row = {
                "timestamp": ts.isoformat(),
                "date": date,
                "accountType": ACCOUNT_TYPE,
                "equity": f"{eq:.4f}",
                "walletBalance": f"{bal:.4f}",
                "unrealizedPnl": f"{unrl:.4f}",
                "realizedPnl": "",  # left blank unless you wire in realized PnL source
                "openSymbols": ",".join(syms),
                "positionCount": len(syms),
            }

            path = _csv_path_for(ts)
            _append_row(path, row)

            print(f"[{date}] eq={eq:.4f} bal={bal:.4f} unrl={unrl:.4f} pos={len(syms)} → {path.name}")
            time.sleep(POLL)

        except KeyboardInterrupt:
            print("PnL Logger stopped by user.")
            break
        except Exception as e:
            # Notify, but not every millisecond
            now = time.time()
            in_grace = (now - boot_ts) < STARTUP_GRACE_SEC
            if not in_grace and (now - last_alert) >= ALERT_COOLDOWN_SEC:
                tg_send(f"⚠️ PnL logger error: {e}", priority="warn")
                last_alert = now
            print(f"[pnl_logger] error: {e}")
            time.sleep(POLL)

if __name__ == "__main__":
    main()
