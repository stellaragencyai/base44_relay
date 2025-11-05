# bots/pnl_logger.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from core.env_bootstrap import *  # loads config/.env automatically

"""
Daily PnL Logger (relay-aware, monitor-only)

What it does now:
- Verifies your RELAY_URL is reachable (/diag/time) with RELAY_TOKEN before logging.
- Polls wallet balance (UNIFIED) and open positions across multiple settle coins (USDT, USDC by default).
- Writes CSV to /logs/pnl/ with optional daily rotation.
- Telegram alerts on errors with cooldown, startup grace.
- Signature-safe wrappers for base44_client so minor refactors don’t break it.

Env knobs (via .env or process env):
  PNL_POLL_SEC=30
  PNL_ACCOUNT_TYPE=UNIFIED
  RISK_CATEGORY=linear
  PNL_SETTLE_COINS=USDT,USDC
  PNL_LOG_DIR=logs/pnl
  PNL_ROTATE_DAILY=0   # set 1 for YYYY-MM-DD.csv rotation
  PNL_STARTUP_GRACE_SEC=10
  PNL_ALERT_COOLDOWN_SEC=120
  RELAY_URL=https://<your-ngrok-domain>
  RELAY_TOKEN=...
"""

import os
import csv
import time
import sys
import importlib
import requests
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
        p = Path(env_hint)
        if p.exists():
            candidates.append(p)

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
    # Prefer notifier_bot if present; otherwise base44_client.tg_send; else console only.
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
SETTLE_COINS = [c.strip().upper() for c in os.getenv("PNL_SETTLE_COINS", "USDT,USDC").split(",") if c.strip()]

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

# Relay sanity (so we stop calling 127.0.0.1:8080 like it’s 2012)
RELAY_URL = (os.getenv("RELAY_URL", "") or os.getenv("RELAY_BASE_URL", "")).rstrip("/")
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "") or os.getenv("RELAY_SECRET", "")

def _relay_headers():
    h = {"Content-Type": "application/json"}
    if RELAY_TOKEN:
        h["Authorization"] = f"Bearer {RELAY_TOKEN}"
        h["x-relay-token"] = RELAY_TOKEN
    return h

def _probe_relay():
    if not RELAY_URL:
        raise SystemExit("RELAY_URL missing in .env (expected your ngrok https URL).")
    try:
        r = requests.get(f"{RELAY_URL}/diag/time", headers=_relay_headers(), timeout=8)
        r.raise_for_status()
    except Exception as e:
        raise SystemExit(f"Relay not reachable at {RELAY_URL} → {e}")

# --------------------------------------------------------------------------------------
# Safe wrappers around client calls so different signatures won't explode
# --------------------------------------------------------------------------------------
def _safe_get_wallet_balance(account_type: str, coin: str):
    fn = getattr(_client, "get_wallet_balance", None)
    if fn is None:
        # Try alternative name used in some Base44 builds
        fn = getattr(_client, "get_balance_unified", None)
        if fn is None:
            raise RuntimeError("base44_client.get_wallet_balance/get_balance_unified not found")

    for attempt in (
        (lambda: fn(accountType=account_type, coin=coin)),
        (lambda: fn(accountType=account_type)),
        (lambda: fn(account_type)),
        (lambda: fn()),
    ):
        try:
            return attempt()
        except TypeError:
            continue
    raise RuntimeError("get_wallet_balance signature mismatch. Update base44_client or adjust logger.")

def _safe_get_positions(category: str, settle_coin: str):
    # Try generic name, then specific linear helper
    fn = getattr(_client, "get_positions", None)
    if fn is None:
        fn = getattr(_client, "get_positions_linear", None)
        if fn is None:
            raise RuntimeError("base44_client.get_positions/get_positions_linear not found")

    for attempt in (
        (lambda: fn(category=category, settleCoin=settle_coin)),
        (lambda: fn(category=category)),
        (lambda: fn(settleCoin=settle_coin)),
        (lambda: fn()),
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
    body = _safe_get_wallet_balance(ACCOUNT_TYPE, "USDT")
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
    syms = set()
    for coin in SETTLE_COINS:
        body = _safe_get_positions(CATEGORY, coin)
        if not isinstance(body, dict) or (body.get("retCode") not in (0, "0")):
            rc = None if not isinstance(body, dict) else body.get("retCode")
            msg = None if not isinstance(body, dict) else body.get("retMsg")
            raise RuntimeError(f"Bybit retCode={rc} retMsg={msg}")
        lst = ((body.get("result") or {}).get("list")) or []
        for p in lst:
            try:
                if _as_float(p.get("size")) > 0:
                    sym = p.get("symbol") or ""
                    if sym:
                        syms.add(sym)
            except Exception:
                continue
    return sorted(list(syms))

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

    # Fail fast if relay is wrong so we don’t silently log garbage
    _probe_relay()

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
                "realizedPnl": "",  # placeholder until fills parser is wired
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
