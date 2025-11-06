#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Daily PnL Logger (automation-ready, relay-aware)

What it does
- Verifies your RELAY_URL is reachable (/diag/time) with RELAY_TOKEN before logging.
- Polls unified wallet and open positions across configurable settle coins.
- Writes CSV to logs/pnl/, optional daily rotation.
- Persists baselines/snapshots under .state/ for realized PnL and drawdown math.
- Sends Telegram alerts on errors (cooldown), equity drawdown (warn/breaker), and summaries (hourly/daily).
- Emits decision-log events if core.decision_log exists.

Env knobs (.env)
  PNL_POLL_SEC=30
  PNL_ACCOUNT_TYPE=UNIFIED
  RISK_CATEGORY=linear
  PNL_SETTLE_COINS=USDT,USDC

  PNL_LOG_DIR=logs/pnl
  PNL_ROTATE_DAILY=0            # set 1 for YYYY-MM-DD.csv rotation

  PNL_STARTUP_GRACE_SEC=10
  PNL_ALERT_COOLDOWN_SEC=120

  # Drawdown handling
  PNL_DD_WARN_PCT=3.0
  PNL_DD_BREAKER_PCT=5.0
  WD_SET_BREAKER=true
  WD_BREAKER_FILE=.state/risk_state.json

  # Summaries
  PNL_SEND_HOURLY=true
  PNL_SEND_DAILY=true
  PNL_DAILY_SEND_HOUR=23        # local hour to send daily summary (America/Phoenix)

  # Relay (ngrok or local)
  RELAY_URL=https://<your-ngrok-domain>
  RELAY_TOKEN=...

Notes
- Uses core/base44_client via safe wrappers (signature tolerant).
- Realized PnL is approximated as ΔEquity - ΔUnrealized across polls.
"""

from core.env_bootstrap import *  # loads config/.env automatically

import os
import csv
import json
import time
import sys
import importlib
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# --------------------------------------------------------------------------------------
# Locate and import base44_client & notifier; decision_log is optional
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

try:
    from core.decision_log import log_event
except Exception:  # soft dependency
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        print(f"[DECLOG/{component}/{event}] {payload or {}}")

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

# Alerts & summaries
STARTUP_GRACE_SEC   = int(os.getenv("PNL_STARTUP_GRACE_SEC", "10"))
ALERT_COOLDOWN_SEC  = int(os.getenv("PNL_ALERT_COOLDOWN_SEC", "120"))
PNL_SEND_HOURLY     = (os.getenv("PNL_SEND_HOURLY", "true").strip().lower() in {"1","true","yes","on"})
PNL_SEND_DAILY      = (os.getenv("PNL_SEND_DAILY", "true").strip().lower() in {"1","true","yes","on"})
PNL_DAILY_SEND_HOUR = int(os.getenv("PNL_DAILY_SEND_HOUR", "23"))

# Drawdown handling
PNL_DD_WARN_PCT    = float(os.getenv("PNL_DD_WARN_PCT", "3.0"))
PNL_DD_BREAKER_PCT = float(os.getenv("PNL_DD_BREAKER_PCT", "5.0"))
SET_BREAKER        = (os.getenv("WD_SET_BREAKER", "true").strip().lower() in {"1","true","yes","on"})
BREAKER_FILE       = Path(os.getenv("WD_BREAKER_FILE", ".state/risk_state.json"))

# Timezone for "daily hour" is America/Phoenix from your .env bootstrap
TZ_LOCAL = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"

FIELDS = [
    "timestamp", "date", "accountType", "equity", "walletBalance",
    "unrealizedPnl", "realizedPnl", "openSymbols", "positionCount"
]

# Relay sanity
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
        raise SystemExit("RELAY_URL missing in .env (expected your https tunnel URL).")
    try:
        r = requests.get(f"{RELAY_URL}/diag/time", headers=_relay_headers(), timeout=8)
        r.raise_for_status()
    except Exception as e:
        raise SystemExit(f"Relay not reachable at {RELAY_URL} → {e}")

# --------------------------------------------------------------------------------------
# Safe wrappers around client calls
# --------------------------------------------------------------------------------------
def _safe_get_wallet_balance(account_type: str, coin: str):
    fn = getattr(_client, "get_wallet_balance", None) or getattr(_client, "get_balance_unified", None)
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
    fn = getattr(_client, "get_positions", None) or getattr(_client, "get_positions_linear", None)
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
# Helpers
# --------------------------------------------------------------------------------------
STATE_DIR   = BASE_DIR / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
SNAP_FILE   = STATE_DIR / "pnl_snap.json"         # last eq/unrl for realized calc
BASELINE_FN = STATE_DIR / "wallet_baseline.json"  # session baseline
DAILY_FLAG  = STATE_DIR / "pnl_daily_sent.json"   # day marker for daily summary
HOURLY_FLAG = STATE_DIR / "pnl_hourly_sent.json"  # last hour marker

def _as_float(x) -> float:
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

def _append_row(path: Path, row: Dict[str, Any]):
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            w.writeheader()
        w.writerow(row)
        f.flush()

def _read_json(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _write_json(path: Path, obj: dict):
    try:
        path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass

def _set_breaker(breach: bool, reason: str):
    if not SET_BREAKER:
        return
    data = _read_json(BREAKER_FILE) or {}
    data.update({"breach": bool(breach), "source": "pnl_logger", "reason": reason, "ts": int(time.time())})
    _write_json(BREAKER_FILE, data)

def _phoenix_now():
    # crude local-wallclock hour logic without external tz libs (env bootstrap already set TZ)
    return datetime.now()

def _maybe_hourly_summary(now_local, eq: float, bal: float, unrl: float):
    if not PNL_SEND_HOURLY:
        return
    hour_key = now_local.strftime("%Y-%m-%d %H")
    sent = _read_json(HOURLY_FLAG) or {}
    if sent.get("hour") == hour_key:
        return
    tg_send(f"⌛ Hourly PnL • eq={eq:.2f} • bal={bal:.2f} • unrl={unrl:.2f} • open={','.join(_get_open_symbols()) or '-'}")
    _write_json(HOURLY_FLAG, {"hour": hour_key})
    log_event("pnl", "hourly_summary", "", "MAIN", {"eq":eq,"bal":bal,"unrl":unrl,"hour":hour_key})

def _maybe_daily_summary(now_local, eq_open: float, eq: float):
    if not PNL_SEND_DAILY:
        return
    day_key = now_local.strftime("%Y-%m-%d")
    sent = _read_json(DAILY_FLAG) or {}
    if sent.get("day") == day_key:
        return
    if now_local.hour != PNL_DAILY_SEND_HOUR:
        return
    change = eq - eq_open
    emoji = "🟢" if change >= 0 else "🔴"
    tg_send(f"{emoji} Daily PnL • {day_key} • start={eq_open:.2f} → end={eq:.2f} • Δ={change:.2f}")
    _write_json(DAILY_FLAG, {"day": day_key})
    log_event("pnl", "daily_summary", "", "MAIN", {"start":eq_open,"end":eq,"delta":change,"day":day_key})

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    print(f"PnL Logger running • poll {POLL}s • dir → {LOG_DIR} • rotate_daily={int(ROTATE_DAILY)}")

    # Fail fast if relay is wrong so we don’t silently log garbage
    _probe_relay()

    boot_ts = time.time()
    last_alert = 0.0

    # Initialize baseline for session and daily open equity
    snap = _read_json(SNAP_FILE) or {}
    baseline = _read_json(BASELINE_FN) or {}
    eq_daily_open = float(baseline.get("totalEquity") or 0.0)
    if eq_daily_open <= 0:
        # seed at first successful poll later
        eq_daily_open = 0.0

    prev_eq  = float(snap.get("eq") or 0.0)
    prev_unr = float(snap.get("unrl") or 0.0)

    while True:
        try:
            ts_utc = datetime.now(timezone.utc)  # timezone-aware UTC
            date = ts_utc.strftime("%Y-%m-%d")

            eq, bal, unrl = _get_equity_tuple()
            syms = _get_open_symbols()

            # seed daily open the first time we see equity for the day
            now_local = _phoenix_now()
            day_key = now_local.strftime("%Y-%m-%d")
            baseline_today = _read_json(BASELINE_FN) or {}
            if (baseline_today.get("day") != day_key) and eq > 0:
                eq_daily_open = eq
                _write_json(BASELINE_FN, {"day": day_key, "t": ts_utc.isoformat(), "totalEquity": eq})

            # realized change approximation
            realized = ""
            if prev_eq > 0 or prev_unr != 0:
                realized_delta = (eq - prev_eq) - (unrl - prev_unr)
                realized = f"{realized_delta:.4f}"

            # write CSV
            row = {
                "timestamp": ts_utc.isoformat(),
                "date": date,
                "accountType": ACCOUNT_TYPE,
                "equity": f"{eq:.4f}",
                "walletBalance": f"{bal:.4f}",
                "unrealizedPnl": f"{unrl:.4f}",
                "realizedPnl": realized,
                "openSymbols": ",".join(syms),
                "positionCount": len(syms),
            }
            path = _csv_path_for(ts_utc)
            _append_row(path, row)

            # persist snapshot for next iteration
            _write_json(SNAP_FILE, {"t": ts_utc.isoformat(), "eq": eq, "unrl": unrl})

            # drawdown checks against daily open
            if eq_daily_open > 0:
                dd_pct = 100.0 * max(0.0, (eq_daily_open - eq)) / eq_daily_open
                if dd_pct >= PNL_DD_BREAKER_PCT:
                    tg_send(f"⛔ PnL: equity drawdown {dd_pct:.2f}% ≥ {PNL_DD_BREAKER_PCT:.2f}% — breaker asserted.")
                    _set_breaker(True, f"pnl_dd {dd_pct:.2f}%")
                    log_event("pnl", "dd_breaker", "", "MAIN", {"dd_pct": dd_pct, "open": eq_daily_open, "eq": eq})
                elif dd_pct >= PNL_DD_WARN_PCT:
                    tg_send(f"⚠️ PnL: equity drawdown {dd_pct:.2f}% (start {eq_daily_open:.2f} → now {eq:.2f}).")
                    log_event("pnl", "dd_warn", "", "MAIN", {"dd_pct": dd_pct, "open": eq_daily_open, "eq": eq})

            # summaries
            _maybe_hourly_summary(now_local, eq, bal, unrl)
            _maybe_daily_summary(now_local, eq_daily_open or eq, eq)

            print(f"[{date}] eq={eq:.4f} bal={bal:.4f} unrl={unrl:.4f} pos={len(syms)} → {path.name}")

            # set previous after emitting
            prev_eq, prev_unr = eq, unrl

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
                log_event("pnl", "error", "", "MAIN", {"err": str(e)})
                last_alert = now
            print(f"[pnl_logger] error: {e}")
            time.sleep(POLL)

if __name__ == "__main__":
    main()
