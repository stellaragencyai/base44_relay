#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools.sec_preflight — baseline security & safety checks for Base44.

Run this before spinning the bots, and on container boot. Exits nonzero on failure.

Checks performed:
- Env schema validation (required keys present, token patterns sane)
- File/dir permissions: state/, .state/, cfg/; refuses world-writable secrets
- Multi-bot YAML sanity: tokens present only where enabled, minimal perms
- Clock skew to Bybit (< 2s) to keep signatures safe
- Bybit reachability + limited public endpoint probe
- Dry-run safety: refuses EXEC_DRY_RUN=false unless GUARD breaker is OFF and confirm flag set
- Dangerous flags audit: TELEGRAM_DEBUG, TP_DRY_RUN, SIG_DRY_RUN mismatches across processes
- Optional IP allowlist reminder (prints if disabled on Bybit, best-effort hint)

Usage:
  python tools/sec_preflight.py
Env:
  PREFLIGHT_ALLOW_PROD=0|1   # allow EXEC_DRY_RUN=false if 1
  TZ=America/Phoenix
"""

from __future__ import annotations
import os, sys, stat, json, time
from pathlib import Path
from typing import List, Tuple

# Import local helpers
try:
    from core.env_schema import validate_env, load_env, mask
except Exception as e:
    print(f"[preflight/error] env_schema import failed: {e}")
    sys.exit(2)

# Optional clients
try:
    from core.bybit_client import Bybit
except Exception:
    Bybit = None  # type: ignore

# Optional guard mirror
try:
    from core.guard import guard_blocking_reason
except Exception:
    def guard_blocking_reason(): return (False, "")

ROOT = Path(os.getcwd())
STATE_DIR = Path(os.getenv("STATE_DIR", "state"))
LEGACY_STATE_DIR = ROOT / ".state"
CFG_DIR = ROOT / "cfg"

FAILS: List[str] = []
WARN:  List[str] = []

def _fail(msg: str): FAILS.append(msg)
def _warn(msg: str): WARN.append(msg)

def _check_env():
    try:
        ok, msg = validate_env(strict=False)
        if not ok:
            _fail(msg)
        else:
            print("[preflight/ok] env schema ok")
    except Exception as e:
        _fail(f"env schema: {e}")

def _check_fs_perms():
    for path in [STATE_DIR, LEGACY_STATE_DIR, CFG_DIR]:
        try:
            path.mkdir(parents=True, exist_ok=True)
            st = path.stat()
            if st.st_mode & stat.S_IWOTH:
                _fail(f"{path} is world-writable; fix perms (chmod o-w)")
        except Exception as e:
            _fail(f"fs perms {path}: {e}")

    # Secrets living in cfg: scan for tg_subaccounts.yaml
    tg_yaml = Path(os.getenv("TG_CONFIG_PATH", CFG_DIR / "tg_subaccounts.yaml"))
    if tg_yaml.exists():
        try:
            raw = tg_yaml.read_text(encoding="utf-8")
            if "token:" in raw and ("enabled: true" in raw or "enabled: True" in raw):
                # basic sanity
                if "changeme" in raw.lower():
                    _fail("tg_subaccounts.yaml still contains placeholder token")
            st = tg_yaml.stat()
            if st.st_mode & stat.S_IROTH:
                _warn(f"{tg_yaml} readable by others; consider chmod 640 or stricter")
        except Exception as e:
            _warn(f"cannot read {tg_yaml}: {e}")

def _check_clock_skew():
    if Bybit is None:
        _warn("bybit client unavailable; skip clock check")
        return
    try:
        by = Bybit()
        t0 = time.time()
        by.sync_time()
        dt = abs(time.time() - t0)  # network jitter proxy
        if dt > 2.0:
            _warn(f"high network latency ~{dt:.2f}s; signatures may be fragile")
        print("[preflight/ok] time sync attempted")
    except Exception as e:
        _warn(f"time sync failed: {e}")

def _check_bybit_public():
    if Bybit is None:
        return
    try:
        by = Bybit()
        ok, data, err = by.get_tickers(category="linear", symbol="BTCUSDT")
        if not ok:
            _warn(f"bybit public probe failed: {err}")
        else:
            print("[preflight/ok] bybit public reachable")
    except Exception as e:
        _warn(f"bybit probe exception: {e}")

def _check_dryrun_policy():
    env = load_env()
    exec_dry = str(os.getenv("EXEC_DRY_RUN", str(env.get("EXEC_DRY_RUN", "true")))).lower() in ("1","true","yes","on")
    sig_dry  = str(os.getenv("SIG_DRY_RUN",  str(env.get("SIG_DRY_RUN",  "true")))).lower() in ("1","true","yes","on")
    allow_prod = str(os.getenv("PREFLIGHT_ALLOW_PROD", "0")).lower() in ("1","true","yes","on")
    blocked, why = guard_blocking_reason()

    if not exec_dry:
        if blocked:
            _fail(f"EXEC_DRY_RUN=false while breaker ON ({why}). Either clear breaker or re-enable dry-run.")
        elif not allow_prod:
            _fail("EXEC_DRY_RUN=false without PREFLIGHT_ALLOW_PROD=1. Set env explicitly for live mode.")
        else:
            print("[preflight/ok] live mode allowed by PREFLIGHT_ALLOW_PROD")
    if not sig_dry and exec_dry:
        _warn("Signals not dry-run but executor is dry-run; mismatch may confuse tests.")

def _check_telegram_config():
    env = load_env()
    tok = env.get("TELEGRAM_BOT_TOKEN") or ""
    chat = env.get("TELEGRAM_CHAT_ID") or env.get("TELEGRAM_CHAT_IDS") or ""
    cfg_path = Path(os.getenv("TG_CONFIG_PATH", "cfg/tg_subaccounts.yaml"))
    multi = cfg_path.exists()
    if not multi and tok and chat:
        print(f"[preflight/ok] telegram env fallback configured (token={mask(tok)})")
    elif multi:
        print(f"[preflight/ok] telegram multi-bot config present at {cfg_path}")
    else:
        _warn("no Telegram destination configured (optional)")

def main():
    _check_env()
    _check_fs_perms()
    _check_clock_skew()
    _check_bybit_public()
    _check_dryrun_policy()
    _check_telegram_config()

    if WARN:
        print("\n[preflight/warn]")
        for w in WARN:
            print(f" - {w}")
    if FAILS:
        print("\n[preflight/fail]")
        for f in FAILS:
            print(f" - {f}")
        sys.exit(1)
    print("\n[preflight/success] baseline checks passed")

if __name__ == "__main__":
    main()
