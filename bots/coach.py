#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Milestone Coach (master or per-subaccount; monitor-only)

What it does
- Watches total equity and announces level-ups across a milestone ladder.
- Tracks best equity (peak) and optional drawdown alerts off the peak.
- Sends Telegram via core.notifier_bot.tg_send (if available) else core.base44_client.tg_send.
- Can target:
    • MASTER account (default), or
    • A specific SUB-ACCOUNT by subUid OR memberId (COACH_SUB_UID / COACH_MEMBER_ID)
- Reads registry/sub_map.json to pretty-print sub name/role/tier when available.
- Keeps separate state files per scope (master vs each sub).
- Emits structured events to core/decision_log (if present).

ENV (optional):
  COACH_POLL_SEC=30
  COACH_ACCOUNT_TYPE=UNIFIED
  COACH_SUB_UID=              # preferred for subs
  COACH_MEMBER_ID=            # legacy alias
  COACH_NAME=
  COACH_TIER_LABEL=
  STATE_DIR=.state
  COACH_DRAWDOWN_PCT=12
  COACH_DRAWDOWN_COOLDOWN_MIN=60
  COACH_ANNOUNCE_MIN_GAP_SEC=5
  TELEGRAM_SILENT=0
"""

from __future__ import annotations

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Tuple

# ── Robust import: add project root, then import from core package
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Prefer rich notifier if present; else fallback to base44_client
def _resolve_notifier():
    try:
        from core.notifier_bot import tg_send as _tg  # type: ignore
        return _tg
    except Exception:
        pass
    try:
        from core.base44_client import tg_send as _tg  # type: ignore
        return _tg
    except Exception:
        def _console_only(msg: str, **_):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"[{ts}] {msg}")
        return _console_only

tg_send = _resolve_notifier()

# Data helpers
from core.base44_client import (  # type: ignore
    get_wallet_balance
)

# Decision log if available
def _resolve_decision_logger():
    try:
        from core.decision_log import log_event as _log  # type: ignore
        return _log
    except Exception:
        def _noop(*_a, **_k):  # never crash if missing
            return None
        return _noop

log_event = _resolve_decision_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Config / State
# ──────────────────────────────────────────────────────────────────────────────
POLL = int(os.getenv("COACH_POLL_SEC", "30"))
ACCOUNT_TYPE = os.getenv("COACH_ACCOUNT_TYPE", "UNIFIED")

SUB_UID = (os.getenv("COACH_SUB_UID") or "").strip()
# Legacy alias support
MEMBER_ID = (os.getenv("COACH_MEMBER_ID") or "").strip()
if SUB_UID and MEMBER_ID:
    # prefer explicit subUid when both are set
    MEMBER_ID = ""

NAME_OVERRIDE = (os.getenv("COACH_NAME") or "").strip()
TIER_OVERRIDE = (os.getenv("COACH_TIER_LABEL") or "").strip()

STATE_DIR = Path(os.getenv("STATE_DIR", ".state"))
STATE_DIR.mkdir(parents=True, exist_ok=True)
scope_key = f"sub_{SUB_UID}" if SUB_UID else (f"member_{MEMBER_ID}" if MEMBER_ID else "master")
STATE_PATH = STATE_DIR / f"coach_state_{scope_key}.json"

DRAWDOWN_PCT = float(os.getenv("COACH_DRAWDOWN_PCT", "12"))
DRAWDOWN_COOLDOWN_MIN = int(os.getenv("COACH_DRAWDOWN_COOLDOWN_MIN", "60"))
ANNOUNCE_MIN_GAP_SEC = int(os.getenv("COACH_ANNOUNCE_MIN_GAP_SEC", "5"))
TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "0") == "1"

# Milestones + tiers (leave as-is so your ladder matches the plan)
LEVELS = [
    (1,     25.0,   "Tier 1"),
    (2,     50.0,   "Tier 1"),
    (3,    100.0,   "Tier 1"),
    (4,    250.0,   "Tier 1"),
    (5,    500.0,   "Tier 2"),
    (6,   1000.0,   "Tier 2"),
    (7,   2500.0,   "Tier 2"),
    (8,   5000.0,   "Tier 3"),
    (9,  10000.0,   "Tier 3"),
    (10, 25000.0,   "Tier 3"),
]
LEVEL_TARGETS = {lvl: tgt for (lvl, tgt, _tier) in LEVELS}

REGISTRY_DIR = PROJECT_ROOT / "registry"
SUB_CSV = (REGISTRY_DIR / "sub_uids.csv")
SUB_MAP = (REGISTRY_DIR / "sub_map.json")  # may be string map or rich object map

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_tg_send(text: str) -> None:
    if TELEGRAM_SILENT:
        print(f"[coach→telegram muted] {text}")
        return
    try:
        tg_send(text)
    except Exception as e:
        print(f"[coach] telegram send failed: {e}\n{text}")

def _read_sub_meta() -> dict:
    """
    Returns mapping for sub display:
      - If sub_map.json is { "302..": "Vehicle", ... } → value is str
      - If it's { "subs": { "302..": {name, role, tier, flags} } } → value is object
      - If it's { "302..": {name, role, ...} } → value is object
    """
    name_map: dict = {}
    try:
        # Try to read rich map first
        if SUB_MAP.exists():
            raw = json.loads(SUB_MAP.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and "subs" in raw and isinstance(raw["subs"], dict):
                for uid, rec in raw["subs"].items():
                    name_map[str(uid)] = rec
            elif isinstance(raw, dict):
                for uid, rec in raw.items():
                    name_map[str(uid)] = rec
        # Fallback: CSV-only adds keys with uid as display name
        if SUB_CSV.exists():
            for line in SUB_CSV.read_text(encoding="utf-8").splitlines()[1:]:
                parts = [p.strip() for p in line.split(",")]
                if parts and parts[0]:
                    name_map.setdefault(parts[0], parts[0])
    except Exception:
        pass
    return name_map

def _display_for_member(member_id: str, name_map: dict) -> Tuple[str, str]:
    if NAME_OVERRIDE:
        nm = NAME_OVERRIDE
    else:
        val = name_map.get(member_id, "")
        if isinstance(val, dict):
            nm = val.get("name") or val.get("label") or str(member_id)
        elif isinstance(val, str) and val.strip():
            nm = val.strip()
        else:
            nm = str(member_id)
    if TIER_OVERRIDE:
        th = TIER_OVERRIDE
    else:
        val = name_map.get(member_id, {})
        th = val.get("tier") if isinstance(val, dict) else ""
    return nm, th

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"current_level": 0, "best_equity": 0.0, "last_levelup_ts": "", "last_drawdown_ts": ""}

def save_state(st: dict) -> None:
    STATE_PATH.write_text(json.dumps(st, indent=2), encoding="utf-8")

def read_equity(account_type: str, sub_uid: str | None, member_id: str | None) -> float:
    """
    Uses base44_client.get_wallet_balance with optional subUid/memberId.
    Returns totalEquity as float for the chosen scope.
    """
    if sub_uid:
        body = get_wallet_balance(accountType=account_type, subUid=sub_uid)
    elif member_id:
        body = get_wallet_balance(accountType=account_type, memberId=member_id)
    else:
        body = get_wallet_balance(accountType=account_type)

    if (body or {}).get("retCode") not in (0, "0"):
        raise RuntimeError(f"Bybit retCode={body.get('retCode')} retMsg={body.get('retMsg')}")
    lst = (body.get("result") or {}).get("list") or []
    if not lst:
        return 0.0
    try:
        return float(lst[0].get("totalEquity") or 0)
    except Exception:
        return 0.0

def compute_level(eq: float) -> int:
    lvl = 0
    for n, thresh, _tier in LEVELS:
        if eq >= thresh:
            lvl = n
        else:
            break
    return lvl

def tier_of(level: int) -> str:
    for n, _th, tier in LEVELS:
        if n == level:
            return tier
    return "-"

def should_rate_limit(last_ts: str, min_gap_sec: int) -> bool:
    if not last_ts:
        return False
    try:
        last = datetime.fromisoformat(last_ts)
        now = datetime.now(timezone.utc)
        return (now - last).total_seconds() < min_gap_sec
    except Exception:
        return False

def fmt_money(x: float) -> str:
    return f"${x:,.2f}"

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    scope = f"sub:{SUB_UID}" if SUB_UID else (f"member:{MEMBER_ID}" if MEMBER_ID else "master")
    name_map = _read_sub_meta()
    sub_name, tier_hint = (_display_for_member(SUB_UID or MEMBER_ID, name_map) if (SUB_UID or MEMBER_ID) else ("MASTER", ""))

    print(f"Coach running • scope={scope} • poll {POLL}s • accountType={ACCOUNT_TYPE} • state={STATE_PATH}")
    if SUB_UID or MEMBER_ID:
        uid_display = SUB_UID or MEMBER_ID
        print(f"→ Watching sub-account {sub_name} (UID {uid_display})" + (f" • role/tier: {tier_hint}" if tier_hint else ""))

    st = load_state()

    # Startup snapshot
    try:
        eq0 = read_equity(ACCOUNT_TYPE, SUB_UID or None, MEMBER_ID or None)
        lvl0 = compute_level(eq0)
        print(f"[startup] equity={eq0:.4f} level={lvl0} ({tier_of(lvl0)}) best={st.get('best_equity', 0.0):.4f}")
    except Exception as e:
        print(f"[coach] startup read failed: {e}")

    while True:
        try:
            eq = read_equity(ACCOUNT_TYPE, SUB_UID or None, MEMBER_ID or None)

            # Track best equity
            best = float(st.get("best_equity", 0.0))
            if eq > best:
                best = eq
                st["best_equity"] = best

            # Level logic with debounce
            lvl_prev = int(st.get("current_level", 0))
            lvl_now = compute_level(eq)
            if lvl_now > lvl_prev and not should_rate_limit(st.get("last_levelup_ts", ""), ANNOUNCE_MIN_GAP_SEC):
                st["current_level"] = lvl_now
                st["last_levelup_ts"] = _utc_now_iso()
                ladder_tier = tier_of(lvl_now)

                hdr = f"🏆 Level Up → Level {lvl_now} ({ladder_tier})"
                scope_line = (f"Sub: {sub_name} (UID {SUB_UID or MEMBER_ID})" if (SUB_UID or MEMBER_ID) else "Scope: MASTER")
                extra = []
                if tier_hint:
                    extra.append(f"Assigned: {tier_hint}")
                msg = (
                    f"{hdr}\n"
                    f"{scope_line}\n"
                    f"Equity: {fmt_money(eq)} (Best: {fmt_money(best)})\n"
                    f"Milestone hit: {fmt_money(LEVEL_TARGETS[lvl_now])}\n"
                )
                if extra:
                    msg += " • ".join(extra) + "\n"

                next_level = lvl_now + 1
                if next_level in LEVEL_TARGETS:
                    msg += f"Next target: Level {next_level} at {fmt_money(LEVEL_TARGETS[next_level])} — keep pushing! 🚀"
                else:
                    msg += "You reached the final milestone — outstanding! 🏔️"

                print(msg)
                _safe_tg_send(msg)
                try:
                    log_event(
                        component="coach",
                        event="level_up",
                        symbol="",
                        account_uid=(SUB_UID or MEMBER_ID or "master"),
                        payload={"equity": eq, "best_equity": best, "level": lvl_now, "tier": ladder_tier},
                        level="info",
                    )
                except Exception:
                    pass
                save_state(st)

            # Optional drawdown alert from best equity
            best_equity = float(st.get("best_equity", 0.0))
            if best_equity > 0 and 1 <= DRAWDOWN_PCT < 95:
                threshold = best_equity * (1 - DRAWDOWN_PCT / 100.0)
                if eq > 0 and eq <= threshold:
                    if not should_rate_limit(st.get("last_drawdown_ts", ""), DRAWDOWN_COOLDOWN_MIN * 60):
                        st["last_drawdown_ts"] = _utc_now_iso()
                        pct = ((best_equity - eq) / best_equity) * 100.0 if best_equity else 0.0
                        hdr = "⚠️ Drawdown Alert"
                        scope_line = (f"Sub: {sub_name} (UID {SUB_UID or MEMBER_ID})" if (SUB_UID or MEMBER_ID) else "Scope: MASTER")
                        msg = (
                            f"{hdr}: {pct:.1f}% from peak\n"
                            f"{scope_line}\n"
                            f"Peak: {fmt_money(best_equity)} → Now: {fmt_money(eq)}\n"
                            f"Alert threshold ({DRAWDOWN_PCT}%): {fmt_money(threshold)}"
                        )
                        print(msg)
                        _safe_tg_send(msg)
                        try:
                            log_event(
                                component="coach",
                                event="drawdown_alert",
                                symbol="",
                                account_uid=(SUB_UID or MEMBER_ID or "master"),
                                payload={"peak": best_equity, "now": eq, "pct": pct, "threshold": threshold},
                                level="warn",
                            )
                        except Exception:
                            pass
                        save_state(st)

            time.sleep(POLL)

        except KeyboardInterrupt:
            print("Coach stopped by user.")
            break
        except Exception as e:
            print(f"[coach] error: {e}")
            time.sleep(POLL)

if __name__ == "__main__":
    main()
