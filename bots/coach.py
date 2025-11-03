# bots/coach.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Milestone Coach (master or per-subaccount; monitor-only)

What it does
- Watches total equity and announces level-ups across a milestone ladder.
- Tracks best equity (peak) and optional drawdown alerts off the peak.
- Sends Telegram via core.base44_client.tg_send (if configured).
- Can target:
    • MASTER account (default), or
    • A specific SUB-ACCOUNT by memberId (COACH_MEMBER_ID)
- Reads registry/sub_map.json to pretty-print sub name/role/tier when available.
- Keeps separate state files per scope (master vs each sub).

ENV (optional):
  COACH_POLL_SEC=30
  COACH_ACCOUNT_TYPE=UNIFIED
  COACH_MEMBER_ID=           # e.g., "302355261" for a sub; unset = master
  COACH_NAME=                # override display name (else from sub_map or fallback)
  COACH_TIER_LABEL=          # override tier label (else Tier 1/2/3 by level, or sub_map if available)
  STATE_DIR=.state
  COACH_DRAWDOWN_PCT=12
  COACH_DRAWDOWN_COOLDOWN_MIN=60
  COACH_ANNOUNCE_MIN_GAP_SEC=5
  TELEGRAM_SILENT=0

Milestones & tiers:
  L1 $25 • L2 $50 • L3 $100 • L4 $250  → Tier 1
  L5 $500 • L6 $1k • L7 $2.5k          → Tier 2
  L8 $5k • L9 $10k • L10 $25k          → Tier 3
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime, timezone

# ── Robust import: add project root, then import from core package
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.base44_client import (  # type: ignore
    get_wallet_balance, tg_send, load_sub_uids
)

# ──────────────────────────────────────────────────────────────────────────────
# Config / State
# ──────────────────────────────────────────────────────────────────────────────
POLL = int(os.getenv("COACH_POLL_SEC", "30"))
ACCOUNT_TYPE = os.getenv("COACH_ACCOUNT_TYPE", "UNIFIED")
MEMBER_ID = (os.getenv("COACH_MEMBER_ID") or "").strip()  # if set, we coach this sub UID
NAME_OVERRIDE = (os.getenv("COACH_NAME") or "").strip()
TIER_OVERRIDE = (os.getenv("COACH_TIER_LABEL") or "").strip()

STATE_DIR = Path(os.getenv("STATE_DIR", ".state"))
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / (f"coach_state_sub_{MEMBER_ID}.json" if MEMBER_ID else "coach_state_master.json")

DRAWDOWN_PCT = float(os.getenv("COACH_DRAWDOWN_PCT", "12"))
DRAWDOWN_COOLDOWN_MIN = int(os.getenv("COACH_DRAWDOWN_COOLDOWN_MIN", "60"))
ANNOUNCE_MIN_GAP_SEC = int(os.getenv("COACH_ANNOUNCE_MIN_GAP_SEC", "5"))
TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "0") == "1"

# Milestones + tiers
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

def _read_sub_meta():
    """
    Returns:
      - name_map: Dict[str, Any]
        * If sub_map.json is { "302..": "Vehicle", ... } → value is str
        * If it's { "302..": {"name":"Vehicle","role":"vehiclefund","tier":"Tier 2","flags":{"vehicle":true}} }
          → value is dict with fields.
    """
    name_map: dict = {}
    try:
        _, name_map_simple = load_sub_uids(str(SUB_CSV), str(SUB_MAP))
        # load_sub_uids returns either simple map (str->str) or empty
        if name_map_simple:
            name_map = name_map_simple
        # If it's a file with objects, read raw to capture roles/tiers/flags
        if SUB_MAP.exists():
            raw = json.loads(SUB_MAP.read_text(encoding="utf-8"))
            # Merge: raw wins (richer)
            if isinstance(raw, dict):
                for k, v in raw.items():
                    name_map[k] = v
    except Exception:
        pass
    return name_map

def _display_for_member(member_id: str, name_map: dict) -> tuple[str, str]:
    """
    Returns (display_name, display_tier_hint)
    """
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

    # Tier hint priority: env override → sub_map tier → None
    if TIER_OVERRIDE:
        th = TIER_OVERRIDE
    else:
        val = name_map.get(member_id, {})
        if isinstance(val, dict):
            th = val.get("tier") or ""
        else:
            th = ""
    return nm, th

def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "current_level": 0,
        "best_equity": 0.0,
        "last_levelup_ts": "",
        "last_drawdown_ts": "",
    }

def save_state(st: dict) -> None:
    STATE_PATH.write_text(json.dumps(st, indent=2), encoding="utf-8")

def read_equity(account_type: str, member_id: str | None) -> float:
    """
    Uses core.base44_client.get_wallet_balance with optional memberId.
    Returns totalEquity as float for the chosen scope.
    """
    body = get_wallet_balance(accountType=account_type, memberId=member_id) if member_id else \
           get_wallet_balance(accountType=account_type)
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
    scope = f"sub:{MEMBER_ID}" if MEMBER_ID else "master"
    name_map = _read_sub_meta()
    sub_name, tier_hint = (_display_for_member(MEMBER_ID, name_map) if MEMBER_ID else ("MASTER", ""))

    print(f"Coach running • scope={scope} • poll {POLL}s • accountType={ACCOUNT_TYPE} • state={STATE_PATH}")
    if MEMBER_ID:
        print(f"→ Watching sub-account {sub_name} (UID {MEMBER_ID})"
              + (f" • role/tier: {tier_hint}" if tier_hint else ""))

    st = load_state()

    # Startup snapshot
    try:
        eq0 = read_equity(ACCOUNT_TYPE, MEMBER_ID or None)
        lvl0 = compute_level(eq0)
        print(f"[startup] equity={eq0:.4f} level={lvl0} ({tier_of(lvl0)}) best={st.get('best_equity', 0.0):.4f}")
    except Exception as e:
        print(f"[coach] startup read failed: {e}")

    while True:
        try:
            eq = read_equity(ACCOUNT_TYPE, MEMBER_ID or None)

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
                scope_line = (f"Sub: {sub_name} (UID {MEMBER_ID})" if MEMBER_ID else "Scope: MASTER")
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
                        scope_line = (f"Sub: {sub_name} (UID {MEMBER_ID})" if MEMBER_ID else "Scope: MASTER")
                        msg = (
                            f"{hdr}: {pct:.1f}% from peak\n"
                            f"{scope_line}\n"
                            f"Peak: {fmt_money(best_equity)} → Now: {fmt_money(eq)}\n"
                            f"Alert threshold ({DRAWDOWN_PCT}%): {fmt_money(threshold)}"
                        )
                        print(msg)
                        _safe_tg_send(msg)
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
