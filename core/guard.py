#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Portfolio Guard v2
Daily DD cap, gross exposure cap, symbol concentration cap.
Sets/clears the global breaker via core.guard.

Design
- Anchors "day start equity" once per UTC day (or local if TZ set).
- Computes current equity from /v5/account/wallet-balance (unified: USDT).
- Computes gross exposure and per-symbol exposure from positions.
- Enforces thresholds with TTL + hysteresis headroom.

Env (core.config.settings):
  # Schedule
  PG_SWEEP_SEC=7

  # Limits
  PG_DD_MAX_PCT=0.012          # 1.2% daily loss cap
  PG_DD_CLEAR_PCT=0.009        # clear when DD recovers below 0.9%
  PG_GROSS_EXPO_MAX_PCT=0.60   # 60% of equity max gross
  PG_CONC_MAX_PCT=0.35         # 35% per-symbol concentration

  # Breaker TTL and messages
  PG_BREAKER_TTL_SEC=900       # 15 min lock
  PG_REASON_PREFIX=[B44]

  # Ownership filter (match TP/SL behavior)
  OWNERSHIP_ENFORCED=true
  MANAGE_UNTAGGED=false
  OWNERSHIP_SUB_UID=260417078
  OWNERSHIP_STRATEGY=A2
  TP_MANAGED_TAG=B44

Notes
- HTTP-only; uses Bybit private endpoints via core.bybit_client.
- Uses USDT settling assumption for linear perps.
"""

from __future__ import annotations
import json, time, math
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, List, Tuple

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.guard import guard_set, guard_clear, guard_blocking_reason

getcontext().prec = 28
log = bind_context(get_logger("bots.portfolio_guard"), comp="pg")

ROOT = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ANCHOR_PATH = STATE_DIR / "guard_day_anchor.json"

# ---------- env ----------
def _b(s, default):  # boolean-ish
    return str(getattr(settings, s, default)).lower() in ("1","true","yes","on")

PG_SWEEP_SEC          = int(getattr(settings, "PG_SWEEP_SEC", 7))
PG_DD_MAX_PCT         = float(getattr(settings, "PG_DD_MAX_PCT", 0.012))
PG_DD_CLEAR_PCT       = float(getattr(settings, "PG_DD_CLEAR_PCT", 0.009))
PG_GROSS_EXPO_MAX_PCT = float(getattr(settings, "PG_GROSS_EXPO_MAX_PCT", 0.60))
PG_CONC_MAX_PCT       = float(getattr(settings, "PG_CONC_MAX_PCT", 0.35))
PG_BREAKER_TTL_SEC    = int(getattr(settings, "PG_BREAKER_TTL_SEC", 900))
PG_REASON_PREFIX      = str(getattr(settings, "PG_REASON_PREFIX", "[B44]"))

OWNERSHIP_ENFORCED    = _b("OWNERSHIP_ENFORCED", True)
MANAGE_UNTAGGED       = _b("MANAGE_UNTAGGED", False)
TP_MANAGED_TAG        = str(getattr(settings, "TP_MANAGED_TAG", "B44"))
OWNERSHIP_SUB_UID     = str(getattr(settings, "OWNERSHIP_SUB_UID", "")).strip()
OWNERSHIP_STRATEGY    = str(getattr(settings, "OWNERSHIP_STRATEGY", "")).strip()

# ---------- tag helpers (reuse same heuristics as TP/SL) ----------
def _link_is_ours(link: str | None) -> bool:
    if not link:
        return False
    s = str(link)
    return (TP_MANAGED_TAG in s) or s.startswith("B44") or (OWNERSHIP_SUB_UID and OWNERSHIP_SUB_UID in s)

def _position_owned(symbol: str, pos_row: dict, open_orders: List[dict]) -> bool:
    for k in ("positionTag","comment","lastOrderLinkId","last_exec_link_id"):
        v = pos_row.get(k)
        if v and _link_is_ours(str(v)):
            return True
    for it in open_orders:
        try:
            if str(it.get("symbol","")).upper() != symbol.upper():
                continue
            if str(it.get("reduceOnly","")).lower() not in ("true","1"):
                continue
            if _link_is_ours(it.get("orderLinkId")):
                return True
        except Exception:
            pass
    return MANAGE_UNTAGGED

# ---------- persistence ----------
def _utc_day(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts))

def _load_anchor() -> Dict:
    if ANCHOR_PATH.exists():
        try:
            return json.loads(ANCHOR_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_anchor(obj: Dict) -> None:
    try:
        ANCHOR_PATH.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _ensure_anchor(total_equity: Decimal) -> Decimal:
    now = int(time.time())
    day = _utc_day(now)
    st = _load_anchor()
    if st.get("day") != day or not st.get("equity_start"):
        st = {"day": day, "ts": now, "equity_start": str(total_equity)}
        _save_anchor(st)
        log.info("set daily anchor equity: %s", total_equity)
        return total_equity
    try:
        return Decimal(str(st.get("equity_start")))
    except Exception:
        return total_equity

# ---------- bybit ----------
by = Bybit()
try:
    by.sync_time()
except Exception as e:
    log.warning("time sync failed: %s", e)

def _wallet_equity() -> Decimal:
    ok, data, err = by._request_private_json("/v5/account/wallet-balance", params={"accountType":"UNIFIED"})
    if not ok:
        raise RuntimeError(err)
    lst = (data.get("result") or {}).get("list") or []
    # sum USDT-equity-like; if multiple coins present, use totalEquity in USDT
    total = Decimal("0")
    for acc in lst:
        total += Decimal(str(acc.get("totalEquity") or "0"))
    return total

def _positions() -> List[dict]:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        raise RuntimeError(err)
    return (data.get("result") or {}).get("list") or []

def _open_orders() -> List[dict]:
    ok, data, err = by.get_open_orders(category="linear", openOnly=True)
    if not ok:
        return []
    return (data.get("result") or {}).get("list") or []

# ---------- risk math ----------
def _position_notional_usdt(p: dict) -> Decimal:
    try:
        sz = Decimal(str(p.get("size") or "0"))
        avg = Decimal(str(p.get("avgPrice") or "0"))
        return abs(sz * avg)
    except Exception:
        return Decimal("0")

def _aggregate_exposure(positions: List[dict], orders: List[dict]) -> Tuple[Decimal, Dict[str, Decimal]]:
    gross = Decimal("0")
    per_sym: Dict[str, Decimal] = {}
    for p in positions:
        sym = str(p.get("symbol") or "").upper()
        if not sym:
            continue
        if OWNERSHIP_ENFORCED and not _position_owned(sym, p, orders):
            continue
        notional = _position_notional_usdt(p)
        if notional <= 0:
            continue
        gross += notional
        per_sym[sym] = per_sym.get(sym, Decimal("0")) + notional
    return gross, per_sym

# ---------- loop ----------
def sweep_once():
    # If already blocked, still compute to decide if we can clear
    blocked, _ = guard_blocking_reason()

    eq = _wallet_equity()
    anchor = _ensure_anchor(eq)

    # Guard against zero anchor
    if anchor <= 0:
        log.warning("anchor equity non-positive, skipping")
        return

    dd = max(0.0, float((anchor - eq) / anchor))
    orders = _open_orders()
    positions = _positions()
    gross, per_sym = _aggregate_exposure(positions, orders)

    gross_pct = float(gross / eq) if eq > 0 else 0.0
    conc_pct = max((float(v / eq) for v in per_sym.values()), default=0.0) if eq > 0 else 0.0

    # Violations
    dd_violation    = dd >= PG_DD_MAX_PCT
    gross_violation = gross_pct > PG_GROSS_EXPO_MAX_PCT
    conc_violation  = conc_pct > PG_CONC_MAX_PCT

    reason_parts = []
    if dd_violation:
        reason_parts.append(f"DD {dd:.3%} ≥ {PG_DD_MAX_PCT:.2%}")
    if gross_violation:
        reason_parts.append(f"Gross {gross_pct:.1%} > {PG_GROSS_EXPO_MAX_PCT:.0%}")
    if conc_violation:
        reason_parts.append(f"Conc {conc_pct:.1%} > {PG_CONC_MAX_PCT:.0%}")

    if reason_parts:
        msg = f"{PG_REASON_PREFIX} Breaker: " + " | ".join(reason_parts)
        guard_set(msg, ttl_sec=PG_BREAKER_TTL_SEC, meta={
            "dd": dd, "gross_pct": gross_pct, "conc_pct": conc_pct,
            "anchor": float(anchor), "equity": float(eq)
        })
        log.warning(msg)
        return

    # Clear rule: drawdown recovered below clear threshold AND no exposure breaches
    can_clear = (dd <= PG_DD_CLEAR_PCT) and (gross_pct <= PG_GROSS_EXPO_MAX_PCT) and (conc_pct <= PG_CONC_MAX_PCT)

    if blocked and can_clear:
        guard_clear(note=f"Recovered: DD {dd:.3%}, gross {gross_pct:.1%}, conc {conc_pct:.1%}")
        log.info("%s breaker cleared", PG_REASON_PREFIX)

    # telemetry
    log.info("eq=%.2f anchor=%.2f dd=%.3f gross=%.3f conc=%.3f pos=%d",
             float(eq), float(anchor), dd, gross_pct, conc_pct, len(per_sym))

def main():
    log.info("Portfolio Guard v2 online: sweep=%ss DDmax=%.2f%% clear=%.2f%% gross=%.0f%% conc=%.0f%% ttl=%ss",
             PG_SWEEP_SEC, PG_DD_MAX_PCT*100, PG_DD_CLEAR_PCT*100,
             PG_GROSS_EXPO_MAX_PCT*100, PG_CONC_MAX_PCT*100, PG_BREAKER_TTL_SEC)
    # First sweep
    sweep_once()
    while True:
        try:
            time.sleep(max(3, PG_SWEEP_SEC))
            sweep_once()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("loop error: %s", e)
            time.sleep(PG_SWEEP_SEC)

if __name__ == "__main__":
    main()
