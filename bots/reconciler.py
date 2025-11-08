#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots.reconciler — Keep DB and exchange in sync; maintain 5-rung ladders.

What it does every N seconds:
  1) Pull open orders, recent executions, and positions from Bybit via your client.
  2) Update DB:
     - Mark orders FILLED/CANCELED/REJECTED where applicable
     - Insert executions (fills) with price/qty/fee
     - Upsert positions snapshot
  3) For each open bot-managed position, call your bots.reconcile_ladder to ensure:
     - exactly 5 reduce-only TP rungs exist (per policy)
     - SL policy present
     - foreign/stray orders are canceled (configurable)
Safeties:
  - Won’t touch untagged/manual orders unless RECON_TOUCH_MANUAL=true
  - Respects RECON_SYMBOL_WHITELIST if set
"""

from __future__ import annotations
import os, time, math
from typing import Optional, Dict, Any, List

# logger
from core.logger import get_logger, bind_context
log = bind_context(get_logger("bots.reconciler"), comp="reconciler")

# config via env (no drama)
RECON_INTERVAL_SEC   = int(os.getenv("RECON_INTERVAL_SEC", "5"))
RECON_DRY_RUN        = os.getenv("RECON_DRY_RUN","true").lower() in ("1","true","yes","on")
RECON_SAFE_MODE      = os.getenv("RECON_SAFE_MODE","true").lower() in ("1","true","yes","on")
RECON_TOUCH_MANUAL   = os.getenv("RECON_TOUCH_MANUAL","false").lower() in ("1","true","yes","on")
RECON_TAG_PREFIX     = os.getenv("RECON_TAG_PREFIX","B44")
RECON_CANCEL_STRAYS  = os.getenv("RECON_CANCEL_STRAYS","false").lower() in ("1","true","yes","on")
RECON_INCLUDE_LONGS  = os.getenv("RECON_INCLUDE_LONGS","true").lower() in ("1","true","yes","on")
RECON_INCLUDE_SHORTS = os.getenv("RECON_INCLUDE_SHORTS","true").lower() in ("1","true","yes","on")
RECON_SYMBOL_WHITELIST = [s.strip().upper() for s in (os.getenv("RECON_SYMBOL_WHITELIST","") or "").split(",") if s.strip()]

# DB
from core.db import migrate, get_open_orders, insert_execution, set_order_state, upsert_position

# Exchange client
from core.bybit_client import Bybit

# Ladder reconciler (your existing file)
try:
    from bots.reconcile_ladder import reconcile_ladder_for_symbol  # assume you expose this
except Exception:
    reconcile_ladder_for_symbol = None  # we'll warn later

# Notifier optional
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(*a, **k): pass

def _is_bot_order(order: Dict[str, Any]) -> bool:
    """Detect bot-managed orders via orderLinkId or tag."""
    link = order.get("orderLinkId") or order.get("order_link_id") or ""
    return link.startswith(RECON_TAG_PREFIX)

def _allowed_symbol(sym: str) -> bool:
    if not RECON_SYMBOL_WHITELIST:
        return True
    return sym.upper() in RECON_SYMBOL_WHITELIST

def _side_from_qty(qty: float) -> str:
    if qty > 0: return "Long"
    if qty < 0: return "Short"
    return "Flat"

def _mark_orders_from_exchange(by: Bybit) -> None:
    """
    Fetch open orders; mark DB orders that are missing as CANCELED (or FILLED via executions flow).
    This pass focuses on state sanity; fills get handled in _apply_fills.
    """
    # Pull all open orders (linear perps)
    ok, exch, err = by.get_open_orders(category="linear")
    if not ok:
        log.warning("open orders fetch failed: %s", err); return
    open_bybit: Dict[str, Dict] = {}
    for it in (exch.get("result", {}) or {}).get("list", []) or []:
        open_bybit[(it.get("orderLinkId") or "")] = it

    # Walk our DB open orders and see what's gone on the exchange
    rows = get_open_orders()
    for r in rows:
        oid, sym, state, tag = r["id"], r["symbol"], r["state"], (r["tag"] or "")
        if not _allowed_symbol(sym): 
            continue
        if not RECON_TOUCH_MANUAL and not str(tag).startswith(RECON_TAG_PREFIX):
            continue  # leave manual or foreign stuff alone
        exch_row = open_bybit.get(oid)
        if exch_row is None:
            # No longer open at exchange; leave final state resolution to fills step,
            # but if it's stuck NEW/SENT/ACKED with no fills, we can mark CANCELED heuristically.
            if state in ("NEW","SENT","ACKED","PARTIAL"):
                set_order_state(oid, "CANCELED")
                log.info("marked CANCELED (missing on exchange) id=%s sym=%s", oid, sym)

def _apply_fills(by: Bybit) -> None:
    """
    Pull recent executions and persist them, marking orders FILLED when fully done.
    This assumes your Bybit client can return recent user trades.
    """
    ok, data, err = by.get_executions(category="linear")
    if not ok:
        log.warning("executions fetch failed: %s", err); return
    # Normalize and write
    for tr in (data.get("result",{}) or {}).get("list",[]) or []:
        try:
            oid   = tr.get("orderLinkId") or tr.get("order_link_id")
            px    = float(tr.get("execPrice"))
            qty   = float(tr.get("execQty"))
            fee   = float(tr.get("execFee", 0.0))
            if not oid or qty <= 0 or px <= 0:
                continue
            insert_execution(oid, qty, px, fee=fee)
            # Heuristic: if execution reports "is maker" false and order no longer open, mark FILLED
            # Safer to let positions check finalize the order state too.
            set_order_state(oid, "FILLED")
        except Exception as e:
            log.warning("skip exec row err=%s row=%s", e, tr)

def _sync_positions(by: Bybit) -> List[Dict[str, Any]]:
    """
    Pull positions and upsert canonical snapshot;
    returns normalized list for ladder reconciliation.
    """
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        log.warning("positions fetch failed: %s", err); return []
    out: List[Dict[str, Any]] = []
    for p in (data.get("result",{}) or {}).get("list",[]) or []:
        try:
            sym = p.get("symbol"); if_notional = p.get("positionValue")
            qty = float(p.get("size") or p.get("qty") or 0)
            side = "Long" if (p.get("side","").lower() == "buy" or qty > 0) else ("Short" if qty < 0 else "Flat")
            avg  = float(p.get("avgPrice") or p.get("avgEntryPrice") or 0.0)
            sub  = str(p.get("accountId") or p.get("subUid") or "MAIN")
            # upsert canonical
            upsert_position(sym, sub, abs(qty), avg, side)
            out.append({"symbol": sym, "sub_uid": sub, "qty": abs(qty), "side": side, "avg_price": avg})
        except Exception as e:
            log.warning("skip position err=%s row=%s", e, p)
    return out

def _rebuild_ladders(positions: List[Dict[str, Any]], by: Bybit) -> None:
    if reconcile_ladder_for_symbol is None:
        log.warning("bots.reconcile_ladder missing exported reconcile_ladder_for_symbol(); ladder step skipped.")
        return
    for pos in positions:
        sym, side, qty = pos["symbol"], pos["side"], float(pos["qty"])
        if not _allowed_symbol(sym):
            continue
        if side == "Flat" or qty <= 0:
            continue
        if (side == "Long" and not RECON_INCLUDE_LONGS) or (side == "Short" and not RECON_INCLUDE_SHORTS):
            continue
        try:
            reconcile_ladder_for_symbol(
                symbol=sym,
                side=side,
                qty=qty,
                dry_run=RECON_DRY_RUN,
                safe_mode=RECON_SAFE_MODE,
                tag_prefix=RECON_TAG_PREFIX,
                cancel_strays=RECON_CANCEL_STRAYS,
                bybit=by
            )
        except Exception as e:
            log.warning("ladder reconcile failed for %s: %s", sym, e)

def main():
    migrate()
    by = Bybit()
    try:
        by.sync_time()
    except Exception:
        pass

    tg_send("🟢 Reconciler online • interval={}s • tag={}".format(RECON_INTERVAL_SEC, RECON_TAG_PREFIX), priority="success")
    log.info("online • interval=%ss tag=%s dry=%s safe=%s touch_manual=%s",
             RECON_INTERVAL_SEC, RECON_TAG_PREFIX, RECON_DRY_RUN, RECON_SAFE_MODE, RECON_TOUCH_MANUAL)

    while True:
        t0 = time.time()
        try:
            _mark_orders_from_exchange(by)
            _apply_fills(by)
            positions = _sync_positions(by)
            _rebuild_ladders(positions, by)
        except Exception as e:
            log.warning("reconcile loop error: %s", e)
        # pace
        dt = time.time() - t0
        sleep_for = max(0.5, RECON_INTERVAL_SEC - dt)
        time.sleep(sleep_for)

if __name__ == "__main__":
    main()
