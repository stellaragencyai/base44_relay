#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots.reconciler — DB⇄Exchange sync + ladder maintenance + MFE ratchet.

What this does
- Compares DB "open" orders vs exchange; marks missing ones as CANCELLED.
- Pulls recent executions from exchange and records them in DB.
- Syncs live positions (optional upsert; no-op if DB lacks positions API).
- Rebuilds TP ladder for live positions; passes MFE hint to ladder reconciler.

Env (bools accept: 1/true/yes/on)
  RECON_INTERVAL_SEC=5
  RECON_DRY_RUN=true
  RECON_SAFE_MODE=true
  RECON_TOUCH_MANUAL=false
  RECON_TAG_PREFIX=B44
  RECON_CANCEL_STRAYS=false
  RECON_INCLUDE_LONGS=true
  RECON_INCLUDE_SHORTS=true
  RECON_SYMBOL_WHITELIST=BTCUSDT,ETHUSDT
"""
from __future__ import annotations

import os
import time
from typing import Optional, Dict, Any, List

from core.logger import get_logger, bind_context
log = bind_context(get_logger("bots.reconciler"), comp="reconciler")

# ---------- config ----------
RECON_INTERVAL_SEC    = int(os.getenv("RECON_INTERVAL_SEC", "5"))
RECON_DRY_RUN         = os.getenv("RECON_DRY_RUN", "true").lower() in ("1","true","yes","on")
RECON_SAFE_MODE       = os.getenv("RECON_SAFE_MODE", "true").lower() in ("1","true","yes","on")
RECON_TOUCH_MANUAL    = os.getenv("RECON_TOUCH_MANUAL", "false").lower() in ("1","true","yes","on")
RECON_TAG_PREFIX      = os.getenv("RECON_TAG_PREFIX", "B44")
RECON_CANCEL_STRAYS   = os.getenv("RECON_CANCEL_STRAYS", "false").lower() in ("1","true","yes","on")
RECON_INCLUDE_LONGS   = os.getenv("RECON_INCLUDE_LONGS", "true").lower() in ("1","true","yes","on")
RECON_INCLUDE_SHORTS  = os.getenv("RECON_INCLUDE_SHORTS", "true").lower() in ("1","true","yes","on")
RECON_SYMBOL_WHITELIST = [s.strip().upper() for s in (os.getenv("RECON_SYMBOL_WHITELIST","") or "").split(",") if s.strip()]

# ---------- deps (tolerant) ----------
from core.bybit_client import Bybit

# DB API: we accept your current core.db, and emulate helpers if missing.
try:
    from core.db import migrate, insert_execution, set_order_state, list_orders
except Exception as e:
    raise RuntimeError(f"core.db missing required functions: {e}")

# Optional: positions upsert (no-op if not present)
try:
    from core.db import upsert_position  # type: ignore
except Exception:
    def upsert_position(*_a, **_k):  # type: ignore
        return None

# Optional notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(*_a, **_k):  # silent fallback
        pass

# Optional ladder reconciler entrypoint
_recon_func = None
try:
    from bots.reconcile_ladder import reconcile_ladder_for_symbol as _recon_func  # type: ignore
except Exception:
    try:
        from bots.reconcile_ladder import reconcile_for_symbol as _recon_func  # type: ignore
    except Exception:
        try:
            from bots.reconcile_ladder import reconcile as _recon_func  # type: ignore
        except Exception:
            _recon_func = None

# ---------- helpers ----------

_OPEN_STATES = ("NEW","SENT","ACKED","PARTIAL")

def _allowed_symbol(sym: str) -> bool:
    return not RECON_SYMBOL_WHITELIST or (sym or "").upper() in RECON_SYMBOL_WHITELIST

def _is_bot_order(tag: Optional[str]) -> bool:
    return bool(tag) and str(tag).startswith(RECON_TAG_PREFIX)

def _db_open_orders() -> List[Dict[str, Any]]:
    """
    Emulate a 'get_open_orders()' from DB using list_orders(), filtering to states that imply open.
    Returns rows with at least: link_id, symbol, state, tag.
    """
    rows = list_orders(limit=10000)  # generous cap; SQLite is fast enough
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            st = (r.get("state") or "").upper()
            if st in _OPEN_STATES:
                out.append({
                    "link_id": r.get("link_id"),
                    "symbol": r.get("symbol"),
                    "state": st,
                    "tag": r.get("tag") or "",
                })
        except Exception:
            continue
    return out

def _mark_orders_from_exchange(by: Bybit) -> None:
    ok, exch, err = by.get_open_orders(category="linear")
    if not ok:
        log.warning("open orders fetch failed: %s", err)
        return

    open_bybit: Dict[str, Dict] = {}
    for it in (exch.get("result", {}) or {}).get("list", []) or []:
        lid = (it.get("orderLinkId") or "").strip()
        if lid:
            open_bybit[lid] = it

    db_rows = _db_open_orders()
    for r in db_rows:
        lid = r["link_id"]
        if not lid:
            continue
        sym = r.get("symbol") or ""
        tag = r.get("tag") or ""
        if not _allowed_symbol(sym):
            continue
        if not RECON_TOUCH_MANUAL and not _is_bot_order(tag):
            continue
        if lid not in open_bybit:
            # Missing live; mark CANCELLED in DB
            try:
                set_order_state(lid, "CANCELLED")
                log.info("marked CANCELLED (not on exchange) link_id=%s sym=%s", lid, sym)
            except Exception as e:
                log.warning("failed to mark CANCELLED for %s: %s", lid, e)

def _apply_fills(by: Bybit) -> None:
    ok, data, err = by.get_executions(category="linear")
    if not ok:
        log.warning("executions fetch failed: %s", err)
        return

    # Note: We don’t compute partial aggregation here; we append fills.
    # If you later add per-order fill totals, you can decide FILLED vs PARTIAL precisely.
    for tr in (data.get("result", {}) or {}).get("list", []) or []:
        try:
            lid = (tr.get("orderLinkId") or tr.get("order_link_id") or "").strip()
            if not lid:
                continue
            px = float(tr.get("execPrice"))
            qty = float(tr.get("execQty"))
            fee = float(tr.get("execFee", 0.0))
            if px <= 0 or qty <= 0:
                continue

            insert_execution(lid, qty, px, fee=fee)

            # Minimal heuristic:
            # If Bybit says this trade closed the order, state should be FILLED.
            # Many payloads include isMaker/lastLiquidityInd but not a clean 'closed' flag; we default to PARTIAL.
            # You can switch to FILLED if your exchange payload provides a decisive indicator.
            set_order_state(lid, "PARTIAL")
        except Exception as e:
            log.warning("skip exec row err=%s row=%s", e, tr)

def _sync_positions(by: Bybit) -> List[Dict[str, Any]]:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        log.warning("positions fetch failed: %s", err)
        return []

    out: List[Dict[str, Any]] = []
    for p in (data.get("result", {}) or {}).get("list", []) or []:
        try:
            sym = p.get("symbol") or ""
            size = float(p.get("size") or p.get("qty") or 0)
            if not sym:
                continue
            side = "Long" if (p.get("side","").lower().startswith("b") or size > 0) else ("Short" if size < 0 else "Flat")
            avg  = float(p.get("avgPrice") or p.get("avgEntryPrice") or 0.0)
            sub  = str(p.get("accountId") or p.get("subUid") or "MAIN")

            # Optional persistence; safe no-op if not implemented
            try:
                upsert_position(sym, sub, abs(size), avg, side)  # type: ignore
            except Exception:
                pass

            out.append({"symbol": sym, "sub_uid": sub, "qty": abs(size), "side": side, "avg_price": avg})
        except Exception as e:
            log.warning("skip position err=%s row=%s", e, p)
    return out

def _mfe_bps(current_px: float, avg_px: float, side: str) -> float:
    if current_px <= 0 or avg_px <= 0:
        return 0.0
    if side == "Long":
        return (current_px / avg_px - 1.0) * 10000.0
    return (avg_px / current_px - 1.0) * 10000.0

def _rebuild_ladders(positions: List[Dict[str, Any]], by: Bybit) -> None:
    if _recon_func is None:
        log.warning("reconcile_ladder entrypoint not found; ladder maintenance skipped.")
        return

    # cache mid prices per symbol
    px_cache: Dict[str, float] = {}
    for pos in positions:
        sym = pos["symbol"]
        side = pos["side"]
        qty = float(pos["qty"])

        if not _allowed_symbol(sym) or side == "Flat" or qty <= 0:
            continue
        if side == "Long" and not RECON_INCLUDE_LONGS:
            continue
        if side == "Short" and not RECON_INCLUDE_SHORTS:
            continue

        if sym not in px_cache:
            ok, tk, _ = by.get_tickers(category="linear", symbol=sym)
            if ok:
                lst = (tk.get("result", {}) or {}).get("list", []) or []
                if lst:
                    bid = float(lst[0].get("bid1Price", 0.0))
                    ask = float(lst[0].get("ask1Price", 0.0))
                    px_cache[sym] = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                else:
                    px_cache[sym] = 0.0
            else:
                px_cache[sym] = 0.0

        mid = px_cache.get(sym, 0.0)
        mfe = _mfe_bps(mid, pos["avg_price"], side)

        try:
            _recon_func(
                symbol=sym,
                side=side,
                qty=qty,
                dry_run=RECON_DRY_RUN,
                safe_mode=RECON_SAFE_MODE,
                tag_prefix=RECON_TAG_PREFIX,
                cancel_strays=RECON_CANCEL_STRAYS,
                bybit=by,
                mfe_hint_bps=mfe,  # newer signature
            )
        except TypeError:
            # Backward compatibility with older signature
            _recon_func(
                symbol=sym,
                side=side,
                qty=qty,
                dry_run=RECON_DRY_RUN,
                safe_mode=RECON_SAFE_MODE,
                tag_prefix=RECON_TAG_PREFIX,
                cancel_strays=RECON_CANCEL_STRAYS,
                bybit=by,
            )
        except Exception as e:
            log.warning("ladder reconcile failed for %s: %s", sym, e)

# ---------- main ----------

def main():
    # Make sure DB is ready
    try:
        from core.db import migrate
        migrate()
    except Exception:
        log.warning("DB migrate skipped (core.db.migrate not available)")

    by = Bybit()
    try:
        by.sync_time()
    except Exception:
        pass

    tg_send(f"🟢 Reconciler online • interval={RECON_INTERVAL_SEC}s • tag={RECON_TAG_PREFIX}", priority="success")
    log.info(
        "online • interval=%ss tag=%s dry=%s safe=%s touch_manual=%s",
        RECON_INTERVAL_SEC, RECON_TAG_PREFIX, RECON_DRY_RUN, RECON_SAFE_MODE, RECON_TOUCH_MANUAL
    )

    while True:
        t0 = time.time()
        try:
            _mark_orders_from_exchange(by)
            _apply_fills(by)
            positions = _sync_positions(by)
            _rebuild_ladders(positions, by)
        except Exception as e:
            log.warning("reconcile loop error: %s", e)
        dt = time.time() - t0
        time.sleep(max(0.5, RECON_INTERVAL_SEC - dt))

if __name__ == "__main__":
    main()
