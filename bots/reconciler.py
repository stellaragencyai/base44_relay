#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots.reconciler — DB⇄Exchange sync + exit hygiene + ladder sanity + MFE ratchet.

What this does
- DB hygiene:
  • Compares DB “open” orders vs exchange; marks missing ones as CANCELED.
  • Appends executions to DB; marks order PARTIAL best-effort.
  • Optionally upserts latest positions snapshot.

- Exit hygiene (ownership-aware):
  • Enforces reduceOnly on our exit orders.
  • Trims TP quantities so sum(TP qty) ≤ live position qty after partial fills.
  • Cancels stray reduceOnly exits when position is flat.
  • Caps our total open orders per symbol and cancels extras (oldest first).

- Ladder maintenance:
  • Calls your reconcile_ladder module (if present) to rebuild targets.
  • Passes MFE hint (bps) to let ladder tighten greed intelligently.

- Breaker-aware:
  • If breaker is ON, reconciler steps aside (tp_sl_manager handles flatten/cancel).
    We still keep DB syncing to avoid skew.

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

Ownership/tagging
  TP_MANAGED_TAG=B44
  OWNERSHIP_ENFORCED=true
  MANAGE_UNTAGGED=false
  OWNERSHIP_SUB_UID=...
  OWNERSHIP_STRATEGY=...

Caps
  TP_MAX_ORDERS_PER_SYMBOL=12
"""

from __future__ import annotations

import os
import time
from decimal import Decimal, ROUND_DOWN, getcontext
from typing import Optional, Dict, Any, List, Tuple

from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.config import settings

# breaker awareness
try:
    from core.guard import guard_blocking_reason
except Exception:
    def guard_blocking_reason():
        return False, ""

# DB API (required)
try:
    from core.db import migrate, insert_execution, set_order_state, list_orders
except Exception as e:
    raise RuntimeError(f"core.db missing required functions: {e}")

# Optional: positions upsert
try:
    from core.db import upsert_position  # type: ignore
except Exception:
    def upsert_position(*_a, **_k):  # type: ignore
        return None

# Optional notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(*_a, **_k):  # silent
        pass

# Optional decision log
try:
    from core.decision_log import log_event
except Exception:
    def log_event(*_, **__):  # type: ignore
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

getcontext().prec = 28
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

OWNERSHIP_ENFORCED    = str(getattr(settings, "OWNERSHIP_ENFORCED", "true")).lower() in ("1","true","yes","on")
MANAGE_UNTAGGED       = str(getattr(settings, "MANAGE_UNTAGGED", "false")).lower() in ("1","true","yes","on")
TP_TAG                = (str(getattr(settings, "TP_MANAGED_TAG", "B44")).strip() or "B44")[:12]
SUB_UID               = str(getattr(settings, "OWNERSHIP_SUB_UID", "")).strip()
STRATEGY              = str(getattr(settings, "OWNERSHIP_STRATEGY", "")).strip()

MAX_ORDERS_PER_SYMBOL = max(6, int(getattr(settings, "TP_MAX_ORDERS_PER_SYMBOL", 12)))

# ---------- helpers ----------
_OPEN_STATES = ("NEW","SENT","ACKED","PARTIAL")

def _allowed_symbol(sym: str) -> bool:
    return not RECON_SYMBOL_WHITELIST or (sym or "").upper() in RECON_SYMBOL_WHITELIST

def _is_bot_order(tag: Optional[str]) -> bool:
    return bool(tag) and str(tag).startswith(RECON_TAG_PREFIX)

def _link_is_ours(link: Optional[str]) -> bool:
    if not link:
        return False
    s = str(link)
    return (TP_TAG in s) or (s.find(SUB_UID) >= 0 if SUB_UID else False) or (TP_TAG in s.split("|")[0])

def _close_side(side_word: str) -> str:
    return "Sell" if side_word == "long" else "Buy"

def _side_word_from_pos(row: dict) -> Optional[str]:
    try:
        s = (row.get("side") or "").lower()
        if s.startswith("b"): return "long"
        if s.startswith("s"): return "short"
    except Exception:
        pass
    try:
        q = Decimal(str(row.get("size") or "0"))
        if q > 0: return "long"
        if q < 0: return "short"
    except Exception:
        pass
    return None

def _owned_position(symbol: str, pos_row: dict, open_orders: List[dict]) -> bool:
    if not OWNERSHIP_ENFORCED:
        return True
    for k in ("positionTag", "comment", "lastOrderLinkId", "last_exec_link_id"):
        v = pos_row.get(k)
        if v and _link_is_ours(str(v)):
            return True
    for it in open_orders:
        try:
            if (it.get("symbol","") or "").upper() != symbol.upper():
                continue
            if str(it.get("reduceOnly","")).lower() not in ("true","1"):
                continue
            if _link_is_ours(it.get("orderLinkId")):
                return True
        except Exception:
            pass
    return MANAGE_UNTAGGED

def _round_step(x: Decimal, step: Decimal) -> Decimal:
    return (x/step).to_integral_value(rounding=ROUND_DOWN) * step

# ---------- exchange wrappers ----------
def _filters(by: Bybit, symbol: str) -> Tuple[Decimal, Decimal, Decimal]:
    ok, data, err = by.get_instruments_info(category="linear", symbol=symbol)
    if not ok:
        raise RuntimeError(f"instruments-info fail {symbol}: {err}")
    info = ((data.get("result") or {}).get("list") or [{}])[0]
    tick = Decimal(info["priceFilter"]["tickSize"])
    step = Decimal(info["lotSizeFilter"]["qtyStep"])
    minq = Decimal(info["lotSizeFilter"]["minOrderQty"])
    return tick, step, minq

def _open_orders(by: Bybit, symbol: Optional[str]=None) -> List[dict]:
    if symbol:
        ok, data, err = by.get_open_orders(category="linear", symbol=symbol, openOnly=True)
    else:
        ok, data, err = by.get_open_orders(category="linear", openOnly=True)
    if not ok:
        return []
    return (data.get("result") or {}).get("list") or []

# ---------- DB→exchange checks ----------
def _db_open_orders() -> List[Dict[str, Any]]:
    """
    Emulate 'get_open_orders()' via list_orders(); returns rows with link_id, symbol, state, tag.
    """
    rows = list_orders(limit=10000)
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
    ok, exch, err = by.get_open_orders(category="linear", openOnly=True)
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
            try:
                set_order_state(lid, "CANCELED")
                log.info("marked CANCELED (not on exchange) link_id=%s sym=%s", lid, sym)
            except Exception as e:
                log.warning("failed to mark CANCELED for %s: %s", lid, e)

def _apply_fills(by: Bybit) -> None:
    ok, data, err = by.get_executions(category="linear")
    if not ok:
        log.warning("executions fetch failed: %s", err)
        return

    for tr in (data.get("result", {}) or {}).get("list", []) or []:
        try:
            lid = (tr.get("orderLinkId") or tr.get("order_link_id") or "").strip()
            if not lid:
                continue
            px = float(tr.get("execPrice") or 0)
            qty = float(tr.get("execQty") or 0)
            fee = float(tr.get("execFee") or 0.0)
            ts  = int(str(tr.get("execTime") or "")[:13] or "0") or None
            if px <= 0 or qty <= 0:
                continue

            insert_execution(lid, qty, px, fee=fee, ts_ms=ts)
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

            try:
                upsert_position(sym, sub, abs(size), avg, side)  # no-op if not implemented
            except Exception:
                pass

            out.append({"symbol": sym, "sub_uid": sub, "qty": abs(size), "side": side, "avg_price": avg, "raw": p})
        except Exception as e:
            log.warning("skip position err=%s row=%s", e, p)
    return out

# ---------- exit hygiene ----------
def _sum_reduce_only_limits(orders: List[dict], close_side: str) -> Decimal:
    total = Decimal("0")
    for o in orders:
        try:
            if str(o.get("reduceOnly","")).lower() not in ("true","1"):
                continue
            if (o.get("orderType") or "") != "Limit":
                continue
            if (o.get("side") or "") != close_side:
                continue
            total += Decimal(str(o.get("qty") or "0"))
        except Exception:
            continue
    return total

def _enforce_reduce_only(by: Bybit, symbol: str, orders: List[dict]) -> None:
    """Force reduceOnly on any of our orders that somehow are not reduce-only."""
    for o in orders:
        try:
            if not _link_is_ours(o.get("orderLinkId")):
                continue
            if str(o.get("reduceOnly","")).lower() in ("1","true"):
                continue
            oid = o.get("orderId")
            if RECON_DRY_RUN:
                tg_send(f"🧪 DRY_RUN: set reduceOnly {symbol} order={oid}")
                log_event("recon", "ro_set_dry", symbol, "MAIN", {"orderId": oid})
                continue
            ok, _, err = by.amend_order(category="linear", symbol=symbol, orderId=oid, reduceOnly=True)
            if not ok:
                log.warning("set RO fail %s: %s", symbol, err)
                log_event("recon", "ro_set_fail", symbol, "MAIN", {"orderId": oid, "err": str(err)}, level="warn")
            else:
                log_event("recon", "ro_set_ok", symbol, "MAIN", {"orderId": oid})
        except Exception:
            continue

def _amend_qty(by: Bybit, symbol: str, order_id: str, new_qty: Decimal) -> bool:
    qtxt = f"{new_qty.normalize()}"
    if RECON_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: amend qty {symbol} order={order_id} -> {qtxt}")
        log_event("recon", "amend_qty_dry", symbol, "MAIN", {"orderId": order_id, "qty": float(new_qty)})
        return True
    ok, _, err = by.amend_order(category="linear", symbol=symbol, orderId=order_id, qty=qtxt)
    if not ok:
        log.warning("amend qty fail %s: %s", symbol, err)
        log_event("recon", "amend_qty_fail", symbol, "MAIN", {"orderId": order_id, "err": str(err)}, level="warn")
        return False
    log_event("recon", "amend_qty_ok", symbol, "MAIN", {"orderId": order_id, "qty": float(new_qty)})
    return True

def _cancel(by: Bybit, symbol: str, order_id: str, link_id: Optional[str]) -> None:
    if (not _link_is_ours(link_id)) and not MANAGE_UNTAGGED:
        return
    if RECON_DRY_RUN:
        tg_send(f"🧪 DRY_RUN: cancel {symbol} order={order_id}")
        log_event("recon", "cancel_dry", symbol, "MAIN", {"orderId": order_id})
        return
    ok, _, err = by.cancel_order(category="linear", symbol=symbol, orderId=order_id)
    if not ok:
        log.warning("cancel fail %s: %s", symbol, err)
        log_event("recon", "cancel_fail", symbol, "MAIN", {"orderId": order_id, "err": str(err)}, level="warn")
    else:
        log_event("recon", "cancel_ok", symbol, "MAIN", {"orderId": order_id})

def _trim_tp_to_position(by: Bybit, symbol: str, side_word: str, pos_qty: Decimal, step: Decimal) -> None:
    """Ensure sum of our reduce-only limit exits ≤ current position. Shrink newest-first."""
    close_side = _close_side(side_word)
    orders = [o for o in _open_orders(by, symbol) if (o.get("orderType") == "Limit" and o.get("side") == close_side)]
    _enforce_reduce_only(by, symbol, orders)

    total = _sum_reduce_only_limits(orders, close_side)
    excess = total - pos_qty
    if excess <= 0:
        return

    # Newest-first shrink to reduce potential price impact on far rungs
    orders_sorted = sorted(orders, key=lambda r: int(r.get("createdTime") or 0), reverse=True)

    for o in orders_sorted:
        if excess <= 0:
            break
        try:
            oid  = o.get("orderId")
            link = o.get("orderLinkId")
            oq   = Decimal(str(o.get("qty") or "0"))
            if oq <= 0:
                _cancel(by, symbol, oid, link); continue

            if oq <= excess or (oq - excess) < step:
                _cancel(by, symbol, oid, link)
                excess -= oq
                continue

            new_q = _round_step(oq - excess, step)
            if new_q <= 0:
                _cancel(by, symbol, oid, link)
                excess -= oq
                continue

            if _amend_qty(by, symbol, oid, new_q):
                excess -= (oq - new_q)
        except Exception as e:
            log.warning("trim error %s: %s", symbol, e)

def _cap_total_orders(by: Bybit, symbol: str) -> None:
    if MAX_ORDERS_PER_SYMBOL <= 0:
        return
    ours = [o for o in _open_orders(by, symbol) if _link_is_ours(o.get("orderLinkId"))]
    if len(ours) <= MAX_ORDERS_PER_SYMBOL:
        return
    # cancel oldest beyond cap
    extras = sorted(ours, key=lambda r: int(r.get("createdTime") or 0))[:len(ours)-MAX_ORDERS_PER_SYMBOL]
    for o in extras:
        _cancel(by, symbol, o.get("orderId"), o.get("orderLinkId"))

# ---------- MFE + ladder ----------
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

    px_cache: Dict[str, float] = {}
    for pos in positions:
        sym  = pos["symbol"]
        side = pos["side"]
        qty  = float(pos["qty"])

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
                mfe_hint_bps=mfe,
            )
        except TypeError:
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
    try:
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
            # Always keep DB in sync
            _mark_orders_from_exchange(by)
            _apply_fills(by)
            positions = _sync_positions(by)

            # If breaker on: skip trading changes (tp_sl_manager handles flatten/cancel)
            blocked, _why = guard_blocking_reason()
            if blocked:
                dt = time.time() - t0
                time.sleep(max(0.5, RECON_INTERVAL_SEC - dt))
                continue

            # Exit hygiene per position
            oo_all = _open_orders(by, None)  # cache to help ownership checks
            for pos in positions:
                try:
                    sym  = pos["symbol"]
                    if not _allowed_symbol(sym):
                        continue

                    qty  = Decimal(str(pos["qty"]))
                    side = pos["side"]
                    raw  = pos.get("raw", {})
                    if side == "Flat" or qty <= 0:
                        # kill our stray reduce-only exits if flat
                        for o in _open_orders(by, sym):
                            if _link_is_ours(o.get("orderLinkId")) and str(o.get("reduceOnly","")).lower() in ("1","true"):
                                _cancel(by, sym, o.get("orderId"), o.get("orderLinkId"))
                        continue

                    if OWNERSHIP_ENFORCED and not _owned_position(sym, raw, oo_all):
                        tg_send(f"🔎 RECON skip untagged {sym} (ownership enforced)")
                        continue

                    _, step, _ = _filters(by, sym)
                    sw = "long" if side == "Long" else "short"
                    _trim_tp_to_position(by, sym, sw, qty, step)
                    _cap_total_orders(by, sym)
                except Exception as e:
                    log.warning("per-position hygiene error %s: %s", pos.get("symbol","?"), e)

            # Ladder rebuild (targets/structure) after hygiene
            _rebuild_ladders(positions, by)

        except Exception as e:
            log.warning("reconcile loop error: %s", e)

        dt = time.time() - t0
        time.sleep(max(0.5, RECON_INTERVAL_SEC - dt))

if __name__ == "__main__":
    main()
