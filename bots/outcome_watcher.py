#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots/outcome_watcher.py — auto-emit outcomes when positions flatten

What it does
- Polls positions (linear) and watches size transitions per symbol.
- On open: records entry meta (side, avgPrice, stopLoss if present, linkId if present).
- On flat: pulls recent reduce-only executions, computes exit VWAP,
  and emits an outcome with realized R using stop distance.

Ownership-aware
- Only tracks positions we "own" (tag present) unless MANAGE_UNTAGGED=true.

Env (core.config.settings)
  HTTP_TIMEOUT_S=10
  OWNERSHIP_ENFORCED=true
  MANAGE_UNTAGGED=false
  OWNERSHIP_SUB_UID=260417078
  OWNERSHIP_STRATEGY=A2
  TP_MANAGED_TAG=B44
  OUTCOME_EXEC_LOOKBACK_SEC=900  # search window for exits (default 15m)
  OUTCOME_POLL_SEC=5

Outputs
- Writes outcomes via core.outcome_bus.emit_outcome(...)

Limitations
- If no stopDist is known (executor metadata or stopLoss), emits pnl_r=None and won=exit>entry by side.
  The online learner will still ingest but R-based updates are stronger if stopDist is present.

"""

from __future__ import annotations
import time, json
from decimal import Decimal, getcontext
from typing import Dict, Optional, Tuple, List

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.outcome_bus import emit_outcome

getcontext().prec = 28
log = bind_context(get_logger("bots.outcome_watcher"), comp="outcome")

# --- env ---
HTTP_TIMEOUT_S   = int(getattr(settings, "HTTP_TIMEOUT_S", 10))
OWNERSHIP_ENF    = str(getattr(settings, "OWNERSHIP_ENFORCED", "true")).lower() in ("1","true","yes","on")
MANAGE_UNTAGGED  = str(getattr(settings, "MANAGE_UNTAGGED", "false")).lower() in ("1","true","yes","on")
SUB_UID          = str(getattr(settings, "OWNERSHIP_SUB_UID", "")).strip()
STRATEGY         = str(getattr(settings, "OWNERSHIP_STRATEGY", "")).strip()
TP_TAG           = str(getattr(settings, "TP_MANAGED_TAG", "B44")).strip() or "B44"
POLL_SEC         = max(3, int(getattr(settings, "OUTCOME_POLL_SEC", 5)))
LOOKBACK_SEC     = max(300, int(getattr(settings, "OUTCOME_EXEC_LOOKBACK_SEC", 900)))  # 5–30 min sensible

STATE = {}  # sym -> dict(entry_px, side_word, pos_idx, link_id, stop_dist)

# --- ownership utils (same heuristics as tp_sl_manager) ---
def _session_id() -> str:
    try:
        import os
        return os.environ.get("B44_SESSION_ID") or time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    except Exception:
        return time.strftime("%Y%m%dT%H%M%S", time.gmtime())

def _build_owner_tag() -> str:
    try:
        from core.order_tag import build_tag
        return build_tag(SUB_UID or "sub", STRATEGY or "strat", _session_id())
    except Exception:
        return f"{TP_TAG}:{SUB_UID or 'sub'}:{STRATEGY or 'strat'}:{_session_id()}"

OWNER_TAG = _build_owner_tag()

def _link_is_ours(link: str | None) -> bool:
    if not link:
        return False
    s = str(link)
    return (TP_TAG in s) or (OWNER_TAG.split(":")[0] in s) or (SUB_UID and SUB_UID in s)

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

# --- side utils ---
def _side_word_from_row(p: dict) -> Optional[str]:
    try:
        side_raw = (p.get("side") or "").lower()
        if side_raw.startswith("b"): return "long"
        if side_raw.startswith("s"): return "short"
    except Exception: pass
    try:
        from decimal import Decimal
        sz = Decimal(p.get("size") or "0")
        if sz > 0: return "long"
        if sz < 0: return "short"
    except Exception: pass
    return None

def _close_side(side_word: str) -> str:
    return "Sell" if side_word == "long" else "Buy"

# --- price helpers ---
def _vwap_exec(price_qty_pairs: List[Tuple[Decimal, Decimal]]) -> Optional[Decimal]:
    if not price_qty_pairs:
        return None
    num = sum(px*q for px, q in price_qty_pairs)
    den = sum(q for _, q in price_qty_pairs)
    if den <= 0:
        return None
    return num / den

# --- core loop ---
by = Bybit()
try: by.sync_time()
except Exception as e: log.warning("time sync failed: %s", e)

def _load_open_orders() -> List[dict]:
    ok, data, err = by.get_open_orders(category="linear", openOnly=True)
    if not ok:
        return []
    return (data.get("result") or {}).get("list") or []

def _load_positions() -> List[dict]:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        log.warning("positions err: %s", err)
        return []
    return (data.get("result") or {}).get("list") or []

def _load_executions(symbol: str, limit: int = 200) -> List[dict]:
    ok, data, err = by._request_private_json("/v5/execution/list", method="POST", body={
        "category":"linear", "symbol":symbol, "limit":str(limit)
    })
    if not ok:
        log.warning("exec err %s: %s", symbol, err)
        return []
    return (data.get("result") or {}).get("list") or []

def _recent_reduce_only(symbol: str, since_ts_ms: int) -> List[dict]:
    rows = _load_executions(symbol, limit=200)
    out = []
    for r in rows:
        try:
            ts = int(r.get("execTime") or r.get("execTimeNs", "0")[:13])
            if ts < since_ts_ms: continue
            if str(r.get("isMaker","")).lower() not in {"true","1","false","0"}:
                pass
            if str(r.get("isLeverage","")).lower() not in {"true","1","false","0"}:
                pass
            # Bybit doesn’t always echo reduceOnly on execs; infer from orderLinkId or side vs position direction later.
            # Keep all regular trades; we’ll filter by side at vwap time.
            out.append(r)
        except Exception:
            continue
    return out

def _entry_meta_from_pos(p: dict) -> Tuple[Optional[Decimal], Optional[str], Optional[int], Optional[str], Optional[Decimal]]:
    from decimal import Decimal
    try:
        entry = Decimal(str(p.get("avgPrice") or "0"))
        if entry <= 0: entry = None
    except Exception:
        entry = None
    side = _side_word_from_row(p)
    try:
        pos_idx = int(p.get("positionIdx") or 0)
    except Exception:
        pos_idx = None
    link = None
    for k in ("lastOrderLinkId","last_exec_link_id","positionTag","comment"):
        v = p.get(k)
        if v:
            link = str(v); break
    stop_d = None
    try:
        sl = p.get("stopLoss")
        if sl:
            sl_d = Decimal(str(sl))
            if entry:
                stop_d = abs(entry - sl_d)
    except Exception:
        stop_d = None
    return entry, side, pos_idx, link, stop_d

def _emit(symbol: str, entry_px: Decimal, side_word: str, exit_px: Decimal, link_id: Optional[str], stop_dist: Optional[Decimal]) -> None:
    # Compute R if we have stop distance
    pnl_r = None
    won = False
    try:
        if stop_dist and stop_dist > 0:
            signed = (exit_px - entry_px) if side_word == "long" else (entry_px - exit_px)
            pnl_r = float(signed / stop_dist)
            won = pnl_r > 0
        else:
            won = (exit_px > entry_px) if side_word == "long" else (exit_px < entry_px)
    except Exception:
        pass
    setup_tag = "Unknown"
    # If executor saved setupTag in the metadata file, we’ll pick it up from STATE
    meta = STATE.get(symbol, {})
    setup_tag = meta.get("setup_tag") or setup_tag

    emit_outcome(
        link_id=link_id or f"{TP_TAG}-{symbol}",
        symbol=symbol,
        setup_tag=setup_tag,
        pnl_r=(pnl_r if pnl_r is not None else (1.0 if won else -1.0)),
        won=bool(won),
        features={
            "entry_px": float(entry_px),
            "exit_px": float(exit_px),
            "side": side_word,
            "stop_dist": (float(stop_dist) if stop_dist is not None else None),
        }
    )
    log.info("outcome %s side=%s entry=%.8f exit=%.8f R=%s", symbol, side_word, float(entry_px), float(exit_px), ("%.3f" % pnl_r) if pnl_r is not None else "NA")

def _load_executor_meta() -> Dict[str, dict]:
    """
    Optional helper: read .state/entries_meta.json if executor writes it
    to enrich STATE with setup_tag and stop_dist at open time.
    """
    from pathlib import Path
    import os, json
    root = settings.ROOT
    p = root / ".state" / "entries_meta.json"
    try:
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def sweep_once():
    # opportunistically refresh executor meta map
    meta_map = _load_executor_meta()

    orders = _load_open_orders()
    positions = _load_positions()

    seen_symbols = set()
    now_ms = int(time.time()*1000)

    for p in positions:
        try:
            symbol = (p.get("symbol") or "").upper()
            if not symbol: continue
            seen_symbols.add(symbol)

            size = Decimal(str(p.get("size") or "0"))
            owned = True
            if OWNERSHIP_ENF:
                owned = _position_owned(symbol, p, orders)
            if not owned:
                # if we had it tracked but it’s not ours, forget it
                if symbol in STATE: STATE.pop(symbol, None)
                continue

            if size > 0:
                # opening or still open
                entry, side, pos_idx, link, stop_d = _entry_meta_from_pos(p)
                prev = STATE.get(symbol)
                if not prev:
                    # new open
                    info = {"entry_px": entry, "side": side, "pos_idx": pos_idx, "link_id": link, "stop_dist": stop_d,
                            "setup_tag": None, "opened_at": now_ms}
                    # enrich from executor meta if present
                    if meta_map:
                        # meta keyed by link_id if available, else by symbol fallback
                        key = link or symbol
                        if key in meta_map:
                            mm = meta_map[key]
                            info["setup_tag"] = mm.get("setup_tag") or info["setup_tag"]
                            if (not stop_d) and mm.get("stop_dist"):
                                try: info["stop_dist"] = Decimal(str(mm["stop_dist"]))
                                except Exception: pass
                    STATE[symbol] = info
                else:
                    # update dynamic fields
                    if entry: prev["entry_px"] = entry
                    if side:  prev["side"] = side
                    if stop_d: prev["stop_dist"] = stop_d
                    if link and not prev.get("link_id"): prev["link_id"] = link
                    # keep setup_tag if we pick it up later
            else:
                # flat; if we had state, emit outcome
                prev = STATE.pop(symbol, None)
                if prev and prev.get("entry_px") and prev.get("side"):
                    # collect executions within lookback and compute exit vwap for the closing side
                    rows = _recent_reduce_only(symbol, since_ts_ms=now_ms - LOOKBACK_SEC*1000)
                    side_close = _close_side(prev["side"])
                    px_qty: List[Tuple[Decimal, Decimal]] = []
                    for r in rows:
                        try:
                            if (r.get("symbol") or "").upper() != symbol: continue
                            if (r.get("side") or "").capitalize() != side_close: continue
                            q = Decimal(str(r.get("execQty") or "0"))
                            if q <= 0: continue
                            px = Decimal(str(r.get("execPrice") or "0"))
                            if px <= 0: continue
                            px_qty.append((px, q))
                        except Exception:
                            continue
                    exit_px = _vwap_exec(px_qty) or prev["entry_px"]  # fallback to entry if we can't find exits
                    _emit(symbol, prev["entry_px"], prev["side"], exit_px, prev.get("link_id"), prev.get("stop_dist"))
        except Exception as e:
            log.warning("row err: %s", e)

    # clean any symbols no longer present (edge cases)
    for sym in list(STATE.keys()):
        if sym not in seen_symbols:
            # treat as flat without exec info
            prev = STATE.pop(sym, None)
            if prev and prev.get("entry_px") and prev.get("side"):
                _emit(sym, prev["entry_px"], prev["side"], prev["entry_px"], prev.get("link_id"), prev.get("stop_dist"))

def main():
    log.info("Outcome watcher online: poll=%ss lookback=%ss own_enf=%s", POLL_SEC, LOOKBACK_SEC, OWNERSHIP_ENF)
    while True:
        try:
            time.sleep(max(3, POLL_SEC))
            sweep_once()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.warning("loop error: %s", e)
            time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
