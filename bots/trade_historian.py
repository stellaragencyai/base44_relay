# bots/trade_historian.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade Historian (monitor-only)
- Polls order history + live open orders + executions through the Base44 relay.
- Logs:
    • ./logs/orders/YYYY-MM-DD_orders.csv
    • ./logs/trades/YYYY-MM-DD_execs.csv
    • ./logs/trades/YYYY-MM-DD_rr_estimates.csv
- Deduplicates by orderId/executionId using a local .state file.
- R:R estimate (best-effort):
    If a position has a valid SL and at least one TP for that symbol/side,
    R:R is estimated as |avg(TP prices − entry)| / |entry − SL|.

Env knobs (optional):
  HIST_POLL_SEC=20
  HIST_CATEGORY=linear
  HIST_SETTLE_COIN=USDT
  HIST_ORDER_LIMIT=200
  STATE_DIR=.state
"""

import os, sys, csv, time, json
from pathlib import Path
from datetime import datetime, timezone

# ── Robust import: add project root, then import from core package
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.base44_client import (  # type: ignore
    get_order_history, get_open_orders, get_execution_list,
    get_positions, tg_send
)

POLL = int(os.getenv("HIST_POLL_SEC", "20"))
CATEGORY = os.getenv("HIST_CATEGORY", "linear")
SETTLE_COIN = os.getenv("HIST_SETTLE_COIN", "USDT")
ORDER_LIMIT = int(os.getenv("HIST_ORDER_LIMIT", "200"))

BASE_DIR = PROJECT_ROOT
LOG_DIR_ORD = BASE_DIR / "logs" / "orders"
LOG_DIR_TRD = BASE_DIR / "logs" / "trades"
STATE_DIR = Path(os.getenv("STATE_DIR", ".state"))
LOG_DIR_ORD.mkdir(parents=True, exist_ok=True)
LOG_DIR_TRD.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

SEEN_ORDERS_PATH = STATE_DIR / "historian_seen_orders.json"
SEEN_EXECS_PATH  = STATE_DIR / "historian_seen_execs.json"

ORDER_FIELDS = [
    "ts_iso","symbol","orderId","orderLinkId","side","orderType","qty","price",
    "avgPrice","reduceOnly","status","isLeverage","timeInForce","createdTime","updatedTime"
]
EXEC_FIELDS = [
    "ts_iso","symbol","execId","orderId","orderLinkId","side","execType",
    "qty","price","grossValue","feeRate","fee","isMaker","tradeTime"
]
RR_FIELDS = [
    "ts_iso","symbol","side","entryPrice","stopLoss","avgTpPrice","tpCount","rr_estimate"
]

def _load_seen(path: Path) -> set:
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def _save_seen(path: Path, items: set):
    try:
        path.write_text(json.dumps(sorted(list(items))), encoding="utf-8")
    except Exception:
        pass

def _csv_path(dirpath: Path, prefix: str) -> Path:
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return dirpath / f"{d}_{prefix}.csv"

def _append_csv(path: Path, fields: list, row: dict):
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fields})

def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def fetch_order_history():
    body = get_order_history(category=CATEGORY, limit=ORDER_LIMIT)
    if (body or {}).get("retCode") != 0:
        raise RuntimeError(f"order/history retCode={body.get('retCode')} retMsg={body.get('retMsg')}")
    return (body.get("result") or {}).get("list") or []

def fetch_open_orders():
    body = get_open_orders(category=CATEGORY, openOnly=1)
    if (body or {}).get("retCode") != 0:
        raise RuntimeError(f"order/realtime retCode={body.get('retCode')} retMsg={body.get('retMsg')}")
    return (body.get("result") or {}).get("list") or []

def fetch_executions():
    body = get_execution_list(category=CATEGORY, limit=ORDER_LIMIT)
    if (body or {}).get("retCode") != 0:
        raise RuntimeError(f"execution/list retCode={body.get('retCode')} retMsg={body.get('retMsg')}")
    return (body.get("result") or {}).get("list") or []

def fetch_positions():
    body = get_positions(category=CATEGORY, settleCoin=SETTLE_COIN)
    if (body or {}).get("retCode") != 0:
        raise RuntimeError(f"position/list retCode={body.get('retCode')} retMsg={body.get('retMsg')}")
    return (body.get("result") or {}).get("list") or []

def avg_tp_price_for(symbol: str, side: str, open_orders: list):
    """Compute average TP trigger/limit price on opposite side reduce-only or take_profit stop types."""
    opp = "Sell" if (side or "").lower() == "buy" else "Buy"
    prices = []
    count = 0
    for o in open_orders:
        try:
            if o.get("symbol") != symbol:
                continue
            reduce_only = bool(o.get("reduceOnly"))
            side_o = o.get("side")
            stop_type = (o.get("stopOrderType") or "").lower()
            trig = _safe_float(o.get("triggerPrice"))
            lim = _safe_float(o.get("price"))
            # prioritize triggerPrice if it's a TP-type; else consider limit price on reduce-only opposite orders
            if stop_type in ("take_profit","partial_take_profit") and trig > 0 and side_o == opp:
                prices.append(trig); count += 1
            elif reduce_only and side_o == opp:
                if lim > 0: prices.append(lim); count += 1
        except Exception:
            pass
    if not prices:
        return (0.0, 0)
    return (sum(prices)/len(prices), count)

def estimate_rr(entries: list, sl: float, avg_tp: float) -> float:
    """entries: list of entry prices (we'll average them).
       R:R = |avg_tp - avg_entry| / |avg_entry - sl|  (returns 0 if invalid)"""
    try:
        if not entries or sl <= 0 or avg_tp <= 0:
            return 0.0
        avg_entry = sum(entries) / len(entries)
        num = abs(avg_tp - avg_entry)
        den = abs(avg_entry - sl)
        if den <= 0:
            return 0.0
        return round(num / den, 4)
    except Exception:
        return 0.0

def main():
    print(f"Trade Historian running • poll {POLL}s")
    seen_orders = _load_seen(SEEN_ORDERS_PATH)
    seen_execs  = _load_seen(SEEN_EXECS_PATH)

    while True:
        try:
            ts_iso = datetime.now(timezone.utc).isoformat()

            # 1) Orders (recent history)
            orders = fetch_order_history()
            p_orders = _csv_path(LOG_DIR_ORD, "orders")
            new_orders = 0
            for o in orders:
                oid = o.get("orderId") or ""
                if not oid or oid in seen_orders:
                    continue
                row = {
                    "ts_iso": ts_iso,
                    "symbol": o.get("symbol",""),
                    "orderId": oid,
                    "orderLinkId": o.get("orderLinkId",""),
                    "side": o.get("side",""),
                    "orderType": o.get("orderType",""),
                    "qty": o.get("qty",""),
                    "price": o.get("price",""),
                    "avgPrice": o.get("avgPrice",""),
                    "reduceOnly": o.get("reduceOnly",""),
                    "status": o.get("orderStatus") or o.get("status",""),
                    "isLeverage": o.get("isLeverage",""),
                    "timeInForce": o.get("timeInForce",""),
                    "createdTime": o.get("createdTime",""),
                    "updatedTime": o.get("updatedTime",""),
                }
                _append_csv(p_orders, ORDER_FIELDS, row)
                seen_orders.add(oid); new_orders += 1

            if new_orders:
                _save_seen(SEEN_ORDERS_PATH, seen_orders)
                print(f"[orders] logged {new_orders} new orders")

            # 2) Executions (fills)
            execs = fetch_executions()
            p_execs = _csv_path(LOG_DIR_TRD, "execs")
            new_execs = 0
            for e in execs:
                xid = e.get("execId") or ""
                if not xid or xid in seen_execs:
                    continue
                row = {
                    "ts_iso": ts_iso,
                    "symbol": e.get("symbol",""),
                    "execId": xid,
                    "orderId": e.get("orderId",""),
                    "orderLinkId": e.get("orderLinkId",""),
                    "side": e.get("side",""),
                    "execType": e.get("execType",""),
                    "qty": e.get("execQty") or e.get("lastTradedQty") or "",
                    "price": e.get("execPrice") or e.get("lastTradedPrice") or "",
                    "grossValue": e.get("execValue") or e.get("grossValue") or "",
                    "feeRate": e.get("feeRate",""),
                    "fee": e.get("execFee") or e.get("fee") or "",
                    "isMaker": e.get("isMaker",""),
                    "tradeTime": e.get("execTime") or e.get("tradeTime") or "",
                }
                _append_csv(p_execs, EXEC_FIELDS, row)
                seen_execs.add(xid); new_execs += 1

            if new_execs:
                _save_seen(SEEN_EXECS_PATH, seen_execs)
                print(f"[execs] logged {new_execs} new fills")

            # 3) R:R snapshot (best-effort)
            pos = fetch_positions()
            oo = fetch_open_orders()
            rr_rows = 0
            for p in pos:
                try:
                    sz = _safe_float(p.get("size"))
                    if sz <= 0:
                        continue
                    symbol = p.get("symbol")
                    side = p.get("side")
                    entry = _safe_float(p.get("avgPrice")) or _safe_float(p.get("entryPrice"))
                    sl = _safe_float(p.get("stopLoss"))
                    if entry <= 0 or sl <= 0:
                        continue
                    avg_tp, tp_count = avg_tp_price_for(symbol, side, oo)
                    if tp_count <= 0:
                        continue
                    rr = estimate_rr([entry], sl, avg_tp)
                    rr_row = {
                        "ts_iso": ts_iso,
                        "symbol": symbol,
                        "side": side,
                        "entryPrice": f"{entry:.10f}",
                        "stopLoss": f"{sl:.10f}",
                        "avgTpPrice": f"{avg_tp:.10f}",
                        "tpCount": tp_count,
                        "rr_estimate": rr
                    }
                    p_rr = _csv_path(LOG_DIR_TRD, "rr_estimates")
                    _append_csv(p_rr, RR_FIELDS, rr_row)
                    rr_rows += 1
                except Exception:
                    pass

            if rr_rows:
                print(f"[rr] wrote {rr_rows} RR estimates")

            time.sleep(POLL)

        except Exception as e:
            print(f"[historian] error: {e}")
            time.sleep(POLL)

if __name__ == "__main__":
    main()
