#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core.bybit_mock — drop-in test double for core.bybit_client.Bybit

Implements the minimal surface your bots use:
  sync_time()
  get_tickers(category, symbol)
  place_order(...)
  cancel_order(...)
  amend_order(...)
  get_open_orders(category, symbol=None, openOnly=False)
  get_positions(category, symbol=None)
  get_executions(category=None, symbol=None)          # reconciler helper
  _request_private_json(path, params/body/method)     # wallet, trading-stop, executions

Behavior
- Maintains a simple L1 book (bid/ask around a mid), per-symbol.
- Entry orders (reduceOnly=False) fill immediately at limit or mid (IOC semantics) unless chaos says “reject.”
- Reduce-only limit orders rest and fill when price crosses on ticks.
- Positions update average price and size; stopLoss stored per positionIdx.
- Executions recorded with basic fields similar to Bybit payload.
- Chaos knobs via env:
    CHAOS_REJECT_RATE=0.05        # chance to reject place_order
    CHAOS_NETWORK_RATE=0.03       # chance to raise a network-ish error
    CHAOS_PARTIAL_RATE=0.25       # chance a resting TP partially fills on cross
    CHAOS_LATENCY_MS=30           # sleep per call to simulate latency

Use
- The harness monkeypatches core.bybit_client.Bybit = MockBybit before importing your bots.
- You can also force it manually in tests.
"""

from __future__ import annotations
import os, time, random, threading
from typing import Dict, Any, Optional, Tuple, List

def _p(name: str, dflt: float) -> float:
    try: return float(os.getenv(name, dflt))
    except Exception: return dflt

CHAOS_REJECT_RATE  = _p("CHAOS_REJECT_RATE", 0.05)
CHAOS_NET_RATE     = _p("CHAOS_NETWORK_RATE", 0.03)
CHAOS_PARTIAL_RATE = _p("CHAOS_PARTIAL_RATE", 0.25)
CHAOS_LAT_MS       = int(_p("CHAOS_LATENCY_MS", 30))

def _maybe_latency():
    if CHAOS_LAT_MS > 0:
        time.sleep(CHAOS_LAT_MS/1000.0)

def _maybe_netfail():
    if random.random() < CHAOS_NET_RATE:
        raise RuntimeError("mock: transient network")

class _Seq:
    def __init__(self): self.i = 0; self.lock = threading.Lock()
    def next(self) -> str:
        with self.lock:
            self.i += 1
            return f"{int(time.time()*1000)}-{self.i}"

class MockBybit:
    def __init__(self):
        self._lock = threading.RLock()
        self._seq = _Seq()
        # symbol -> state
        self._state: Dict[str, Dict[str, Any]] = {}
        # flat executions log (newest first per bybit list style not guaranteed)
        self._exec: List[Dict[str, Any]] = []
        # wallet equity (fake but sufficient)
        self._equity_usd = 10000.0

    # ---------- boot ----------
    def _ensure_sym(self, sym: str):
        sym = sym.upper()
        with self._lock:
            st = self._state.get(sym)
            if st: return
            mid = 50000.0 if "BTC" in sym else 3000.0
            st = {
                "mid": mid,
                "tick": 0.5 if "BTC" in sym else 0.05,
                "step": 0.001,
                "pos": { "size": 0.0, "avg": 0.0, "side": "None", "positionIdx": 1, "stopLoss": None },
                "orders": {},   # orderId -> row
                "link_to_id": {},  # link -> orderId
            }
            self._state[sym] = st

    # externally called by harness to move price and cross resting orders
    def _tick(self, sym: str, new_mid: float):
        self._ensure_sym(sym)
        with self._lock:
            st = self._state[sym]
            old = st["mid"]
            st["mid"] = float(new_mid)
            bid = new_mid - st["tick"]/2
            ask = new_mid + st["tick"]/2
            # fill resting reduce-only TPs if crossed
            for oid, o in list(st["orders"].items()):
                if not o.get("reduceOnly"): continue
                side = o["side"]
                px   = float(o.get("price") or (ask if side=="Sell" else bid))
                qty  = float(o["qty"])
                if side == "Sell" and px <= bid:
                    self._fill(sym, oid, px, qty, partial=True)
                elif side == "Buy" and px >= ask:
                    self._fill(sym, oid, px, qty, partial=True)

    # ---------- public-ish API ----------
    def sync_time(self): _maybe_latency(); return True

    def get_tickers(self, *, category: str, symbol: Optional[str]=None):
        _maybe_latency(); _maybe_netfail()
        if symbol:
            self._ensure_sym(symbol)
            st = self._state[symbol.upper()]
            bid = st["mid"] - st["tick"]/2
            ask = st["mid"] + st["tick"]/2
            return True, {"result":{"list":[{"symbol":symbol.upper(),"bid1Price":str(bid),"ask1Price":str(ask)}]}}, ""
        return True, {"result":{"list":[]}}, ""

    def _gen_order_row(self, sym: str, req: Dict[str, Any]) -> Dict[str, Any]:
        oid = self._seq.next()
        row = {
            "orderId": oid,
            "orderLinkId": req.get("orderLinkId") or "",
            "symbol": sym,
            "side": req.get("side"),
            "orderType": req.get("orderType"),
            "price": req.get("price"),
            "qty": req.get("qty"),
            "reduceOnly": bool(req.get("reduceOnly")),
            "timeInForce": req.get("timeInForce") or "GoodTillCancel",
            "createdTime": str(int(time.time()*1000)),
        }
        return row

    def place_order(self, **req):
        _maybe_latency(); _maybe_netfail()
        sym = req.get("symbol","").upper()
        self._ensure_sym(sym)
        if random.random() < CHAOS_REJECT_RATE:
            return False, {}, "mock: rejected"
        with self._lock:
            st = self._state[sym]
            row = self._gen_order_row(sym, req)
            if not row["reduceOnly"]:
                # entry: immediate fill at limit or mid (IOC-ish)
                bid = st["mid"] - st["tick"]/2
                ask = st["mid"] + st["tick"]/2
                px  = float(row["price"]) if row["price"] else (ask if row["side"]=="Buy" else bid)
                qty = float(row["qty"])
                self._fill(sym, row["orderId"], px, qty, entry=True, side=row["side"])
                return True, {"result":{"orderId":row["orderId"]}}, ""
            # reduce-only limit: park it
            st["orders"][row["orderId"]] = row
            if row.get("orderLinkId"):
                st["link_to_id"][row["orderLinkId"]] = row["orderId"]
            return True, {"result":{"orderId":row["orderId"]}}, ""

    def amend_order(self, **req):
        _maybe_latency(); _maybe_netfail()
        oid = req.get("orderId")
        symbol = req.get("symbol")
        if not symbol:  # sometimes amend by id only
            for sym, st in self._state.items():
                if oid in st["orders"]:
                    symbol = sym; break
        if not symbol: return False, {}, "mock: not found"
        st = self._state[symbol]
        row = st["orders"].get(oid)
        if not row: return False, {}, "mock: not found"
        if "price" in req and req["price"]:
            row["price"] = str(req["price"])
        if "qty" in req and req["qty"]:
            row["qty"] = str(req["qty"])
        if "reduceOnly" in req:
            row["reduceOnly"] = bool(req["reduceOnly"])
        return True, {"result":{"orderId":oid}}, ""

    def cancel_order(self, *, category: str, symbol: str, orderId: Optional[str]=None, orderLinkId: Optional[str]=None):
        _maybe_latency(); _maybe_netfail()
        self._ensure_sym(symbol)
        with self._lock:
            st = self._state[symbol]
            oid = orderId or st["link_to_id"].get(orderLinkId or "")
            if not oid or oid not in st["orders"]:
                return False, {}, "mock: not found"
            st["orders"].pop(oid, None)
            return True, {"result":{"orderId":oid}}, ""

    def get_open_orders(self, *, category: str, symbol: Optional[str]=None, openOnly: bool=False):
        _maybe_latency(); _maybe_netfail()
        out = []
        with self._lock:
            if symbol:
                self._ensure_sym(symbol); items = self._state[symbol]["orders"].values()
                out.extend(list(items))
            else:
                for st in self._state.values():
                    out.extend(list(st["orders"].values()))
        return True, {"result":{"list": list(out)}}, ""

    def get_positions(self, *, category: str, symbol: Optional[str]=None):
        _maybe_latency(); _maybe_netfail()
        out = []
        with self._lock:
            syms = [symbol.upper()] if symbol else list(self._state.keys())
            for s in syms:
                self._ensure_sym(s)
                st = self._state[s]
                pos = st["pos"]
                side = "Buy" if pos["size"] > 0 else ("Sell" if pos["size"] < 0 else "None")
                out.append({
                    "symbol": s,
                    "size": str(abs(pos["size"])),
                    "side": side,
                    "avgPrice": str(pos["avg"] or 0.0),
                    "positionIdx": pos["positionIdx"],
                    "stopLoss": pos.get("stopLoss"),
                    "lastOrderLinkId": "",  # optional
                })
        return True, {"result":{"list": out}}, ""

    def get_executions(self, *, category: str, symbol: Optional[str]=None):
        _maybe_latency(); _maybe_netfail()
        with self._lock:
            rows = [e for e in self._exec if (not symbol or e.get("symbol")==symbol)]
        return True, {"result":{"list": rows[-200:]}}, ""

    # Private generic used by your code for wallet, execution list, trading-stop
    def _request_private_json(self, path: str, *, params: Optional[dict]=None, body: Optional[dict]=None, method: str="GET"):
        _maybe_latency(); _maybe_netfail()
        if path == "/v5/account/wallet-balance":
            return True, {"result":{"list":[{"accountType":"UNIFIED","totalEquity":str(self._equity_usd)}]}}, ""
        if path == "/v5/execution/list":
            symbol = (body or {}).get("symbol") or (params or {}).get("symbol")
            ok, data, err = self.get_executions(category="linear", symbol=symbol)
            return ok, data, err
        if path == "/v5/position/trading-stop":
            sym = (body or {}).get("symbol"); posidx = int((body or {}).get("positionIdx") or 1)
            stop = (body or {}).get("stopLoss")
            self._ensure_sym(sym)
            with self._lock:
                self._state[sym]["pos"]["stopLoss"] = stop
            return True, {"result":{}}, ""
        return False, {}, f"mock: unsupported path {path}"

    # ---------- internals ----------
    def _fill(self, sym: str, oid: str, price: float, qty: float, *, entry: bool=False, side: Optional[str]=None, partial: bool=False):
        st = self._state[sym]
        pos = st["pos"]
        side = side or ("Sell" if entry and False else "Buy")
        fill_qty = qty
        if partial and random.random() < CHAOS_PARTIAL_RATE:
            fill_qty = max(0.0, qty * random.uniform(0.1, 0.6))
            # shrink resting order
            row = st["orders"].get(oid)
            if row:
                try:
                    left = max(0.0, float(row["qty"]) - fill_qty)
                    if left <= 0.0:
                        st["orders"].pop(oid, None)
                    else:
                        row["qty"] = str(left)
                except Exception:
                    st["orders"].pop(oid, None)
        else:
            # full consume resting order
            st["orders"].pop(oid, None)

        # execution row
        e = {
            "symbol": sym,
            "side": side,
            "execPrice": str(price),
            "execQty": str(fill_qty),
            "execFee": "0.0",
            "orderLinkId": "",  # may be empty in mock
            "orderId": oid,
            "execTime": str(int(time.time()*1000)),
            "isMaker": "true",
        }
        self._exec.append(e)

        # update position (reduceOnly => close)
        if entry:
            # Buy increases size, Sell decreases size (negative)
            sgn = 1.0 if side=="Buy" else -1.0
            new_qty = pos["size"] + sgn*fill_qty
            if abs(pos["size"]) <= 1e-9:
                pos["avg"] = price
            else:
                # moving average when adding to position
                if sgn * pos["size"] >= 0:  # same direction
                    pos["avg"] = (abs(pos["size"])*pos["avg"] + fill_qty*price) / (abs(pos["size"])+fill_qty)
                else:
                    # reducing/flip: if crossing zero, new avg = price
                    if abs(fill_qty) > abs(pos["size"]):
                        pos["avg"] = price
            pos["size"] = new_qty
        else:
            # reduce-only: move toward zero
            sgn = -1.0 if side=="Sell" else 1.0   # close long with Sell, short with Buy
            pos["size"] += sgn*fill_qty
            if abs(pos["size"]) <= 1e-9:
                pos["size"] = 0.0
                pos["avg"] = 0.0
