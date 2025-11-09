#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/spread_sanity.py — fast, resilient spread/depth sanity check

Why this exists
- Your executor already checks spread using tickers. That’s fine until the book flickers,
  your relay is grumpy, or some meme coin is 35 bps wide and waiting to mug you.
- This module centralizes the check with retries, per-symbol overrides, soft depth screening,
  and clean telemetry so you can reuse it across bots.

Public API
    check(symbol: str) -> tuple[bool, dict, str]
        ok:     True if entry should be allowed
        stats:  { "bid":float, "ask":float, "mid":float, "spread_bps":float, "depth_usd":float|None,
                  "max_bps":float, "min_depth_usd":float|None, "source":"orderbook|ticker" }
        reason: empty if ok, else short block reason

CLI
    python -m core.spread_sanity --symbol BTCUSDT --json
    python -m core.spread_sanity --symbol HBARUSDT

Env knobs (all optional)
    # Global thresholds
    SIG_SPREAD_MAX_BPS=8                  # soft default used by executor too
    SPREAD_MAX_BPS=                        # if set, overrides globally
    SPREAD_WARN_BPS=15                     # warn level to notify but not block (<= max still blocks)
    SPREAD_MIN_DEPTH_USD=0                 # if >0 and depth estimate < this, block
    SPREAD_SOURCE=ticker                   # "orderbook" | "ticker" | "auto"
    SPREAD_RETRIES=2                       # quick retries on transient errors
    SPREAD_TIMEOUT_MS=800
    SPREAD_COOLDOWN_SEC=10                 # minimum seconds between identical warn notifications

    # Per-symbol overrides (CSV of RULES; first match wins)
    # Example: BTCUSDT:10;ETHUSDT:12;PUMPFUNUSDT:20@depth=5000
    SPREAD_RULES="SYMBOL:MAX_BPS[@depth=USD];SYMBOL:MAX_BPS;..."

    # Relay base (read-only public endpoints)
    RELAY_BASE=http://127.0.0.1:5000

Notes
- Depth is a cheap approximation using top-of-book size multiplied by mid; if your relay exposes
  a proper depth endpoint you can wire it later without touching call sites.
- If everything fails, we prefer to BLOCK rather than YOLO through fog.
"""

from __future__ import annotations
import os, time, json, math
from typing import Optional, Tuple, Dict

import urllib.request
import urllib.parse

# Optional notifier/decision-log are best-effort
try:
    from core.notifier_bot import tg_send  # type: ignore
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):  # type: ignore
        pass

try:
    from core.decision_log import log_event  # type: ignore
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass


# ---------- env ----------
def _env_float(k: str, default: float) -> float:
    try:
        return float(os.getenv(k, str(default)))
    except Exception:
        return default

def _env_int(k: str, default: int) -> int:
    try:
        return int(os.getenv(k, str(default)))
    except Exception:
        return default

RELAY_BASE      = (os.getenv("RELAY_BASE") or "http://127.0.0.1:5000").rstrip("/")
SOURCE_MODE     = (os.getenv("SPREAD_SOURCE") or "auto").strip().lower()  # auto|orderbook|ticker
TIMEOUT_MS      = _env_int("SPREAD_TIMEOUT_MS", 800)
RETRIES         = _env_int("SPREAD_RETRIES", 2)
COOLDOWN_SEC    = _env_int("SPREAD_COOLDOWN_SEC", 10)

# thresholds
DEFAULT_MAX_BPS = _env_float("SPREAD_MAX_BPS", float(os.getenv("SIG_SPREAD_MAX_BPS", "8") or 8.0))
WARN_BPS        = _env_float("SPREAD_WARN_BPS", 15.0)
MIN_DEPTH_USD   = _env_float("SPREAD_MIN_DEPTH_USD", 0.0)

# per-symbol rules
# Format: "BTCUSDT:10;ETHUSDT:12;PUMPFUNUSDT:20@depth=5000"
_RULES_RAW = (os.getenv("SPREAD_RULES") or "").strip()

def _parse_rules(raw: str) -> list[dict]:
    rules: list[dict] = []
    if not raw:
        return rules
    for part in raw.split(";"):
        p = part.strip()
        if not p:
            continue
        # SYMBOL:MAX[@depth=USD]
        try:
            sym_vs = p.split(":")
            sym = sym_vs[0].strip().upper()
            rest = sym_vs[1].strip()
            max_bps = float(rest.split("@")[0])
            depth_req = None
            if "@depth=" in rest:
                try:
                    depth_req = float(rest.split("@depth=")[1])
                except Exception:
                    depth_req = None
            rules.append({"symbol": sym, "max_bps": max_bps, "min_depth_usd": depth_req})
        except Exception:
            continue
    return rules

RULES = _parse_rules(_RULES_RAW)

# ---------- http helpers ----------
def _http_get(path: str, params: dict) -> tuple[bool, dict, str, dict]:
    q = urllib.parse.urlencode(params)
    url = f"{RELAY_BASE}{path}?{q}"
    req = urllib.request.Request(url=url, method="GET", headers={
        "User-Agent": "Base44-SpreadSanity/1.0",
        "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
    })
    try:
        with urllib.request.urlopen(req, timeout=max(0.1, TIMEOUT_MS/1000.0)) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            ct = resp.headers.get("content-type","")
    except Exception as e:
        return False, {}, f"http:{e}", {}
    try:
        data = json.loads(raw) if raw.strip().startswith("{") or raw.strip().startswith("[") else {}
    except Exception:
        data = {}
    meta = {"ct": ct}
    return True, data, "", meta

def _get_orderbook_top(symbol: str) -> Optional[tuple[float,float,float,float]]:
    """
    Returns bid, ask, bidSz, askSz (floats). None on failure.
    """
    ok, data, err, _ = _http_get("/v5/market/orderbook", {"category":"linear", "symbol":symbol, "limit":"1"})
    if not ok:
        return None
    result = (data.get("result") or {})
    bids = result.get("b") or result.get("bids") or []
    asks = result.get("a") or result.get("asks") or []
    if not bids or not asks:
        return None
    try:
        bid_px = float(bids[0][0]); bid_sz = float(bids[0][1])
        ask_px = float(asks[0][0]); ask_sz = float(asks[0][1])
        if bid_px <= 0 or ask_px <= 0:
            return None
        return bid_px, ask_px, bid_sz, ask_sz
    except Exception:
        return None

def _get_ticker(symbol: str) -> Optional[tuple[float,float]]:
    ok, data, err, _ = _http_get("/v5/market/tickers", {"category":"linear", "symbol":symbol})
    if not ok:
        return None
    lst = ((data.get("result") or {}).get("list") or [])
    if not lst:
        return None
    try:
        bid = float(lst[0].get("bid1Price")); ask = float(lst[0].get("ask1Price"))
        if bid <= 0 or ask <= 0:
            return None
        return bid, ask
    except Exception:
        return None

# ---------- logic ----------
def _spread_bps(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 1e9
    return (ask - bid) / mid * 10000.0

def _depth_usd_estimate(bid: float, ask: float, bid_sz: float, ask_sz: float) -> float:
    # crude mid-depth proxy at top of book
    mid = (bid + ask) / 2.0
    return max(0.0, mid * max(0.0, min(bid_sz, ask_sz)))

_last_warn: dict[str, float] = {}

def _maybe_warn(key: str, msg: str, priority: str = "warn"):
    now = time.time()
    last = _last_warn.get(key, 0.0)
    if now - last >= COOLDOWN_SEC:
        tg_send(msg, priority=priority)
        _last_warn[key] = now

def _apply_rules(symbol: str) -> tuple[float, Optional[float]]:
    # return max_bps, min_depth_usd
    sym = symbol.upper()
    for r in RULES:
        if r.get("symbol") == sym:
            return float(r.get("max_bps", DEFAULT_MAX_BPS)), (r.get("min_depth_usd"))
    return DEFAULT_MAX_BPS, (MIN_DEPTH_USD if MIN_DEPTH_USD > 0 else None)

def check(symbol: str) -> Tuple[bool, Dict, str]:
    symbol = symbol.upper().strip()
    max_bps, depth_req = _apply_rules(symbol)

    bid = ask = mid = spread = None
    depth_usd = None
    source_used = None

    attempts = max(1, RETRIES + 1)
    for i in range(attempts):
        # choose source
        if SOURCE_MODE == "orderbook" or SOURCE_MODE == "auto":
            ob = _get_orderbook_top(symbol)
            if ob:
                b, a, bs, as_ = ob
                bid, ask = b, a
                spread = _spread_bps(bid, ask)
                depth_usd = _depth_usd_estimate(bid, ask, bs, as_)
                source_used = "orderbook"
        if (bid is None or ask is None) and (SOURCE_MODE in ("ticker","auto")):
            tk = _get_ticker(symbol)
            if tk:
                bid, ask = tk
                spread = _spread_bps(bid, ask)
                source_used = "ticker"
        if bid is not None and ask is not None:
            break
        # tiny backoff
        time.sleep(0.15)

    if bid is None or ask is None or spread is None:
        log_event("executor", "spread_block", symbol, "MAIN", {"error":"no_ob_or_ticker"})
        return False, {"bid":0,"ask":0,"mid":0,"spread_bps":1e9,"depth_usd":None,
                       "max_bps":max_bps,"min_depth_usd":depth_req,"source": source_used or "none"}, "no orderbook/ticker"

    mid = (bid + ask) / 2.0

    # blocking rules
    if spread > max_bps:
        msg = f"spread {spread:.2f} bps > max {max_bps} ({symbol})"
        log_event("executor", "spread_block", symbol, "MAIN", {"spread_bps":spread, "max_bps":max_bps, "src":source_used})
        _maybe_warn(f"wide:{symbol}", f"⛔ Spread too wide • {symbol} {spread:.2f} bps > {max_bps}")
        return False, {"bid":bid,"ask":ask,"mid":mid,"spread_bps":spread,"depth_usd":depth_usd,
                       "max_bps":max_bps,"min_depth_usd":depth_req,"source":source_used}, msg

    if depth_req is not None and depth_usd is not None and depth_usd < depth_req:
        msg = f"depth ${depth_usd:,.0f} < min ${depth_req:,.0f} ({symbol})"
        log_event("executor", "depth_block", symbol, "MAIN", {"depth_usd":depth_usd, "min_depth_usd":depth_req, "src":source_used})
        _maybe_warn(f"shallow:{symbol}", f"⛔ Book too shallow • {symbol} depth ${depth_usd:,.0f} < ${depth_req:,.0f}")
        return False, {"bid":bid,"ask":ask,"mid":mid,"spread_bps":spread,"depth_usd":depth_usd,
                       "max_bps":max_bps,"min_depth_usd":depth_req,"source":source_used}, msg

    # warn-only channel if spread > WARN_BPS but still under max (useful visibility)
    if spread >= WARN_BPS:
        _maybe_warn(f"warn:{symbol}", f"⚠️ Wide spread (warning) • {symbol} {spread:.2f} bps ≤ max {max_bps}", priority="warn")

    return True, {"bid":bid,"ask":ask,"mid":mid,"spread_bps":spread,"depth_usd":depth_usd,
                  "max_bps":max_bps,"min_depth_usd":depth_req,"source":source_used}, ""

# ---------- CLI ----------
def _as_json(d):
    import json as _json
    return _json.dumps(d, indent=2, ensure_ascii=False)

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Spread/depth sanity")
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    ok, stats, reason = check(args.symbol)
    out = {"ok": ok, "reason": reason, "stats": stats}
    if args.json:
        print(_as_json(out))
    else:
        msg = "ALLOW" if ok else "BLOCK"
        s = stats
        depth = f", depth≈${s['depth_usd']:.0f}" if s["depth_usd"] is not None else ""
        print(f"[{msg}] {args.symbol} spread={s['spread_bps']:.2f} bps (max {s['max_bps']}){depth} via {s['source'] or 'n/a'}")
        if reason:
            print(" reason:", reason)

if __name__ == "__main__":
    main()
