#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Linear Futures — Risk Tiers Display
Bucket coins by leverage tier and show each coin's max position size (USD notional) at that tier.

What it does:
- Gets all linear instruments (USDT/USDC) via /v5/market/instruments-info
- Gets ALL linear risk-limit rows via /v5/market/risk-limit (paged)
- For each symbol, reads its multiple tiers: (maxLeverage, riskLimitValue)
- Builds buckets by leverage and prints:  === 75× ===  SYMBOL | $riskLimitValue
- Optional --quote filter: USDT, USDC, ANY
- Optional --status filter: Trading (default)

Install:
  pip install requests

Run:
  python bybit_risk_tiers_by_leverage.py --quote ANY

Notes:
- riskLimitValue for linear contracts is the position limit in USD notional (per Bybit docs).
- As leverage decreases, allowed notional typically increases (auto risk limit system).
- This is the headline limit; actual brackets and maintenance margin still apply.
"""

import argparse
import sys
import time
from typing import Any, Dict, List, Tuple
import requests

BASE = "https://api.bybit.com"

def get_all_linear_instruments(status: str = "Trading") -> List[Dict[str, Any]]:
    url = f"{BASE}/v5/market/instruments-info"
    out = []
    cursor = None
    params = {"category": "linear", "status": status, "limit": 1000}
    s = requests.Session()
    while True:
        if cursor:
            params["cursor"] = cursor
        r = s.get(url, params=params, timeout=20)
        r.raise_for_status()
        j = r.json()
        if str(j.get("retCode")) != "0":
            raise RuntimeError(f"Bybit instruments error: {j.get('retCode')} - {j.get('retMsg')}")
        res = (j.get("result") or {})
        out.extend(res.get("list") or [])
        cursor = res.get("nextPageCursor")
        if not cursor:
            break
        time.sleep(0.15)
    return out

def get_all_linear_risk_limits() -> List[Dict[str, Any]]:
    """
    Pull every linear risk-limit row across all symbols using pagination.
    Each row includes: symbol, riskLimitValue, maxLeverage, initial/maintenance margin, etc.
    """
    url = f"{BASE}/v5/market/risk-limit"
    out = []
    cursor = None
    params = {"category": "linear"}
    s = requests.Session()
    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor
        r = s.get(url, params=p, timeout=20)
        r.raise_for_status()
        j = r.json()
        if str(j.get("retCode")) != "0":
            raise RuntimeError(f"Bybit risk-limit error: {j.get('retCode')} - {j.get('retMsg')}")
        res = (j.get("result") or {})
        out.extend(res.get("list") or [])
        cursor = res.get("nextPageCursor")
        if not cursor:
            break
        time.sleep(0.15)
    return out

def ffloat(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def main():
    ap = argparse.ArgumentParser(description="List Bybit linear futures by leverage tier with per-coin max USD position size.")
    ap.add_argument("--quote", type=str, default="ANY", help="USDT, USDC, or ANY (default ANY)")
    ap.add_argument("--status", type=str, default="Trading", help="Instrument status filter (default Trading)")
    args = ap.parse_args()

    # 1) Instruments
    instruments = get_all_linear_instruments(status=args.status)
    q = args.quote.upper()
    if q not in ("USDT", "USDC", "ANY"):
        q = "ANY"
    if q != "ANY":
        instruments = [r for r in instruments if r.get("quoteCoin") == q]
    else:
        instruments = [r for r in instruments if r.get("quoteCoin") in ("USDT", "USDC")]

    symbol_set = {r.get("symbol") for r in instruments if r.get("symbol")}
    # Map symbol -> quote for info line
    sym_quote = {r.get("symbol"): r.get("quoteCoin") for r in instruments if r.get("symbol")}

    # 2) Risk limits (all linear) then filter to our symbols
    risk_rows = get_all_linear_risk_limits()
    risk_rows = [r for r in risk_rows if r.get("symbol") in symbol_set]

    # 3) Build buckets: leverage -> list of (symbol, riskLimitUSD)
    # A symbol has multiple rows (tiers). We want each distinct maxLeverage tier.
    buckets: Dict[str, List[Tuple[str, float]]] = {}
    # Some symbols may have duplicate maxLeverage rows with different riskLimitValue; keep the largest notional per leverage.
    best_by_sym_lev: Dict[Tuple[str, str], float] = {}

    for rr in risk_rows:
        sym = rr.get("symbol")
        lev_raw = rr.get("maxLeverage")
        lim_raw = rr.get("riskLimitValue")
        lev = ffloat(lev_raw)
        lim = ffloat(lim_raw)
        if sym is None or lev is None or lim is None:
            continue
        # Format leverage as an integer-like label when clean (75 vs 75.0)
        lev_key = str(int(lev)) if float(lev).is_integer() else str(lev)
        key = (sym, lev_key)
        prev = best_by_sym_lev.get(key)
        if prev is None or lim > prev:
            best_by_sym_lev[key] = lim

    # Move to leverage buckets
    for (sym, lev_key), lim in best_by_sym_lev.items():
        buckets.setdefault(lev_key, []).append((sym, lim))

    # Sort: leverage desc, then symbols alpha; place UNKNOWN if any at end
    def lev_sort_key(k: str):
        try:
            return (0, -float(k))
        except Exception:
            return (1, 0.0)

    ordered_levs = sorted(buckets.keys(), key=lev_sort_key)
    for lev_key in ordered_levs:
        buckets[lev_key].sort(key=lambda x: x[0])

    # 4) Print
    total_pairs = len(symbol_set)
    print(f"\nBybit Linear Perps (quote={q}, status={args.status}) — {total_pairs} symbols")
    print("Max position size shown is USD notional allowed at that leverage tier for each coin.\n")

    for lev_key in ordered_levs:
        rows = buckets[lev_key]
        print(f"=== {lev_key}× — {len(rows)} symbols ===")
        for sym, lim in rows:
            qcoin = sym_quote.get(sym, "")
            print(f"{sym}  |  ${lim:,.2f}  ({qcoin})")
        print()

    print("Notes:")
    print("• Data from /v5/market/risk-limit and /v5/market/instruments-info.")
    print("• Lower leverage tiers generally allow larger notional caps; higher leverage tiers cap notional lower.")
    print("• This is the headline position limit per tier; maintenance/initial margin and auto-risk rules still apply.")

if __name__ == "__main__":
    main()
