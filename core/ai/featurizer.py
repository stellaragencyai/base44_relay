#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Turns raw kline + signal context into a compact numeric dict.
No big deps; just math and Bybit public kline.
"""

from __future__ import annotations
import math, statistics, time
from typing import Dict, List, Tuple
from core.bybit_client import Bybit

_by = Bybit()
try: _by.sync_time()
except Exception: pass

def _fetch_kline(symbol: str, interval: str = "5", limit: int = 120) -> List[List[str]]:
    ok, data, err = _by._request_public("/v5/market/kline",
        params={"category":"linear","symbol":symbol,"interval":str(interval),"limit":str(limit)})
    if not ok: return []
    return (data.get("result") or {}).get("list") or []

def _atr(rows: List[List[str]], n: int = 14) -> float:
    trs: List[float] = []
    prev_close = None
    for it in rows:
        o,h,l,c = map(float, [it[1],it[2],it[3],it[4]])
        if prev_close is not None:
            trs.append(max(h-l, abs(h-prev_close), abs(l-prev_close)))
        prev_close = c
    if len(trs) < 1: return 0.0
    k = min(n, len(trs))
    return sum(trs[-k:]) / float(k)

def _sma(vals: List[float], n: int) -> float:
    if len(vals) < 1: return 0.0
    n = min(n, len(vals))
    return sum(vals[-n:]) / float(n)

def make_features(symbol: str) -> Dict[str, float]:
    rows = _fetch_kline(symbol, "5", 150)
    if not rows: return {}
    closes = [float(it[4]) for it in rows]
    highs  = [float(it[2]) for it in rows]
    lows   = [float(it[3]) for it in rows]

    px = closes[-1]
    atr14 = _atr(rows, 14)
    sma20 = _sma(closes, 20)
    sma50 = _sma(closes, 50)
    mom20 = px - closes[-20] if len(closes) >= 21 else 0.0
    rng   = (max(highs[-20:]) - min(lows[-20:])) if len(highs) >= 20 else 0.0
    vol_z = 0.0
    try:
        win = [abs(closes[i]-closes[i-1]) for i in range(len(closes))][1:]
        m = statistics.mean(win[-50:]); s = statistics.pstdev(win[-50:]) or 1.0
        vol_z = (abs(closes[-1]-closes[-2]) - m) / s
    except Exception:
        pass

    return {
        "px": px, "atr14": atr14, "sma20": sma20, "sma50": sma50,
        "mom20": mom20, "rng20": rng, "vol_z": vol_z,
        "ts": int(time.time()*1000)
    }
