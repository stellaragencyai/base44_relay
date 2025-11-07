#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Breakout-Impulse strategy (simple v1)
- Uses 5m candles
- Looks for range compression (low ATR%) then breakout of recent high/low with volume pop
"""

from __future__ import annotations
from typing import Dict, Any, List
from strategies.base import Strategy, Signal

def atr(high: List[float], low: List[float], close: List[float], n: int=14) -> List[float]:
    trs = []
    for i in range(len(close)):
        if i == 0:
            trs.append(high[i]-low[i])
        else:
            trs.append(max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1])))
    out = []
    for i in range(len(trs)):
        if i < n:
            out.append(sum(trs[:i+1]) / max(1, i+1))
        else:
            out.append((out[-1]*(n-1) + trs[i]) / n)
    return out

class BreakoutImpulse(Strategy):
    name = "breakout_impulse"

    def intervals(self) -> List[str]:
        return ["5"]

    def required_history(self) -> int:
        return 200

    def generate(self, klines: Dict[str, List[Dict[str, Any]]], last_price: float) -> List[Signal]:
        out: List[Signal] = []
        m5 = klines.get("5", [])
        if len(m5) < 120:
            return out
        h = [float(k["h"]) for k in m5]
        l = [float(k["l"]) for k in m5]
        c = [float(k["c"]) for k in m5]
        v = [float(k["v"]) for k in m5]

        _atr = atr(h,l,c,14)
        px = c[-1]
        # compression via ATR% of price
        atr_pct = (_atr[-1] / max(px, 1e-9)) * 100.0
        vol_z = 0.0
        try:
            import statistics as stats
            vol_z = (v[-1] - stats.mean(v[-30:])) / (stats.pstdev(v[-30:]) + 1e-9)
        except Exception:
            pass

        rng_hi = max(h[-30:-1])
        rng_lo = min(l[-30:-1])

        ts_ms = int(m5[-1]["ts"])
        sym = str(m5[-1]["symbol"])

        # Long breakout
        if atr_pct <= float(self.cfg.get("atr_pct_max", 0.6)) and vol_z >= float(self.cfg.get("min_vol_z", 0.5)) and px > rng_hi:
            out.append(Signal(
                symbol=sym, direction="LONG", ts_ms=ts_ms, confidence=0.55,
                stop_hint=rng_hi,  # stop just under the breakout area; TP/SL manager will refine
                features={"atr_pct": atr_pct, "vol_z": vol_z, "breakout":"up"},
                params={"spread_max_bps": float(self.cfg.get("spread_max_bps", 10.0))},
                source=self.name
            ))

        # Short breakdown
        if atr_pct <= float(self.cfg.get("atr_pct_max", 0.6)) and vol_z >= float(self.cfg.get("min_vol_z", 0.5)) and px < rng_lo:
            out.append(Signal(
                symbol=sym, direction="SHORT", ts_ms=ts_ms, confidence=0.55,
                stop_hint=rng_lo,
                features={"atr_pct": atr_pct, "vol_z": vol_z, "breakout":"down"},
                params={"spread_max_bps": float(self.cfg.get("spread_max_bps", 10.0))},
                source=self.name
            ))
        return out
