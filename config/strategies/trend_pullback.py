#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trend-Pullback strategy (simple v1)
- Uses 5m candles
- HTF bias via 1h EMA(50) slope
- Entry when price pulls back to EMA(20) in direction of bias
"""

from __future__ import annotations
from typing import Dict, Any, List
from dataclasses import dataclass
from strategies.base import Strategy, Signal

def ema(series: List[float], n: int) -> List[float]:
    if not series or n <= 1:
        return series[:]
    k = 2 / (n + 1)
    out = [series[0]]
    for x in series[1:]:
        out.append(out[-1] + k * (x - out[-1]))
    return out

class TrendPullback(Strategy):
    name = "trend_pullback"

    def intervals(self) -> List[str]:
        return ["5", "60"]

    def required_history(self) -> int:
        return 200

    def generate(self, klines: Dict[str, List[Dict[str, Any]]], last_price: float) -> List[Signal]:
        out: List[Signal] = []
        m5 = klines.get("5", [])
        h1 = klines.get("60", [])
        if len(m5) < 120 or len(h1) < 60:
            return out

        h1_close = [float(k["c"]) for k in h1]
        h1_ema50 = ema(h1_close, 50)
        bias_up = h1_ema50[-1] > h1_ema50[-2]
        bias_down = h1_ema50[-1] < h1_ema50[-2]

        m5_close = [float(k["c"]) for k in m5]
        m5_low   = [float(k["l"]) for k in m5]
        m5_high  = [float(k["h"]) for k in m5]
        ema20    = ema(m5_close, 20)

        ts_ms = int(m5[-1]["ts"])
        # Long when bias up and last candle pulled back to EMA20 but didn't break previous swing low
        if bias_up:
            touched = m5_low[-1] <= ema20[-1] <= m5_high[-1]
            if touched and last_price > ema20[-1]:
                stop = min(m5_low[-10:])  # crude structure-ish
                conf = 0.6
                out.append(Signal(
                    symbol=str(m5[-1]["symbol"]),
                    direction="LONG",
                    ts_ms=ts_ms,
                    confidence=conf,
                    stop_hint=stop,
                    features={
                        "h1_ema50_slope":"up",
                        "pullback_to_ema20": True
                    },
                    params={"spread_max_bps": float(self.cfg.get("spread_max_bps", 8.0))},
                    source=self.name
                ))

        # Short when bias down and last candle pulled up to EMA20 but closed below
        if bias_down:
            touched = m5_low[-1] <= ema20[-1] <= m5_high[-1]
            if touched and last_price < ema20[-1]:
                stop = max(m5_high[-10:])
                conf = 0.6
                out.append(Signal(
                    symbol=str(m5[-1]["symbol"]),
                    direction="SHORT",
                    ts_ms=ts_ms,
                    confidence=conf,
                    stop_hint=stop,
                    features={
                        "h1_ema50_slope":"down",
                        "pullup_to_ema20": True
                    },
                    params={"spread_max_bps": float(self.cfg.get("spread_max_bps", 8.0))},
                    source=self.name
                ))
        return out
