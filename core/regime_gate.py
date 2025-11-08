#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Adaptive regime gate:
- Tracks rolling realized volatility, volume z-score, and trend proxy
- Persists regime_state.json for other bots
"""
from __future__ import annotations
import os, json, time, math
from pathlib import Path
from collections import deque
from typing import Dict, Deque

STATE = Path(os.getenv("STATE_DIR","./.state")).resolve()
OUT   = STATE / "regime_state.json"
WINDOW = int(os.getenv("REGIME_WINDOW", "120"))  # ticks

class Regime:
    def __init__(self):
        self.mid: Deque[float] = deque(maxlen=WINDOW)
        self.vol: Deque[float] = deque(maxlen=WINDOW)
        self.volumes: Deque[float] = deque(maxlen=WINDOW)

    def update(self, mid: float, vol: float) -> Dict:
        if mid > 0:
            self.mid.append(mid)
        self.volumes.append(max(0.0, vol))

        if len(self.mid) >= 3:
            # realized vol (log-returns)
            r = []
            for i in range(1, len(self.mid)):
                a, b = self.mid[i-1], self.mid[i]
                r.append(math.log(max(b,1e-9)/max(a,1e-9)))
            rv = (sum(x*x for x in r) / max(1,len(r)))**0.5 * 10000  # bps

            # simple trend proxy: slope over last N
            n = min(40, len(self.mid))
            xs = list(range(n))
            ys = list(self.mid)[-n:]
            xbar = sum(xs)/n; ybar = sum(ys)/n
            num = sum((x-xbar)*(y-ybar) for x,y in zip(xs,ys))
            den = sum((x-xbar)**2 for x in xs) or 1.0
            slope = (num/den) / max(1e-6, ybar)

            # volume z
            vols = list(self.volumes)
            mu = sum(vols)/max(1,len(vols))
            sig = (sum((v-mu)**2 for v in vols)/max(1,len(vols)))**0.5
            vz = 0.0 if sig == 0 else (vols[-1]-mu)/sig

            st = {
                "realized_vol_bps": rv,
                "trend_slope": slope,
                "vol_z": vz,
                "ts": int(time.time()*1000),
            }
        else:
            st = {"realized_vol_bps": 0.0, "trend_slope": 0.0, "vol_z": 0.0, "ts": int(time.time()*1000)}

        OUT.parent.mkdir(parents=True, exist_ok=True)
        try:
            old = json.loads(OUT.read_text(encoding="utf-8"))
        except Exception:
            old = {}
        old.update(st)
        OUT.write_text(json.dumps(old, separators=(",",":"), ensure_ascii=False), encoding="utf-8")
        return st
