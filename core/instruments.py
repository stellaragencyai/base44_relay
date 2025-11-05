#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time, math, pathlib
from .relay_client import proxy

ROOT = pathlib.Path(__file__).resolve().parents[1]
CACHE = ROOT / "registry" / "instruments.json"
CACHE.parent.mkdir(parents=True, exist_ok=True)

def fetch(symbols):
    res = {}
    for s in symbols:
        j = proxy("GET", "/v5/market/instruments-info", params={"category":"linear","symbol":s})
        body = (j.get("primary",{}) or {}).get("body",{})
        lst = ((body.get("result",{}) or {}).get("list",[]) or [])
        if not lst:
            continue
        it = lst[0]
        res[s] = {
            "tickSize": float(it.get("priceFilter",{}).get("tickSize", 0.01)),
            "lotStep":  float(it.get("lotSizeFilter",{}).get("qtyStep",   0.001)),
            "minQty":   float(it.get("lotSizeFilter",{}).get("minOrderQty",0.001)),
        }
    data = {"ts": int(time.time()*1000), "instruments": res}
    CACHE.write_text(json.dumps(data, indent=2))
    return res

def load_or_fetch(symbols):
    try:
        j = json.loads(CACHE.read_text())
        have = j.get("instruments",{})
    except Exception:
        have = {}
    missing = [s for s in symbols if s not in have]
    if missing:
        have.update(fetch(missing))
    return have

def round_price(p, tick):
    return math.floor(p / tick + 1e-9) * tick

def round_qty(q, step, min_qty):
    q = math.floor(q / step + 1e-9) * step
    if q < min_qty:
        return 0.0
    return q
