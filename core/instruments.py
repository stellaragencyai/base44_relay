#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time, math, pathlib
from typing import Dict, List, Any, Union
from .relay_client import proxy

ROOT = pathlib.Path(__file__).resolve().parents[1]
CACHE = ROOT / "registry" / "instruments.json"
CACHE.parent.mkdir(parents=True, exist_ok=True)

# -------- JSON helpers --------
def _coerce_json(x: Union[str, dict, None]) -> dict:
    """Accept dict or JSON string; return a dict or {}."""
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}

def _extract_list(payload: dict) -> list:
    """
    Instruments payloads show up in a few shapes depending on relay:
      {result:{list:[...]}}
      {list:[...]}
      {"primary":{"body":{result:{list:[...]}}}}
    We normalize all of them.
    """
    # unwrap relay "primary.body" if present
    body = payload
    if "primary" in payload:
        body = payload.get("primary", {}) or {}
        body = body.get("body", {}) if isinstance(body, dict) else {}

    # standard bybit shape
    if isinstance(body, dict):
        result = body.get("result") or {}
        lst = result.get("list") or result.get("data")
        if isinstance(lst, list):
            return lst
        # alternative flattening
        if "list" in body and isinstance(body["list"], list):
            return body["list"]
        if "data" in body and isinstance(body["data"], list):
            return body["data"]

    return []

def _extract_item(it: dict) -> dict:
    """Compact schema with safe defaults."""
    price_filter = it.get("priceFilter", {}) if isinstance(it, dict) else {}
    lot_filter   = it.get("lotSizeFilter", {}) if isinstance(it, dict) else {}
    lev_filter   = it.get("leverageFilter", {}) if isinstance(it, dict) else {}
    try:
        max_lev = float(str(lev_filter.get("maxLeverage", "0")))
    except Exception:
        max_lev = 0.0
    return {
        "tickSize": float(str(price_filter.get("tickSize", 0.01))),
        "lotStep":  float(str(lot_filter.get("qtyStep",   0.001))),
        "minQty":   float(str(lot_filter.get("minOrderQty", 0.001))),
        "maxLeverage": max_lev,
    }

# -------- Public API --------
def fetch(symbols: List[str]) -> Dict[str, dict]:
    res: Dict[str, dict] = {}
    for s in symbols:
        if not s:
            continue
        sym = s.upper().strip()

        # Relay call (may return dict or JSON string depending on implementation)
        raw = proxy("GET", "/v5/market/instruments-info",
                    params={"category": "linear", "symbol": sym})

        payload = _coerce_json(raw)
        lst = _extract_list(payload)
        if not lst or not isinstance(lst, list):
            # try one more time assuming the relay already returned Bybit body directly
            lst = _extract_list(_coerce_json(payload.get("body") if isinstance(payload, dict) else {}))

        if not lst:
            continue

        it = lst[0] if isinstance(lst[0], dict) else {}
        res[sym] = _extract_item(it)

    data = {"ts": int(time.time() * 1000), "instruments": res}
    try:
        CACHE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        # cache failure is non-fatal
        pass
    return res

def load_or_fetch(symbols: List[str]) -> Dict[str, dict]:
    # normalize inputs
    req = [s.upper().strip() for s in symbols if s and s.strip()]
    have: Dict[str, dict] = {}
    try:
        j = json.loads(CACHE.read_text(encoding="utf-8"))
        if isinstance(j, dict):
            have = j.get("instruments", {}) or {}
    except Exception:
        have = {}

    missing = [s for s in req if s not in have]
    if missing:
        fetched = fetch(missing)
        if fetched:
            have.update(fetched)
            try:
                CACHE.write_text(json.dumps({"ts": int(time.time()*1000), "instruments": have}, indent=2),
                                 encoding="utf-8")
            except Exception:
                pass

    # return only requested symbols (preserve original keys’ case-insensitivity)
    out = {}
    for s in symbols:
        key = s.upper().strip() if s else s
        if key in have:
            out[s] = have[key]
    return out

def round_price(p: float, tick: float) -> float:
    """Floor price to legal tick; if tick is invalid, return p."""
    try:
        tick = float(tick)
        p = float(p)
        if tick <= 0:
            return p
        # small epsilon to avoid floating residuals
        steps = math.floor(p / tick + 1e-12)
        return steps * tick
    except Exception:
        return float(p)

def round_qty(q: float, step: float, min_qty: float) -> float:
    """
    Floor qty to legal step; return 0 if result is below min_qty.
    We keep existing semantics (no auto-bump to min_qty) to avoid surprise fills.
    """
    try:
        q = float(q); step = float(step); min_qty = float(min_qty)
        if step <= 0:
            out = q
        else:
            out = math.floor(q / step + 1e-12) * step
        if out < max(min_qty, 0.0):
            return 0.0
        return out
    except Exception:
        return 0.0
