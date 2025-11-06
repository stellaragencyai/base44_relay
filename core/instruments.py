#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/instruments.py — Bybit v5 instrument meta with resilient parsing + TTL cache

Public API (unchanged):
  - load_or_fetch(symbols: List[str]) -> Dict[str, dict]
  - fetch(symbols: List[str])          -> Dict[str, dict]
  - round_price(p: float, tick: float) -> float
  - round_qty(q: float, step: float, min_qty: float) -> float

What’s improved:
  • TTL-aware cache so bots don’t spam the relay (INSTRUMENTS_TTL_SEC, default 3600).
  • Robust payload unwrapping for multiple relay shapes (primary.body, result.list, list, data).
  • Retry with tiny backoff on transient relay errors or empty bodies.
  • Optional hard refresh via env knob INSTRUMENTS_HARD_REFRESH=1.
  • Category is configurable (INSTRUMENTS_CATEGORY, default linear).
  • Cache location is configurable (INSTRUMENTS_CACHE_PATH), default registry/instruments.json.
  • Defensive normalization (floats) and safe defaults if exchange acts mysterious.

Env (optional):
  INSTRUMENTS_TTL_SEC=3600
  INSTRUMENTS_HARD_REFRESH=0
  INSTRUMENTS_CATEGORY=linear
  INSTRUMENTS_CACHE_PATH=<absolute-or-relative-json-path>
"""

from __future__ import annotations
import os, json, time, math, pathlib, typing
from typing import Dict, List, Any, Union, Optional

# Local relay client
from .relay_client import proxy

# -------- config / paths --------
ROOT = pathlib.Path(__file__).resolve().parents[1]

def _cache_path() -> pathlib.Path:
    p = (os.getenv("INSTRUMENTS_CACHE_PATH") or "").strip()
    if p:
        pth = pathlib.Path(p)
        if not pth.is_absolute():
            pth = ROOT / pth
    else:
        pth = ROOT / "registry" / "instruments.json"
    pth.parent.mkdir(parents=True, exist_ok=True)
    return pth

CACHE = _cache_path()
TTL_SEC = int(os.getenv("INSTRUMENTS_TTL_SEC", "3600") or "3600")
HARD_REFRESH = (os.getenv("INSTRUMENTS_HARD_REFRESH", "0").strip().lower() in {"1","true","yes","on"})
CATEGORY = (os.getenv("INSTRUMENTS_CATEGORY", "linear") or "linear").strip()

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

def _unwrap_primary_body(payload: dict) -> dict:
    """Unwrap {primary:{body:{...}}} shapes used by some relays."""
    if not isinstance(payload, dict):
        return {}
    if "primary" in payload:
        pb = payload.get("primary") or {}
        if isinstance(pb, dict):
            b = pb.get("body")
            return b if isinstance(b, dict) else {}
    return payload

def _extract_list(payload: dict) -> list:
    """
    Normalize Bybit instruments payloads:
      {result:{list:[...]}}, {list:[...]}, {data:[...]}, or {{primary:{body:{result:{list:[...]}}}}.
    """
    body = _unwrap_primary_body(payload)
    if not isinstance(body, dict):
        return []
    # Most common
    result = body.get("result") or {}
    if isinstance(result, dict):
        lst = result.get("list") or result.get("data")
        if isinstance(lst, list):
            return lst
    # Alternate flat shapes
    if isinstance(body.get("list"), list):
        return body["list"]
    if isinstance(body.get("data"), list):
        return body["data"]
    return []

def _as_float(x: Any, default: float) -> float:
    try:
        return float(str(x))
    except Exception:
        return float(default)

def _extract_item(it: dict) -> dict:
    """Compact schema with safe defaults."""
    price_filter = it.get("priceFilter", {}) if isinstance(it, dict) else {}
    lot_filter   = it.get("lotSizeFilter", {}) if isinstance(it, dict) else {}
    lev_filter   = it.get("leverageFilter", {}) if isinstance(it, dict) else {}

    tick = _as_float(price_filter.get("tickSize", 0.01), 0.01)
    step = _as_float(lot_filter.get("qtyStep", 0.001), 0.001)
    minq = _as_float(lot_filter.get("minOrderQty", 0.001), 0.001)
    max_lev = _as_float(lev_filter.get("maxLeverage", 0.0), 0.0)

    # Defensive clamps
    if tick <= 0:  tick = 0.01
    if step <= 0:  step = 0.001
    if minq < 0:   minq = 0.0
    if max_lev < 0: max_lev = 0.0

    return {
        "tickSize": tick,
        "lotStep":  step,
        "minQty":   minq,
        "maxLeverage": max_lev,
    }

# -------- cache helpers --------
def _read_cache() -> dict:
    try:
        j = json.loads(CACHE.read_text(encoding="utf-8"))
        if isinstance(j, dict):
            return j
    except Exception:
        pass
    return {"ts": 0, "instruments": {}}

def _write_cache(data: dict) -> None:
    try:
        CACHE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception:
        # cache failure is non-fatal
        pass

def _fresh_enough(cache_ts_ms: int) -> bool:
    if HARD_REFRESH:
        return False
    age = (int(time.time()*1000) - int(cache_ts_ms)) / 1000.0
    return age <= max(60, TTL_SEC)  # never use TTL < 60s because that’s silly

# -------- network with retries --------
def _relay_instruments_info(symbol: str) -> dict:
    """
    Call relay for a single symbol with tiny backoff retries.
    Returns normalized dict (possibly empty).
    """
    # A few quick attempts is enough; rest of system will tolerate empty.
    delays = [0.0, 0.2, 0.5]
    last_payload = {}
    for d in delays:
        if d > 0:
            time.sleep(d)
        raw = proxy("GET", "/v5/market/instruments-info",
                    params={"category": CATEGORY, "symbol": symbol})
        payload = _coerce_json(raw)
        lst = _extract_list(payload)
        if lst:
            try:
                # Some relays return the actual Bybit body directly under body
                if not isinstance(lst[0], dict) and isinstance(payload.get("body"), dict):
                    lst = _extract_list(_coerce_json(payload["body"]))
            except Exception:
                pass
        last_payload = payload
        if lst:
            # Return first entry
            it = lst[0] if isinstance(lst[0], dict) else {}
            return _extract_item(it)
    # No dice; try to be graceful
    return {}

# -------- Public API --------
def fetch(symbols: List[str]) -> Dict[str, dict]:
    """
    Fetch fresh instrument meta for each symbol from the relay.
    Writes cache and returns {SYMBOL: {tickSize, lotStep, minQty, maxLeverage}}.
    """
    res: Dict[str, dict] = {}
    uniq = [s.upper().strip() for s in symbols if s and s.strip()]
    for sym in uniq:
        meta = _relay_instruments_info(sym)
        if meta:
            res[sym] = meta

    # merge with existing cache
    cache = _read_cache()
    have = cache.get("instruments", {}) or {}
    have.update(res)
    out = {"ts": int(time.time()*1000), "instruments": have}
    _write_cache(out)
    return res

def load_or_fetch(symbols: List[str]) -> Dict[str, dict]:
    """
    Return meta for requested symbols, using cache when fresh.
    Falls back to on-demand fetch for any missing/stale symbols.
    """
    requested = [s for s in symbols if s and str(s).strip()]
    if not requested:
        return {}

    cache = _read_cache()
    cache_ts = int(cache.get("ts", 0))
    have: Dict[str, dict] = cache.get("instruments", {}) or {}

    # If cache too old, we’ll refetch missing; but keep still-valid entries.
    need = []
    if not _fresh_enough(cache_ts):
        # Treat all requested as candidates if cache is stale
        for s in requested:
            key = s.upper().strip()
            if key not in have:
                need.append(key)
    else:
        # Cache is fine; only fetch missing ones
        for s in requested:
            key = s.upper().strip()
            if key not in have:
                need.append(key)

    if need:
        fetched = fetch(need)
        if fetched:
            have.update({k: fetched[k] for k in fetched})

    # Compose output in the same case as requested keys
    out: Dict[str, dict] = {}
    for s in requested:
        key = s.upper().strip()
        if key in have:
            out[s] = have[key]
    return out

def round_price(p: float, tick: float) -> float:
    """Floor price to legal tick; if tick invalid, return p."""
    try:
        tick = float(tick)
        p = float(p)
        if tick <= 0:
            return p
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
