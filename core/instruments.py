#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/instruments.py — Bybit v5 instrument meta with resilient parsing + TTL cache

Public API (backward-compatible + extended):
  - load_or_fetch(symbols: List[str]) -> Dict[str, dict]
  - fetch(symbols: List[str])          -> Dict[str, dict]
  - round_price(p: float, tick: float) -> float
  - round_qty(q: float, step: float, min_qty: float) -> float
  - get_filters(symbol: str)           -> dict  {'tickSize','lotStep','minQty','maxLeverage'}
  - get_tick_step_min(symbol: str)     -> (tick, step, min_qty)
  - warm(symbols: List[str])           -> Dict[str, bool]  prefetch into cache
  - invalidate(symbols: List[str]|None)-> None             drop from mem+disk so next call refetches

What’s improved:
  • In-memory LRU with thread lock on top of disk cache for speed.
  • Fallback to Bybit public /v5/market/instruments-info if relay/proxy fails.
  • Helpers to warm/refresh/invalidate symbols.
  • Single-symbol accessor for convenience across bots.
  • TTL-aware and resilient to weird payload shapes.
  • Atomic cache writes.

Env (optional):
  INSTRUMENTS_TTL_SEC=3600
  INSTRUMENTS_HARD_REFRESH=0
  INSTRUMENTS_CATEGORY=linear
  INSTRUMENTS_CACHE_PATH=<absolute-or-relative-json-path>
  BYBIT_BASE_URL=https://api.bybit.com
  HTTP_TIMEOUT_S=15
"""

from __future__ import annotations
import os, json, time, math, pathlib, threading
from typing import Dict, List, Any, Union, Optional, Tuple

# Local relay client
from .relay_client import proxy

# Optional settings for fallback HTTP
try:
    from .config import settings
    _BYBIT_BASE_URL = str(getattr(settings, "BYBIT_BASE_URL", "https://api.bybit.com")).rstrip("/")
    _HTTP_TIMEOUT_S = int(getattr(settings, "HTTP_TIMEOUT_S", 15))
except Exception:
    _BYBIT_BASE_URL = "https://api.bybit.com"
    _HTTP_TIMEOUT_S = 15

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

# -------- in-memory LRU (tiny) --------
_MEM_CAP = 256
_mem_lock = threading.RLock()
_mem: Dict[str, Dict[str, Any]] = {}        # symbol -> {'data': {...}, 'ts': epoch_ms}
_mem_order: List[str] = []                   # naive LRU order

def _mem_get(sym: str) -> Optional[Dict[str, Any]]:
    with _mem_lock:
        ent = _mem.get(sym)
        if not ent:
            return None
        # move to end (most recent)
        try:
            _mem_order.remove(sym)
        except ValueError:
            pass
        _mem_order.append(sym)
        return ent

def _mem_put(sym: str, data: Dict[str, Any]) -> None:
    with _mem_lock:
        _mem[sym] = {"data": data, "ts": int(time.time() * 1000)}
        try:
            _mem_order.remove(sym)
        except ValueError:
            pass
        _mem_order.append(sym)
        while len(_mem_order) > _MEM_CAP:
            old = _mem_order.pop(0)
            _mem.pop(old, None)

def _mem_invalidate(symbols: Optional[List[str]] = None) -> None:
    with _mem_lock:
        if not symbols:
            _mem.clear()
            _mem_order.clear()
            return
        for s in symbols:
            k = s.upper().strip()
            _mem.pop(k, None)
            try:
                _mem_order.remove(k)
            except ValueError:
                pass

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
      {result:{list:[...]}}, {list:[...]}, {data:[...]}, or {primary:{body:{result:{list:[...]}}}}.
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

# -------- disk cache helpers --------
def _read_cache() -> dict:
    try:
        j = json.loads(CACHE.read_text(encoding="utf-8"))
        if isinstance(j, dict):
            return j
    except Exception:
        pass
    return {"ts": 0, "instruments": {}}

def _write_cache(data: dict) -> None:
    # atomic write to avoid torn JSON
    try:
        tmp = CACHE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(CACHE)
    except Exception:
        # cache failure is non-fatal
        pass

def _fresh_enough(cache_ts_ms: int) -> bool:
    if HARD_REFRESH:
        return False
    age = (int(time.time()*1000) - int(cache_ts_ms)) / 1000.0
    return age <= max(60, TTL_SEC)  # never use TTL < 60s

# -------- network with retries --------
def _relay_instruments_info(symbol: str) -> dict:
    """
    Call relay for a single symbol with tiny backoff retries.
    Returns normalized dict (possibly empty).
    """
    delays = [0.0, 0.2, 0.5]
    for d in delays:
        if d > 0:
            time.sleep(d)
        # FIX: relay_client.proxy uses 'qs=' not 'params='
        raw = proxy("GET", "/v5/market/instruments-info",
                    qs={"category": CATEGORY, "symbol": symbol})
        payload = _coerce_json(raw)
        lst = _extract_list(payload)
        if lst:
            it = lst[0] if isinstance(lst[0], dict) else {}
            meta = _extract_item(it)
            if meta:
                return meta
    return {}

def _public_instruments_info(symbol: str) -> dict:
    """
    Fallback to Bybit public if relay fails.
    """
    import urllib.request
    url = f"{_BYBIT_BASE_URL}/v5/market/instruments-info?category={CATEGORY}&symbol={symbol}"
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_S) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(data, dict) or data.get("retCode") != 0:
        return {}
    lst = (data.get("result") or {}).get("list") or []
    if not lst:
        return {}
    return _extract_item(lst[0])

def _fetch_one(symbol: str) -> dict:
    meta = _relay_instruments_info(symbol)
    if meta:
        return meta
    # relay sulked; try public
    return _public_instruments_info(symbol)

# -------- Public API --------
def fetch(symbols: List[str]) -> Dict[str, dict]:
    """
    Fetch fresh instrument meta for each symbol from relay, with public fallback.
    Writes cache and returns {SYMBOL: {tickSize, lotStep, minQty, maxLeverage}}.
    Also updates in-memory LRU.
    """
    res: Dict[str, dict] = {}
    uniq = [s.upper().strip() for s in symbols if s and s.strip()]
    for sym in uniq:
        meta = _fetch_one(sym)
        if meta:
            res[sym] = meta
            _mem_put(sym, meta)

    # merge with existing cache
    cache = _read_cache()
    have = cache.get("instruments", {}) or {}
    have.update(res)
    out = {"ts": int(time.time()*1000), "instruments": have}
    _write_cache(out)
    return res

def load_or_fetch(symbols: List[str]) -> Dict[str, dict]:
    """
    Return meta for requested symbols, using in-mem/disk cache when fresh.
    Falls back to on-demand fetch for any missing/stale symbols.
    """
    requested = [s for s in symbols if s and str(s).strip()]
    if not requested:
        return {}

    cache = _read_cache()
    cache_ts = int(cache.get("ts", 0))
    have: Dict[str, dict] = cache.get("instruments", {}) or {}

    out: Dict[str, dict] = {}

    # try in-memory first
    for s in requested:
        k = s.upper().strip()
        ent = _mem_get(k)
        if ent:
            out[s] = ent["data"]

    need = []
    # if disk cache fresh, use it; otherwise mark as needed
    is_fresh = _fresh_enough(cache_ts)
    for s in requested:
        k = s.upper().strip()
        if s in out:
            continue
        if is_fresh and k in have:
            out[s] = have[k]
            _mem_put(k, have[k])
        else:
            need.append(k)

    if need:
        fetched = fetch(need)
        for k, v in fetched.items():
            # keep the original case of the request key when possible
            for rq in requested:
                if rq.upper().strip() == k:
                    out[rq] = v
                    break

    return out

def get_filters(symbol: str) -> dict:
    """
    Convenience accessor for a single symbol.
    Returns dict {'tickSize','lotStep','minQty','maxLeverage'} or {}.
    """
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return {}
    # in-mem
    ent = _mem_get(symbol)
    if ent and _fresh_enough(ent.get("ts", 0)):
        return ent["data"]
    # disk or fetch
    res = load_or_fetch([symbol])
    return res.get(symbol, {})

def get_tick_step_min(symbol: str) -> Tuple[float, float, float]:
    """
    Returns (tick, step, min_qty) with sane fallbacks.
    """
    f = get_filters(symbol) or {}
    tick = float(f.get("tickSize", 0.01) or 0.01)
    step = float(f.get("lotStep", 0.001) or 0.001)
    minq = float(f.get("minQty", 0.001) or 0.001)
    if tick <= 0: tick = 0.01
    if step <= 0: step = 0.001
    if minq < 0:  minq = 0.0
    return tick, step, minq

def warm(symbols: List[str]) -> Dict[str, bool]:
    """
    Prefetch a set of symbols into cache/LRU; returns map symbol->ok.
    """
    out: Dict[str, bool] = {}
    if not symbols:
        return out
    fetched = fetch(symbols)
    syms = [s.upper().strip() for s in symbols]
    for s in syms:
        out[s] = (s in fetched)
    return out

def invalidate(symbols: Optional[List[str]] = None) -> None:
    """
    Invalidate memory (and optionally disk) cache for given symbols.
    If symbols is None, clears the in-mem cache and bumps disk timestamp
    so next call will refresh.
    """
    if symbols:
        # clear in-mem for those; remove from disk blob
        syms = [s.upper().strip() for s in symbols]
        _mem_invalidate(syms)
        cache = _read_cache()
        have = cache.get("instruments", {}) or {}
        for s in syms:
            have.pop(s, None)
        cache["instruments"] = have
        # maintain ts so others still considered fresh for their own TTL window
        _write_cache(cache)
    else:
        _mem_invalidate(None)
        cache = _read_cache()
        cache["ts"] = 0
        _write_cache(cache)

# -------- rounding helpers (unchanged semantics) --------
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

if __name__ == "__main__":
    # quick manual smoke
    syms = ["BTCUSDT", "ETHUSDT"]
    print("warm:", warm(syms))
    for s in syms:
        print(s, get_filters(s))
        print("tick/step/min:", get_tick_step_min(s))
