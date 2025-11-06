#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, datetime as dt, requests
from typing import Tuple, Dict

def _hms_to_minutes(hhmm: str) -> int:
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m

def _now_local() -> dt.datetime:
    return dt.datetime.now()

def _parse_windows(raw: str):
    wins = []
    for chunk in filter(None, [x.strip() for x in (raw or "").split(",")]):
        start, end = chunk.split("-")
        wins.append((_hms_to_minutes(start), _hms_to_minutes(end)))
    return wins

def _in_windows(now: dt.datetime, wins) -> bool:
    minutes = now.hour * 60 + now.minute
    return any(s <= minutes <= e for s, e in wins)

def _atr_pct_from_ohlc(ohlc: Dict) -> float:
    last = float(ohlc.get("lastPrice", 0) or 0)
    high = float(ohlc.get("highPrice24h", 0) or 0)
    low  = float(ohlc.get("lowPrice24h", 0) or 0)
    if last <= 0 or high <= 0 or low <= 0: 
        return 0.0
    rng = max(1e-12, high - low)
    return (rng / last) * 100.0

def _adx_stub(_: Dict) -> float:
    return 22.0  # replace with real ADX(14) once candle feed is ready

def _fetch_ticker(symbol: str) -> Dict:
    url = f'{os.getenv("RELAY_URL").rstrip("/")}/bybit/tickers'
    headers = {"Authorization": f'Bearer {os.getenv("RELAY_TOKEN")}'}
    r = requests.get(url, headers=headers, timeout=6)
    r.raise_for_status()
    data = r.json()
    items = (data.get("result") or {}).get("list") or []
    for it in items:
        if it.get("symbol") == symbol:
            return it
    return {}

class Gate:
    def __init__(self):
        self.min_adx = float(os.getenv("REGIME_MIN_ADX", "18"))
        self.min_atr = float(os.getenv("REGIME_MIN_ATR_PCT", "0.6"))
        self.max_atr = float(os.getenv("REGIME_MAX_ATR_PCT", "5.0"))
        self.wins = _parse_windows(os.getenv("TRADING_WINDOWS", ""))
        self.whitelist = [x.strip().upper() for x in os.getenv("SYMBOL_WHITELIST", "").split(",") if x.strip()]

    def ok(self, symbol: str) -> Tuple[bool, str, Dict]:
        now = _now_local()
        symu = symbol.upper()

        if self.whitelist and symu not in self.whitelist:
            return False, f"symbol {symu} not in whitelist", {}

        if self.wins and not _in_windows(now, self.wins):
            return False, "outside trading windows", {}

        t = _fetch_ticker(symu)
        if not t:
            return False, "no ticker data", {}

        atr_pct = _atr_pct_from_ohlc(t)
        adx = _adx_stub(t)

        if adx < self.min_adx:
            return False, f"ADX {adx:.1f} < {self.min_adx}", {"atr_pct": atr_pct, "adx": adx}
        if not (self.min_atr <= atr_pct <= self.max_atr):
            return False, f"ATR% {atr_pct:.2f} outside [{self.min_atr},{self.max_atr}]", {"atr_pct": atr_pct, "adx": adx}

        return True, "ok", {"atr_pct": atr_pct, "adx": adx}

gate = Gate()
