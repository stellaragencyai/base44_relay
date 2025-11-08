#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/portfolio_guard.py — Portfolio Guard (DB-backed with JSON fallback)

What this enforces
- Daily loss cap (DAILY_LOSS_CAP_PCT)
- Per-trade risk budget based on equity (RISK_PCT, interpreted as percent: 0.20 = 0.20%)
- Max concurrent trades across all symbols (MAX_CONCURRENT)
- Per-symbol concurrent cap (MAX_SYMBOL_TRADES)

Data sources & persistence
- Prefers core.db guard_state for daily PnL/session tracking; falls back to a tiny JSON file
- Equity fetch prefers direct Bybit (core.bybit_client), then relay (/bybit/wallet/balance or /bybit/proxy)
- In-memory counters for live risk and open slots; persisted lightly to JSON for cold restarts

Env (.env)
  BASE_ASSET=USDT
  DAILY_LOSS_CAP_PCT=1.0          # halt for today if drawdown ≥ this percent
  RISK_PCT=0.20                   # per-trade risk as percent of equity (0.20 = 0.20%)
  MAX_CONCURRENT=3
  MAX_SYMBOL_TRADES=1
  GUARD_STATE_FILE=./registry/guard_state.json

  RELAY_URL=http://127.0.0.1:5000
  RELAY_TOKEN=...

Public API (stable)
  guard.allow_new_trade(symbol) -> bool
  guard.current_risk_value()    -> float          (USD budget for next ticket)
  guard.register_open(trade_id, symbol) -> None   (alias: note_open)
  guard.register_close(trade_id) -> None          (alias: note_close)
  guard.set_session_equity(equity_usd) -> None
  guard.heartbeat() -> dict

Notes
- If DB is present, daily loss checks rely on core.db.guard_load()
- If DB is not present, we still track session start equity and halted flag in JSON
"""

from __future__ import annotations
import os, json, time, threading
from typing import Dict, Any, Optional

# -------- optional DB wiring --------
_DB_AVAILABLE = True
try:
    from core.db import guard_load, guard_update_pnl, guard_reset_day
except Exception:
    _DB_AVAILABLE = False
    def guard_load() -> Dict[str, Any]:  # type: ignore
        return {"session_start_ms": int(time.time()*1000), "start_equity_usd": 0.0, "realized_pnl_usd": 0.0, "breach": False}
    def guard_update_pnl(_delta: float) -> None:  # type: ignore
        pass
    def guard_reset_day(_start_equity: float = 0.0) -> None:  # type: ignore
        pass

# -------- Bybit direct client (preferred equity) --------
_BYBIT_OK = True
try:
    from core.bybit_client import Bybit
except Exception:
    _BYBIT_OK = False
    Bybit = None  # type: ignore

# -------- Relay fallback --------
import requests

def _relay_headers():
    tok = os.getenv("RELAY_TOKEN", "")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def _relay_url(path: str) -> str:
    base = os.getenv("RELAY_URL", "http://127.0.0.1:5000").rstrip("/")
    return base + (path if path.startswith("/") else "/" + path)

def _ensure_dir(p: str):
    d = os.path.dirname(os.path.abspath(p))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

class PortfolioGuard:
    def __init__(self):
        # config
        self.asset = os.getenv("BASE_ASSET", "USDT")
        # Percent values: 1.0 means 1.0%, 0.20 means 0.20%
        self.dd_cap_pct = float(os.getenv("DAILY_LOSS_CAP_PCT", "1.0"))
        self.risk_pct   = float(os.getenv("RISK_PCT", "0.20"))
        self.max_conc   = int(os.getenv("MAX_CONCURRENT", "3"))
        self.max_symbol = int(os.getenv("MAX_SYMBOL_TRADES", "1"))
        self.state_file = os.getenv("GUARD_STATE_FILE", "./registry/guard_state.json")
        _ensure_dir(self.state_file)

        # runtime state
        self._equity_usd: float = 0.0
        self._risk_live_usd: float = 0.0
        self._risk_live_by_symbol: Dict[str, float] = {}
        self._open_by_id: Dict[str, Dict[str, Any]] = {}   # {trade_id: {"symbol":..., "ts":...}}
        self._open_counts: Dict[str, int] = {}             # {symbol: count}

        # bybit client (optional)
        self._by = None
        if _BYBIT_OK:
            try:
                self._by = Bybit()
                try:
                    self._by.sync_time()
                except Exception:
                    pass
            except Exception:
                self._by = None

        # file-backed state for restarts (fallback only; DB remains source of truth for daily PnL)
        self._json_state = self._load_json()
        # bootstrap session start equity if DB not handling it
        if not _DB_AVAILABLE and self._json_state.get("session_start_equity") is None:
            eq = self._fetch_equity()
            self._json_state["session_start_equity"] = eq
            self._save_json()

        # background saver (very light-touch)
        self._dirty = False
        self._saver = threading.Thread(target=self._autosave_loop, daemon=True)
        self._saver.start()

    # ---------- persistence (JSON fallback) ----------
    def _load_json(self) -> Dict[str, Any]:
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            obj = {}
        # normalize
        obj.setdefault("session_start_equity", None)
        obj.setdefault("halted", False)
        obj.setdefault("open_trades", {})
        obj.setdefault("realized_pnl_usd", 0.0)
        return obj

    def _save_json(self) -> None:
        try:
            tmp = {
                "session_start_equity": self._json_state.get("session_start_equity"),
                "halted": bool(self._json_state.get("halted", False)),
                "open_trades": dict(self._json_state.get("open_trades", {})),
                "realized_pnl_usd": float(self._json_state.get("realized_pnl_usd", 0.0)),
            }
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(tmp, f, indent=2)
        except Exception:
            pass

    def _autosave_loop(self) -> None:
        while True:
            time.sleep(2.0)
            if self._dirty:
                self._dirty = False
                self._save_json()

    # ---------- equity ----------
    def _fetch_equity_bybit_direct(self) -> float:
        if not self._by:
            return 0.0
        try:
            ok, data, err = self._by._request_private_json(
                "/v5/account/wallet-balance",
                method="GET",
                params={"accountType": "UNIFIED"},
            )
            if not ok:
                return 0.0
            lst = (data.get("result", {}) or {}).get("list", []) or []
            if not lst:
                return 0.0
            coins = lst[0].get("coin", []) or []
            eq = 0.0
            for c in coins:
                if str(c.get("coin", "")).upper() == self.asset.upper():
                    eq += float(c.get("equity") or 0.0)
            return float(eq)
        except Exception:
            return 0.0

    def _fetch_equity_relay(self) -> float:
        # Try native helper first
        try:
            url = _relay_url("/bybit/wallet/balance")
            r = requests.get(url, headers=_relay_headers(), timeout=6)
            if r.status_code == 200:
                data = r.json()
                # Expect array of accounts with coin+equity
                if isinstance(data, list):
                    for a in data:
                        if str(a.get("coin", "")).upper() == self.asset.upper():
                            return float(a.get("equity", 0) or 0.0)
        except Exception:
            pass
        # Fallback to proxy
        try:
            url = _relay_url("/bybit/proxy")
            payload = {"target": "/v5/account/wallet-balance", "method": "GET", "params": {"accountType": "UNIFIED"}}
            r = requests.post(url, headers=_relay_headers(), json=payload, timeout=8)
            r.raise_for_status()
            js = r.json()
            lst = (js.get("result", {}) or {}).get("list", []) or []
            if not lst:
                return 0.0
            coins = lst[0].get("coin", []) or []
            eq = 0.0
            for c in coins:
                if str(c.get("coin", "")).upper() == self.asset.upper():
                    eq += float(c.get("equity", 0) or 0.0)
            return float(eq)
        except Exception:
            return 0.0

    def _fetch_equity(self) -> float:
        # prefer direct; fall back to relay
        eq = self._fetch_equity_bybit_direct()
        if eq <= 0.0:
            eq = self._fetch_equity_relay()
        self._equity_usd = float(eq)
        return self._equity_usd

    # ---------- helpers ----------
    def _equity_or_default(self) -> float:
        return self._equity_usd if self._equity_usd > 0 else self._fetch_equity() or 100.0

    def _daily_breached(self) -> bool:
        if _DB_AVAILABLE:
            s = guard_load()
            start = float(s.get("start_equity_usd", 0.0))
            realized = float(s.get("realized_pnl_usd", 0.0))
            if start <= 0:
                # If start is unknown, treat realized loss vs cap alone
                cap = self._equity_or_default() * (self.dd_cap_pct / 100.0)
                return (-realized) >= cap
            # Classic drawdown from session start
            cap = start * (self.dd_cap_pct / 100.0)
            return (-realized) >= cap
        else:
            start = float(self._json_state.get("session_start_equity") or 0.0)
            eq = self._equity_or_default()
            if start <= 0:
                return False
            dd_pct = max(0.0, (start - eq) / start * 100.0)
            halted = dd_pct >= self.dd_cap_pct
            self._json_state["halted"] = bool(halted)
            self._dirty = True
            return halted

    def _caps(self) -> Dict[str, float]:
        eq = self._equity_or_default()
        return {
            "daily_loss_cap_usd": eq * (self.dd_cap_pct / 100.0),
            "max_conc_risk_usd":  eq * (float(os.getenv("MAX_CONCURRENT_INIT_RISK_PCT", "6.0")) / 100.0),
        }

    # ---------- API ----------
    def heartbeat(self) -> Dict[str, Any]:
        eq = self._fetch_equity()
        out: Dict[str, Any] = {"equity": eq}

        if _DB_AVAILABLE:
            s = guard_load()
            out.update({
                "start_equity": float(s.get("start_equity_usd", 0.0)),
                "realized_pnl_usd": float(s.get("realized_pnl_usd", 0.0)),
                "halted": bool(self._daily_breached()),
                "open_counts": dict(self._open_counts),
            })
        else:
            start = self._json_state.get("session_start_equity")
            if start is None:
                self._json_state["session_start_equity"] = eq
                self._dirty = True
                start = eq
            dd_pct = max(0.0, (float(start) - eq) / max(1e-9, float(start)) * 100.0)
            out.update({
                "start_equity": float(start),
                "dd_pct": dd_pct,
                "halted": bool(self._json_state.get("halted", False)),
                "open_counts": dict(self._open_counts),
            })
        return out

    def allow_new_trade(self, symbol: str) -> bool:
        if self._daily_breached():
            return False
        # aggregate concurrency by count (legacy rule)
        total_open = sum(int(v) for v in self._open_counts.values())
        if total_open >= self.max_conc:
            return False
        # per-symbol cap
        if self._open_counts.get(symbol.upper(), 0) >= self.max_symbol:
            return False
        # aggregate initial risk cap (soft; only if env MAX_CONCURRENT_INIT_RISK_PCT set)
        caps = self._caps()
        if caps["max_conc_risk_usd"] > 0 and self._risk_live_usd >= caps["max_conc_risk_usd"]:
            return False
        return True

    def current_risk_value(self) -> float:
        """
        USD risk allocation for the next trade.
        If you use stop-based sizing, qty = risk_usd / stop_distance.
        """
        eq = self._equity_or_default()
        per_trade = eq * (self.risk_pct / 100.0)
        # also bounded by remaining concurrent-risk pool if present
        caps = self._caps()
        remaining_pool = max(0.0, caps["max_conc_risk_usd"] - self._risk_live_usd) if caps["max_conc_risk_usd"] > 0 else float("inf")
        return float(max(0.0, min(per_trade, remaining_pool)))

    # Back-compat names kept
    def register_open(self, trade_id: str, symbol: str, initial_risk_usd: float | None = None) -> None:
        sym = symbol.upper()
        self._open_by_id[trade_id] = {"symbol": sym, "ts": int(time.time())}
        self._open_counts[sym] = self._open_counts.get(sym, 0) + 1
        if initial_risk_usd is not None:
            self._risk_live_usd += float(initial_risk_usd)
            self._risk_live_by_symbol[sym] = self._risk_live_by_symbol.get(sym, 0.0) + float(initial_risk_usd)
        # persist light state for restarts
        self._json_state["open_trades"] = dict(self._open_by_id)
        self._dirty = True

    def register_close(self, trade_id: str, realized_pnl_usd: float = 0.0, released_risk_usd: float | None = None) -> None:
        meta = self._open_by_id.pop(trade_id, None)
        if meta:
            sym = meta.get("symbol", "UNKNOWN").upper()
            self._open_counts[sym] = max(0, self._open_counts.get(sym, 0) - 1)
            if released_risk_usd is not None:
                self._risk_live_usd = max(0.0, self._risk_live_usd - float(released_risk_usd))
                self._risk_live_by_symbol[sym] = max(0.0, self._risk_live_by_symbol.get(sym, 0.0) - float(released_risk_usd))
        if realized_pnl_usd != 0.0 and _DB_AVAILABLE:
            try:
                guard_update_pnl(float(realized_pnl_usd))
            except Exception:
                pass
        # persist light state
        self._json_state["open_trades"] = dict(self._open_by_id)
        if not _DB_AVAILABLE:
            self._json_state["realized_pnl_usd"] = float(self._json_state.get("realized_pnl_usd", 0.0)) + float(realized_pnl_usd)
        self._dirty = True

    # aliases expected by some callers
    def note_open(self, symbol: str, initial_risk_usd: float) -> None:
        tid = f"auto-{int(time.time()*1000)}"
        self.register_open(tid, symbol, initial_risk_usd=initial_risk_usd)

    def note_close(self, symbol: str, released_risk_usd: float, realized_pnl_usd: float) -> None:
        # best-effort: find one open trade for this symbol and close it
        for tid, meta in list(self._open_by_id.items()):
            if meta.get("symbol") == symbol.upper():
                self.register_close(tid, realized_pnl_usd=realized_pnl_usd, released_risk_usd=released_risk_usd)
                break

    def set_session_equity(self, equity_usd: float) -> None:
        self._equity_usd = float(equity_usd)
        if not _DB_AVAILABLE:
            self._json_state["session_start_equity"] = float(equity_usd) if self._json_state.get("session_start_equity") in (None, 0) else self._json_state["session_start_equity"]
            self._dirty = True

    # optional helper to reset new session (DB users should call guard_reset_day externally)
    def reset_session(self, start_equity_usd: Optional[float] = None) -> None:
        if _DB_AVAILABLE:
            guard_reset_day(float(start_equity_usd or self._equity_or_default()))
        else:
            self._json_state["session_start_equity"] = float(start_equity_usd or self._equity_or_default())
            self._json_state["realized_pnl_usd"] = 0.0
            self._json_state["halted"] = False
            self._dirty = True

# singleton
guard = PortfolioGuard()
