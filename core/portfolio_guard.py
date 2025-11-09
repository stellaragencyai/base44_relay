#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/portfolio_guard.py — Portfolio Guard (DB-backed with JSON fallback)

What this enforces
- Daily loss cap (DAILY_LOSS_CAP_PCT)
- Per-trade risk budget based on equity (RISK_PCT, interpreted as percent: 0.20 = 0.20%)
- Max concurrent trades across all symbols (MAX_CONCURRENT)
- Per-symbol concurrent cap (MAX_SYMBOL_TRADES)
- Global breaker halt (reads .state/risk_state.json)
- Per-symbol cooldown between new opens (PG_PER_SYMBOL_COOLDOWN_SEC)

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
  PG_PER_SYMBOL_COOLDOWN_SEC=120  # new open on same symbol only after this
  MAX_CONCURRENT_INIT_RISK_PCT=6.0# optional pool cap for sum of initial risks
  GUARD_STATE_FILE=./registry/guard_state.json

  RELAY_URL=http://127.0.0.1:5000
  RELAY_TOKEN=...

Public API (stable)
  guard.allow_new_trade(symbol) -> bool
  guard.current_risk_value()    -> float          (USD budget for next ticket)
  guard.register_open(trade_id, symbol, initial_risk_usd=None) -> None
  guard.register_close(trade_id, realized_pnl_usd=0.0, released_risk_usd=None) -> None
  guard.set_session_equity(equity_usd) -> None
  guard.reset_session(start_equity_usd=None) -> None
  guard.set_halt(on: bool, reason: str = "") -> None
  guard.heartbeat() -> dict

Notes
- If DB is present, daily loss checks rely on core.db.guard_load()
- If DB is not present, we still track session start equity and halted flag in JSON
- Includes a small CLI for ops:
    python -m core.portfolio_guard --status
    python -m core.portfolio_guard --halt on|off --reason "ops"
    python -m core.portfolio_guard --reset-day [--equity 1234.56]
"""

from __future__ import annotations
import os, json, time, threading, pathlib
from typing import Dict, Any, Optional

# -------- optional settings rooting (for breaker path) --------
try:
    from core.config import settings
    _ROOT = settings.ROOT
except Exception:
    _ROOT = pathlib.Path(__file__).resolve().parents[1]

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

def _ensure_dir_for_file(p: str):
    d = os.path.dirname(os.path.abspath(p))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# -------- breaker flag --------
BREAKER_FILE = _ROOT / ".state" / "risk_state.json"
def _breaker_active() -> bool:
    try:
        if not BREAKER_FILE.exists():
            return False
        js = json.loads(BREAKER_FILE.read_text(encoding="utf-8"))
        return bool(js.get("breach") or js.get("active") or js.get("breaker"))
    except Exception:
        return False

class PortfolioGuard:
    def __init__(self):
        # config
        self.asset = os.getenv("BASE_ASSET", "USDT")
        # Percent values: 1.0 means 1.0%, 0.20 means 0.20%
        self.dd_cap_pct = float(os.getenv("DAILY_LOSS_CAP_PCT", "1.0"))
        self.risk_pct   = float(os.getenv("RISK_PCT", "0.20"))
        self.max_conc   = int(os.getenv("MAX_CONCURRENT", "3"))
        self.max_symbol = int(os.getenv("MAX_SYMBOL_TRADES", "1"))
        self.cooldown_s = int(os.getenv("PG_PER_SYMBOL_COOLDOWN_SEC", "120") or "120")
        self.pool_cap_pct = float(os.getenv("MAX_CONCURRENT_INIT_RISK_PCT", "6.0") or "0")
        self.state_file = os.getenv("GUARD_STATE_FILE", "./registry/guard_state.json")
        _ensure_dir_for_file(self.state_file)

        # runtime state
        self._equity_usd: float = 0.0
        self._risk_live_usd: float = 0.0
        self._risk_live_by_symbol: Dict[str, float] = {}
        self._open_by_id: Dict[str, Dict[str, Any]] = {}   # {trade_id: {"symbol":..., "ts":...}}
        self._open_counts: Dict[str, int] = {}             # {symbol: count}
        self._last_open_ts_per_sym: Dict[str, int] = {}    # per-symbol cooldown
        self._manual_halt: bool = False
        self._manual_halt_reason: str = ""

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
            self._dirty = True

        # background saver (very light-touch)
        self._saver = threading.Thread(target=self._autosave_loop, daemon=True)
        self._saver.start()

    # ---------- persistence (JSON fallback) ----------
    _dirty = False

    def _load_json(self) -> Dict[str, Any]:
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            obj = {}
        # normalize
        obj.setdefault("session_start_equity", None)
        obj.setdefault("halted", False)
        obj.setdefault("halt_reason", "")
        obj.setdefault("open_trades", {})
        obj.setdefault("realized_pnl_usd", 0.0)
        obj.setdefault("last_open_ts_per_sym", {})
        return obj

    def _save_json(self) -> None:
        try:
            tmp = {
                "session_start_equity": self._json_state.get("session_start_equity"),
                "halted": bool(self._json_state.get("halted", False)),
                "halt_reason": str(self._json_state.get("halt_reason", "")),
                "open_trades": dict(self._json_state.get("open_trades", {})),
                "realized_pnl_usd": float(self._json_state.get("realized_pnl_usd", 0.0)),
                "last_open_ts_per_sym": dict(self._json_state.get("last_open_ts_per_sym", {})),
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
            ok, data, _err = self._by._request_private_json(
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
            tgt = self.asset.upper()
            for c in coins:
                if str(c.get("coin", "")).upper() == tgt:
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
                if isinstance(data, list):
                    tgt = self.asset.upper()
                    for a in data:
                        if str(a.get("coin", "")).upper() == tgt:
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
            tgt = self.asset.upper()
            for c in coins:
                if str(c.get("coin", "")).upper() == tgt:
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
        """
        True if the daily loss cap is breached.
        DB path: compare realized PnL to cap from start_equity_usd.
        JSON path: compare live equity vs session_start_equity.
        """
        if self.dd_cap_pct <= 0:
            # disabled
            if not _DB_AVAILABLE:
                self._json_state["halted"] = False
                self._dirty = True
            return False

        if _DB_AVAILABLE:
            s = guard_load()
            start = float(s.get("start_equity_usd", 0.0))
            realized = float(s.get("realized_pnl_usd", 0.0))
            cap = (start if start > 0 else self._equity_or_default()) * (self.dd_cap_pct / 100.0)
            return (-realized) >= cap
        else:
            start = float(self._json_state.get("session_start_equity") or 0.0)
            eq = self._equity_or_default()
            if start <= 0:
                self._json_state["halted"] = False
                self._dirty = True
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
            "max_conc_risk_usd":  eq * (self.pool_cap_pct / 100.0) if self.pool_cap_pct > 0 else 0.0,
        }

    # ---------- public api ----------
    def heartbeat(self) -> Dict[str, Any]:
        eq = self._fetch_equity()
        out: Dict[str, Any] = {"equity": eq}

        if _DB_AVAILABLE:
            s = guard_load()
            out.update({
                "start_equity": float(s.get("start_equity_usd", 0.0)),
                "realized_pnl_usd": float(s.get("realized_pnl_usd", 0.0)),
                "dd_cap_pct": self.dd_cap_pct,
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
                "dd_cap_pct": self.dd_cap_pct,
            })

        out.update({
            "halted": self._manual_halt or self._daily_breached() or _breaker_active(),
            "manual_halt": self._manual_halt,
            "manual_halt_reason": self._manual_halt_reason,
            "breaker": _breaker_active(),
            "open_counts": dict(self._open_counts),
            "open_total": sum(int(v) for v in self._open_counts.values()),
            "risk_live_usd": float(self._risk_live_usd),
            "risk_pool_cap_usd": self._caps().get("max_conc_risk_usd", 0.0),
            "cooldown_sec": self.cooldown_s,
            "last_open_ts_per_sym": dict(self._last_open_ts_per_sym),
        })
        return out

    def allow_new_trade(self, symbol: str) -> bool:
        """
        True if a new trade is allowed right now under:
          - breaker OFF and no manual halt
          - daily DD cap not breached (if configured)
          - open_concurrency < MAX_CONCURRENT
          - per-symbol cooldown satisfied
          - per-symbol concurrent cap respected
          - concurrent initial-risk pool not exceeded (if configured)
        """
        sym = (symbol or "").upper()

        if _breaker_active() or self._manual_halt:
            return False

        if self._daily_breached():
            return False

        # concurrency (global)
        total_open = sum(int(v) for v in self._open_counts.values())
        if self.max_conc > 0 and total_open >= self.max_conc:
            return False

        # per-symbol cap
        if self.max_symbol > 0 and self._open_counts.get(sym, 0) >= self.max_symbol:
            return False

        # cooldown
        if self.cooldown_s > 0:
            last = int(self._last_open_ts_per_sym.get(sym, 0))
            if last and int(time.time()) - last < self.cooldown_s:
                return False

        # aggregate initial-risk pool (soft gate)
        caps = self._caps()
        pool = float(caps.get("max_conc_risk_usd", 0.0))
        if pool > 0 and self._risk_live_usd >= pool:
            return False

        return True

    def current_risk_value(self) -> float:
        """
        USD risk allocation for the next trade.
        If you use stop-based sizing, qty = risk_usd / stop_distance.
        """
        eq = self._equity_or_default()
        per_trade = eq * (self.risk_pct / 100.0)
        caps = self._caps()
        pool = float(caps.get("max_conc_risk_usd", 0.0))
        remaining_pool = max(0.0, pool - self._risk_live_usd) if pool > 0 else float("inf")
        return float(max(0.0, min(per_trade, remaining_pool)))

    def register_open(self, trade_id: str, symbol: str, initial_risk_usd: float | None = None) -> None:
        sym = (symbol or "").upper()
        if trade_id:
            self._open_by_id[trade_id] = {"symbol": sym, "ts": int(time.time())}
        self._open_counts[sym] = self._open_counts.get(sym, 0) + 1
        self._last_open_ts_per_sym[sym] = int(time.time())
        if initial_risk_usd is not None:
            val = float(initial_risk_usd)
            self._risk_live_usd += val
            self._risk_live_by_symbol[sym] = self._risk_live_by_symbol.get(sym, 0.0) + val
        # persist light state for restarts
        self._json_state["open_trades"] = dict(self._open_by_id)
        self._json_state["last_open_ts_per_sym"] = dict(self._last_open_ts_per_sym)
        self._dirty = True

    def register_close(self, trade_id: str, realized_pnl_usd: float = 0.0, released_risk_usd: float | None = None) -> None:
        meta = self._open_by_id.pop(trade_id, None)
        if meta:
            sym = (meta.get("symbol") or "UNKNOWN").upper()
            self._open_counts[sym] = max(0, self._open_counts.get(sym, 0) - 1)
            if released_risk_usd is not None:
                val = float(released_risk_usd)
                self._risk_live_usd = max(0.0, self._risk_live_usd - val)
                self._risk_live_by_symbol[sym] = max(0.0, self._risk_live_by_symbol.get(sym, 0.0) - val)
        if realized_pnl_usd != 0.0 and _DB_AVAILABLE:
            try:
                guard_update_pnl(float(realized_pnl_usd))
            except Exception:
                pass
        # persist
        self._json_state["open_trades"] = dict(self._open_by_id)
        if not _DB_AVAILABLE:
            self._json_state["realized_pnl_usd"] = float(self._json_state.get("realized_pnl_usd", 0.0)) + float(realized_pnl_usd)
        self._dirty = True

    # aliases expected by some callers
    def note_open(self, symbol: str, initial_risk_usd: float) -> None:
        tid = f"auto-{int(time.time()*1000)}"
        self.register_open(tid, symbol, initial_risk_usd=initial_risk_usd)

    def note_close(self, symbol: str, released_risk_usd: float, realized_pnl_usd: float) -> None:
        sym = (symbol or "").upper()
        for tid, meta in list(self._open_by_id.items()):
            if (meta.get("symbol") or "").upper() == sym:
                self.register_close(tid, realized_pnl_usd=realized_pnl_usd, released_risk_usd=released_risk_usd)
                break

    def set_session_equity(self, equity_usd: float) -> None:
        self._equity_usd = float(equity_usd)
        if not _DB_AVAILABLE:
            # Bootstrap start equity only if absent
            if self._json_state.get("session_start_equity") in (None, 0):
                self._json_state["session_start_equity"] = float(equity_usd)
                self._dirty = True

    def reset_session(self, start_equity_usd: Optional[float] = None) -> None:
        if _DB_AVAILABLE:
            guard_reset_day(float(start_equity_usd or self._equity_or_default()))
        else:
            self._json_state["session_start_equity"] = float(start_equity_usd or self._equity_or_default())
            self._json_state["realized_pnl_usd"] = 0.0
            self._json_state["halted"] = False
            self._json_state["halt_reason"] = ""
            self._dirty = True
        # clear live pools/counters (we don't close trades for you)
        self._risk_live_usd = 0.0
        self._risk_live_by_symbol.clear()
        self._last_open_ts_per_sym.clear()

    def set_halt(self, on: bool, reason: str = "") -> None:
        self._manual_halt = bool(on)
        self._manual_halt_reason = str(reason or "")
        # also persist to JSON for visibility if DB is absent
        self._json_state["halted"] = self._manual_halt or self._json_state.get("halted", False)
        self._json_state["halt_reason"] = self._manual_halt_reason
        self._dirty = True


# singleton
guard = PortfolioGuard()

# ---------- CLI ----------
def _cli():
    import argparse, json as _json
    ap = argparse.ArgumentParser(description="Portfolio Guard")
    ap.add_argument("--status", action="store_true", help="Print status JSON")
    ap.add_argument("--halt", type=str, choices=["on", "off"], help="Set manual halt")
    ap.add_argument("--reason", type=str, default="", help="Reason for manual halt")
    ap.add_argument("--reset-day", action="store_true", help="Reset session start and counters")
    ap.add_argument("--equity", type=float, default=None, help="Optional equity for reset-day")
    args = ap.parse_args()

    if args.status:
        print(_json.dumps(guard.heartbeat(), indent=2))
        return

    if args.halt is not None:
        guard.set_halt(args.halt == "on", reason=args.reason)
        print(_json.dumps(guard.heartbeat(), indent=2))
        return

    if args.reset_day:
        guard.reset_session(start_equity_usd=args.equity)
        print(_json.dumps(guard.heartbeat(), indent=2))
        return

    ap.print_help()

if __name__ == "__main__":
    _cli()
