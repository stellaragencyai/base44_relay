#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/portfolio_guard.py — Portfolio Guard (merged)
- Keeps your API: allow_new_trade(), current_risk_value(), register_open/close(), heartbeat()
- Uses core.guard as the single source of truth for breaker state
- Daily DD cap (percent), optional gross exposure cap and per-symbol concentration cap
- Per-trade risk budget, global and per-symbol concurrency caps, per-symbol cooldown
- Equity via Bybit private; falls back to relay if needed
- Light JSON persistence for restarts; DB hooks optional

CLI:
  python -m core.portfolio_guard --status
  python -m core.portfolio_guard --halt on|off --reason "ops"
  python -m core.portfolio_guard --reset-day [--equity 1234.56]

Env (read via os.getenv; you can also mirror in core.config.settings if you insist):
  BASE_ASSET=USDT
  DAILY_LOSS_CAP_PCT=1.0           # halt if daily drawdown ≥ 1.0%
  RISK_PCT=0.20                    # per-trade risk as percent of equity (0.20 = 0.20%)
  MAX_CONCURRENT=3
  MAX_SYMBOL_TRADES=1
  PG_PER_SYMBOL_COOLDOWN_SEC=120
  MAX_CONCURRENT_INIT_RISK_PCT=6.0 # pool cap for sum of initial risks
  # exposure caps (optional; 0 disables)
  PG_GROSS_EXPO_MAX_PCT=60         # gross notional / equity ≤ 60%
  PG_CONC_MAX_PCT=35               # any single symbol notional / equity ≤ 35%

  # Relay fallback
  RELAY_URL=http://127.0.0.1:5000
  RELAY_TOKEN=...

  # JSON state file
  GUARD_STATE_FILE=./registry/guard_state.json
"""

from __future__ import annotations
import os, json, time, threading, pathlib
from decimal import Decimal, getcontext
from typing import Dict, Any, Optional, Tuple, List

getcontext().prec = 28

# -------- roots --------
try:
    from core.config import settings
    _ROOT = settings.ROOT
except Exception:
    _ROOT = pathlib.Path(__file__).resolve().parents[1]

# -------- centralized breaker --------
from core.guard import (
    guard_blocking_reason,
    guard_set,
    guard_clear,
)

# -------- DB hooks (optional) --------
_DB_AVAILABLE = True
try:
    from core.db import guard_load, guard_update_pnl, guard_reset_day
except Exception:
    _DB_AVAILABLE = False
    def guard_load() -> Dict[str, Any]:  # type: ignore
        return {"session_start_ms": int(time.time()*1000), "start_equity_usd": 0.0, "realized_pnl_usd": 0.0}
    def guard_update_pnl(_delta: float) -> None:  # type: ignore
        pass
    def guard_reset_day(_start_equity: float = 0.0) -> None:  # type: ignore
        pass

# -------- Bybit client (preferred equity/positions) --------
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

# ============================
# Portfolio Guard
# ============================

class PortfolioGuard:
    def __init__(self):
        # config
        self.asset = os.getenv("BASE_ASSET", "USDT")

        # percents: 1.0 means 1.0%; 0.20 means 0.20%
        self.dd_cap_pct = float(os.getenv("DAILY_LOSS_CAP_PCT", "1.0"))
        self.risk_pct   = float(os.getenv("RISK_PCT", "0.20"))

        self.max_conc   = int(os.getenv("MAX_CONCURRENT", "3"))
        self.max_symbol = int(os.getenv("MAX_SYMBOL_TRADES", "1"))
        self.cooldown_s = int(os.getenv("PG_PER_SYMBOL_COOLDOWN_SEC", "120") or "120")
        self.pool_cap_pct = float(os.getenv("MAX_CONCURRENT_INIT_RISK_PCT", "6.0") or "0")

        # exposure caps (0 disables)
        self.gross_cap_pct = float(os.getenv("PG_GROSS_EXPO_MAX_PCT", "60") or "0")
        self.conc_cap_pct  = float(os.getenv("PG_CONC_MAX_PCT", "35") or "0")

        # persistence
        self.state_file = os.getenv("GUARD_STATE_FILE", "./registry/guard_state.json")
        _ensure_dir_for_file(self.state_file)

        # runtime state
        self._equity_usd: float = 0.0
        self._risk_live_usd: float = 0.0
        self._risk_live_by_symbol: Dict[str, float] = {}
        self._open_by_id: Dict[str, Dict[str, Any]] = {}   # {trade_id: {"symbol":..., "ts":...}}
        self._open_counts: Dict[str, int] = {}             # {symbol: count}
        self._last_open_ts_per_sym: Dict[str, int] = {}    # per-symbol cooldown
        self._manual_halt_reason: str = ""

        # bybit client
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

        # file-backed state for restarts (fallback if DB not used)
        self._json_state = self._load_json()
        if not _DB_AVAILABLE and self._json_state.get("session_start_equity") is None:
            eq = self._fetch_equity()
            self._json_state["session_start_equity"] = eq
            self._dirty = True

        # background saver (tiny)
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
        obj.setdefault("session_start_equity", None)
        obj.setdefault("open_trades", {})
        obj.setdefault("realized_pnl_usd", 0.0)
        obj.setdefault("last_open_ts_per_sym", {})
        return obj

    def _save_json(self) -> None:
        try:
            tmp = {
                "session_start_equity": self._json_state.get("session_start_equity"),
                "open_trades": dict(self._open_by_id),
                "realized_pnl_usd": float(self._json_state.get("realized_pnl_usd", 0.0)),
                "last_open_ts_per_sym": dict(self._last_open_ts_per_sym),
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
        # Try native helper
        try:
            url = _relay_url("/bybit/wallet/balance")
            r = requests.get(url, headers=_relay_headers(), timeout=6)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and "normalized" in data:
                    # if hitting compat shape
                    accs = ((data.get("normalized") or {}).get("accounts") or [])
                    if accs:
                        coins = (accs[0].get("coins") or [])
                        tgt = self.asset.upper()
                        eq = sum(float(c.get("equity") or 0.0) for c in coins if str(c.get("coin","")).upper()==tgt)
                        return float(eq)
        except Exception:
            pass
        # Fallback to proxy
        try:
            url = _relay_url("/bybit/proxy")
            payload = {"method": "GET", "path": "/v5/account/wallet-balance", "params": {"accountType": "UNIFIED"}}
            r = requests.post(url, headers=_relay_headers(), json=payload, timeout=8)
            if r.status_code != 200:
                return 0.0
            js = r.json()
            primary = (js.get("primary") or {})
            body = (primary.get("body") or {})
            lst = (body.get("result", {}) or {}).get("list", []) or []
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
        eq = self._fetch_equity_bybit_direct()
        if eq <= 0.0:
            eq = self._fetch_equity_relay()
        self._equity_usd = float(eq)
        return self._equity_usd

    # ---------- positions / exposure ----------
    def _positions(self) -> List[dict]:
        if not self._by:
            return []
        ok, data, _err = self._by.get_positions(category="linear")
        if not ok:
            return []
        return (data.get("result") or {}).get("list") or []

    @staticmethod
    def _pos_notional_usdt(p: dict) -> Decimal:
        try:
            sz = Decimal(str(p.get("size") or "0"))
            px = Decimal(str(p.get("avgPrice") or p.get("markPrice") or "0"))
            return abs(sz * px)
        except Exception:
            return Decimal("0")

    def _exposure(self) -> Tuple[float, Dict[str, float]]:
        gross = Decimal("0")
        per_sym: Dict[str, Decimal] = {}
        for p in self._positions():
            sym = str(p.get("symbol") or "").upper()
            if not sym:
                continue
            n = self._pos_notional_usdt(p)
            if n <= 0:
                continue
            gross += n
            per_sym[sym] = per_sym.get(sym, Decimal("0")) + n
        return float(gross), {k: float(v) for k, v in per_sym.items()}

    # ---------- helpers ----------
    def _equity_or_default(self) -> float:
        return self._equity_usd if self._equity_usd > 0 else self._fetch_equity() or 100.0

    def _daily_breached(self) -> bool:
        """
        True if the daily loss cap is breached.
        DB path: compare realized PnL to cap from start_equity_usd.
        JSON path: compare live equity vs session_start_equity.
        Also sets the global breaker when breached.
        """
        if self.dd_cap_pct <= 0:
            return False

        if _DB_AVAILABLE:
            s = guard_load()
            start = float(s.get("start_equity_usd", 0.0))
            realized = float(s.get("realized_pnl_usd", 0.0))
            base = start if start > 0 else self._equity_or_default()
            cap = base * (self.dd_cap_pct / 100.0)
            breached = (-realized) >= cap
        else:
            start = float(self._json_state.get("session_start_equity") or 0.0)
            eq = self._equity_or_default()
            if start <= 0:
                return False
            dd_pct = max(0.0, (start - eq) / start * 100.0)
            breached = dd_pct >= self.dd_cap_pct

        if breached:
            guard_set(f"[PG] Daily loss cap hit ≥ {self.dd_cap_pct:.2f}%")
        return breached

    def _caps(self) -> Dict[str, float]:
        eq = self._equity_or_default()
        return {
            "daily_loss_cap_usd": eq * (self.dd_cap_pct / 100.0),
            "max_conc_risk_usd":  eq * (self.pool_cap_pct / 100.0) if self.pool_cap_pct > 0 else 0.0,
        }

    # ---------- public api ----------
    def heartbeat(self) -> Dict[str, Any]:
        eq = self._fetch_equity()
        gross, per_sym = self._exposure()
        out: Dict[str, Any] = {"equity": eq, "gross": gross, "per_sym": per_sym}

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

        blocked, why = guard_blocking_reason()

        out.update({
            "breaker": {"on": bool(blocked), "reason": why},
            "open_counts": dict(self._open_counts),
            "open_total": sum(int(v) for v in self._open_counts.values()),
            "risk_live_usd": float(self._risk_live_usd),
            "risk_pool_cap_usd": self._caps().get("max_conc_risk_usd", 0.0),
            "cooldown_sec": self.cooldown_s,
            "last_open_ts_per_sym": dict(self._last_open_ts_per_sym),
            "gross_cap_pct": self.gross_cap_pct,
            "conc_cap_pct": self.conc_cap_pct,
        })
        return out

    def allow_new_trade(self, symbol: str) -> bool:
        """
        True if a new trade is allowed right now under:
          - global breaker OFF (core.guard)
          - daily DD cap not breached
          - global concurrency cap and per-symbol cap
          - per-symbol cooldown
          - pool cap on sum of initial risks
          - exposure caps: gross and per-symbol (if configured)
        """
        sym = (symbol or "").upper()

        blocked, _ = guard_blocking_reason()
        if blocked:
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

        # exposure caps (live)
        if self.gross_cap_pct > 0 or self.conc_cap_pct > 0:
            eq = self._equity_or_default()
            if eq > 0:
                gross, per_sym = self._exposure()
                if self.gross_cap_pct > 0 and (gross / eq) * 100.0 > self.gross_cap_pct:
                    guard_set(f"[PG] Gross exposure {(gross/eq):.1%} > cap {self.gross_cap_pct:.0f}%")
                    return False
                if self.conc_cap_pct > 0:
                    top = max((v for v in per_sym.values()), default=0.0)
                    if top > 0 and (top / eq) * 100.0 > self.conc_cap_pct:
                        guard_set(f"[PG] Concentration {(top/eq):.1%} > cap {self.conc_cap_pct:.0f}%")
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
            if self._json_state.get("session_start_equity") in (None, 0):
                self._json_state["session_start_equity"] = float(equity_usd)
                self._dirty = True

    def reset_session(self, start_equity_usd: Optional[float] = None) -> None:
        if _DB_AVAILABLE:
            guard_reset_day(float(start_equity_usd or self._equity_or_default()))
        else:
            self._json_state["session_start_equity"] = float(start_equity_usd or self._equity_or_default())
            self._json_state["realized_pnl_usd"] = 0.0
            self._dirty = True
        # clear live pools/counters
        self._risk_live_usd = 0.0
        self._risk_live_by_symbol.clear()
        self._last_open_ts_per_sym.clear()

    def set_halt(self, on: bool, reason: str = "") -> None:
        """
        Manual override that flips the centralized breaker.
        """
        self._manual_halt_reason = str(reason or "")
        if on:
            guard_set(self._manual_halt_reason or "[PG] manual halt")
        else:
            guard_clear(note=self._manual_halt_reason or "manual clear")

# singleton
guard = PortfolioGuard()

# ---------- CLI ----------
def _cli():
    import argparse, json as _json
    ap = argparse.ArgumentParser(description="Portfolio Guard")
    ap.add_argument("--status", action="store_true", help="Print status JSON")
    ap.add_argument("--halt", type=str, choices=["on", "off"], help="Set manual halt (flips global breaker)")
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
