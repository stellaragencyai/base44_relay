#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/portfolio_guard.py — Risk Daemon Lite
- Tracks session equity and enforces:
  • per-trade risk cap (RISK_PCT)
  • daily loss cap (DAILY_LOSS_CAP_PCT)
  • max concurrent trades (MAX_CONCURRENT)
  • per-symbol concentration (MAX_SYMBOL_TRADES)
- Stateless persistence via a tiny json file so restarts keep limits.

Env (.env):
  RELAY_URL
  RELAY_TOKEN
  BASE_ASSET=USDT
  DAILY_LOSS_CAP_PCT=1.0        # halt for today if equity drawdown ≥ this %
  RISK_PCT=0.20                 # risk per trade as % of equity (0.05–0.35 typical)
  MAX_CONCURRENT=3
  MAX_SYMBOL_TRADES=1
  GUARD_STATE_FILE=./registry/guard_state.json
"""

import os, json, time, threading, requests
from typing import Dict, Any

def _relay_headers():
    tok = os.getenv("RELAY_TOKEN","")
    return {"Authorization": f"Bearer {tok}"} if tok else {}

def _relay_url(path: str) -> str:
    base = os.getenv("RELAY_URL","http://127.0.0.1:8080").rstrip("/")
    return base + (path if path.startswith("/") else "/" + path)

def _ensure_dir(p: str):
    d = os.path.dirname(os.path.abspath(p))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

class PortfolioGuard:
    def __init__(self):
        self.asset = os.getenv("BASE_ASSET","USDT")
        self.dd_cap_pct = float(os.getenv("DAILY_LOSS_CAP_PCT","1.0"))
        self.risk_pct = float(os.getenv("RISK_PCT","0.20"))
        self.max_conc = int(os.getenv("MAX_CONCURRENT","3"))
        self.max_symbol = int(os.getenv("MAX_SYMBOL_TRADES","1"))
        self.state_file = os.getenv("GUARD_STATE_FILE","./registry/guard_state.json")
        _ensure_dir(self.state_file)
        self.state = self._load_state()

    # ---------- persistence ----------
    def _load_state(self) -> Dict[str,Any]:
        try:
            with open(self.state_file,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"session_start_equity": None, "halted": False, "open_trades": {}}

    def _save_state(self):
        tmp = self.state.copy()
        with open(self.state_file,"w",encoding="utf-8") as f:
            json.dump(tmp, f, indent=2)

    # ---------- equity ----------
    def _fetch_equity(self) -> float:
        # Relay native helper preferred; fallback to /v5/account/wallet-balance via proxy
        try:
            url = _relay_url("/bybit/wallet/balance")
            r = requests.get(url, headers=_relay_headers(), timeout=6)
            if r.status_code == 200:
                data = r.json()
                # Expect array of accounts with currency and equity
                for a in data:
                    if str(a.get("coin","")).upper() == self.asset.upper():
                        return float(a.get("equity",0) or 0.0)
        except Exception:
            pass
        # Fallback:
        url = _relay_url("/bybit/proxy")
        payload = {"target":"/v5/account/wallet-balance","method":"GET","params":{"accountType":"UNIFIED"}}
        r = requests.post(url, headers=_relay_headers(), json=payload, timeout=8)
        r.raise_for_status()
        js = r.json()
        listv = ((js.get("result") or {}).get("list") or [])
        if not listv: return 0.0
        # Sum USDT-equivalent
        try:
            coins = listv[0].get("coin",[])
            eq = 0.0
            for c in coins:
                if c.get("coin") == self.asset:
                    eq += float(c.get("equity",0) or 0.0)
            return eq
        except Exception:
            return 0.0

    # ---------- API ----------
    def heartbeat(self) -> Dict[str,Any]:
        eq = self._fetch_equity()
        if self.state["session_start_equity"] is None:
            self.state["session_start_equity"] = eq
            self._save_state()
        start = max(1e-9, float(self.state["session_start_equity"]))
        dd_pct = max(0.0, (start - eq) / start * 100.0)
        if dd_pct >= self.dd_cap_pct:
            self.state["halted"] = True
            self._save_state()
        return {"equity": eq, "start_equity": start, "dd_pct": dd_pct, "halted": self.state["halted"],
                "open_trades": self.state.get("open_trades",{})}

    def allow_new_trade(self, symbol: str) -> bool:
        hb = self.heartbeat()
        if hb["halted"]:
            return False
        ot = self.state.get("open_trades",{})
        if sum(1 for _ in ot.values()) >= self.max_conc:
            return False
        count_symbol = sum(1 for s in ot.values() if s.get("symbol")==symbol.upper())
        if count_symbol >= self.max_symbol:
            return False
        return True

    def register_open(self, trade_id: str, symbol: str):
        ot = self.state.setdefault("open_trades",{})
        ot[trade_id] = {"symbol": symbol.upper(), "ts": int(time.time())}
        self._save_state()

    def register_close(self, trade_id: str):
        ot = self.state.setdefault("open_trades",{})
        ot.pop(trade_id, None)
        self._save_state()

    def current_risk_value(self) -> float:
        eq = self._fetch_equity()
        return max(0.0, eq * self.risk_pct / 100.0)  # risk_pct is percent (e.g., 0.20%)

guard = PortfolioGuard()
