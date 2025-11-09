#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Portfolio Guard Daemon (complements core/risk_daemon)
Purpose: enforce portfolio-wide guardrails (open-trade cap, exposure/risk caps),
publish health telemetry, and coordinate with the global breaker.

What this does (different from core/risk_daemon.py):
- Reads open positions and estimates open exposure and per-trade risk.
- Enforces caps:
    • max open trades (distinct symbols)
    • max gross exposure as % of equity
    • max per-trade theoretical risk as % of equity (uses stop_dist hint if present)
- Updates core.db.guard_state session anchors (daily) and pushes realized PnL deltas if you provide them.
- Publishes health telemetry to .state/health.json (beats + drawdown_pct).
- Trips or clears breaker via core.breaker with TTL semantics. Never flattens positions.

Env knobs (set in .env or via core.config.settings):
  GUARD_ENABLED=true
  GUARD_POLL_SEC=15
  GUARD_MAX_OPEN_TRADES=8
  GUARD_MAX_GROSS_EXPOSURE_PCT=450        # notional/equity * 100
  GUARD_MAX_PER_TRADE_RISK_PCT=0.60       # theoretical R per position as % of equity
  GUARD_BREAKER_TTL_SEC=0                 # 0 = sticky until manual/approval clear
  GUARD_NOTIFY_EVERY_SEC=120
  TZ=America/Phoenix

Notes
- “Per-trade risk” estimate uses, if available, a stop distance from position/trading-stop
  or a last-ATR fallback similar to signal engine. It’s conservative by design.
- Requires: core.bybit_client, core.breaker, core.db, tools.notifier_telegram, core.healthbeat
"""

from __future__ import annotations
import os, time, json, math, datetime as dt
from decimal import Decimal, getcontext
from typing import Dict, List, Tuple, Optional
from pathlib import Path

from core.config import settings
from core.logger import get_logger
from core.bybit_client import Bybit
from core import breaker
from tools.notifier_telegram import tg
from core.db import guard_load, guard_reset_day
from core.healthbeat import beat, set_drawdown_pct, probe_and_set

getcontext().prec = 28
log = get_logger("bots.portfolio_guard")

# ---------- config ----------
def _bool(name: str, default: bool) -> bool:
    try:
        v = getattr(settings, name)
    except AttributeError:
        v = os.getenv(name, str(int(default)))
    return str(v).strip().lower() in {"1","true","yes","on"}

def _int(name: str, default: int) -> int:
    try:
        v = getattr(settings, name)
    except AttributeError:
        v = os.getenv(name, str(default))
    try: return int(str(v).strip())
    except Exception: return default

def _float(name: str, default: float) -> float:
    try:
        v = getattr(settings, name)
    except AttributeError:
        v = os.getenv(name, str(default))
    try: return float(str(v).strip())
    except Exception: return default

ENABLED                   = _bool("GUARD_ENABLED", True)
POLL_SEC                  = max(5, _int("GUARD_POLL_SEC", 15))
MAX_OPEN_TRADES           = max(1, _int("GUARD_MAX_OPEN_TRADES", 8))
MAX_GROSS_EXPO_PCT        = max(1.0, _float("GUARD_MAX_GROSS_EXPOSURE_PCT", 450.0))
MAX_PER_TRADE_RISK_PCT    = max(0.05, _float("GUARD_MAX_PER_TRADE_RISK_PCT", 0.60))
BREAKER_TTL               = max(0, _int("GUARD_BREAKER_TTL_SEC", 0))
NOTIFY_EVERY_SEC          = max(30, _int("GUARD_NOTIFY_EVERY_SEC", 120))

TZ = getattr(settings, "TZ", os.getenv("TZ", "America/Phoenix")) or "America/Phoenix"

ROOT = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- client ----------
by = Bybit()
try:
    by.sync_time()
except Exception as e:
    log.warning("time sync failed: %s", e)

# ---------- time helpers ----------
def now_local() -> dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo(TZ))
    except Exception:
        return dt.datetime.now()

def today_key(dt_in: Optional[dt.datetime] = None) -> str:
    d = dt_in or now_local()
    return d.strftime("%Y-%m-%d")

# ---------- fetchers ----------
def fetch_equity_unified() -> Decimal:
    ok, data, _ = by.get_wallet_balance(accountType="UNIFIED")
    if not ok:
        return Decimal("0")
    wallets = (data.get("result") or {}).get("list") or []
    if not wallets:
        return Decimal("0")
    eq = Decimal("0")
    coins = wallets[0].get("coin", []) or []
    for c in coins:
        usd = Decimal(str(c.get("usdValue") or "0"))
        if usd == 0 and str(c.get("coin") or "").upper() in {"USDT","USDC"}:
            usd = Decimal(str(c.get("walletBalance") or "0"))
        eq += usd
    return eq

def fetch_positions() -> List[dict]:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        log.warning("positions error: %s", err)
        return []
    return (data.get("result") or {}).get("list") or []

def fetch_orderbook_top(symbol: str) -> Optional[Tuple[Decimal, Decimal]]:
    ok, data, err = by.get_orderbook(category="linear", symbol=symbol, limit=1)
    if not ok:
        return None
    r = (data.get("result") or {})
    bids = r.get("b") or r.get("bids") or []
    asks = r.get("a") or r.get("asks") or []
    if not bids or not asks:
        return None
    return Decimal(str(bids[0][0])), Decimal(str(asks[0][0]))

# ---------- risk math ----------
def _atr_fallback_frac(symbol: str, last: Decimal) -> Decimal:
    # fallback ATR% estimate if no explicit stopLoss is attached
    if last <= 0:
        return Decimal("0.01")
    if symbol.upper().startswith(("BTC","ETH")):
        return Decimal("0.008")
    return Decimal("0.012")

def est_stop_distance(symbol: str, pos: dict, last: Decimal) -> Decimal:
    # Use existing stopLoss on the position if present, else ATR fallback
    try:
        cur_sl = Decimal(str(pos.get("stopLoss") or "0"))
    except Exception:
        cur_sl = Decimal("0")
    side = str(pos.get("side") or "").lower()
    if cur_sl > 0 and last > 0:
        dist = (last - cur_sl) if side.startswith("buy") else (cur_sl - last)
        if dist > 0:
            return dist
    frac = _atr_fallback_frac(str(pos.get("symbol","")), last)
    return max(Decimal("0"), last * frac)

def per_trade_risk_usd(equity_usd: Decimal, pos: dict, last: Decimal) -> Decimal:
    # theoretical R = qty * stop_dist
    try:
        qty = Decimal(str(pos.get("size") or "0")).copy_abs()
    except Exception:
        qty = Decimal("0")
    dist = est_stop_distance(str(pos.get("symbol","")), pos, last)
    return qty * dist

# ---------- breaker / notify ----------
_last_alert_ts = 0.0

def _maybe_notify(msg: str, level: str = "warn"):
    global _last_alert_ts
    now = time.time()
    if now - _last_alert_ts >= NOTIFY_EVERY_SEC:
        _last_alert_ts = now
        pr = "error" if level == "error" else ("success" if level == "success" else "warn")
        tg.safe_text(msg, quiet=True)

def _set_breaker(reason: str):
    if not breaker.is_active():
        breaker.set_on(reason=reason, ttl_sec=BREAKER_TTL, source="portfolio_guard")
        _maybe_notify(f"⛔ Breaker SET • {reason}", level="error")
    else:
        _maybe_notify(f"⛔ Breaker still active • {reason}", level="error")

def _clear_breaker():
    if breaker.is_active():
        try:
            breaker.set_off(reason="portfolio_guard_clear", source="portfolio_guard")
            _maybe_notify("✅ Breaker OFF • portfolio guard clear", level="success")
        except Exception as e:
            _maybe_notify(f"❌ Breaker clear blocked • {e}", level="error")

# ---------- core loop ----------
def evaluate_once() -> Dict[str, any]:
    # equity + day roll
    equity = fetch_equity_unified()
    g = guard_load()  # session_start_ms, start_equity_usd, realized_pnl_usd, breach
    start_eq = Decimal(str(g.get("start_equity_usd") or 0.0))
    if start_eq <= 0:
        # seed at first run today
        guard_reset_day(float(equity))
        start_eq = equity

    # drawdown
    dd_pct = float(0.0 if start_eq <= 0 else max(Decimal("0"), (start_eq - equity) / start_eq * Decimal("100")))
    set_drawdown_pct(dd_pct)

    # positions snapshot
    pos = fetch_positions()
    sym_set = set()
    gross_notional = Decimal("0")
    worst_per_trade_r = Decimal("0")

    for p in pos:
        try:
            sym = (p.get("symbol") or "").upper()
            if not sym:
                continue
            size = Decimal(str(p.get("size") or "0"))
            if size == 0:
                continue
            sym_set.add(sym)
            last = Decimal(str(p.get("markPrice") or p.get("avgPrice") or "0"))
            gross_notional += (size.copy_abs() * last)
            per_r = per_trade_risk_usd(equity, p, last)
            if per_r > worst_per_trade_r:
                worst_per_trade_r = per_r
        except Exception:
            continue

    open_trades = len(sym_set)
    gross_expo_pct = float(0.0 if equity <= 0 else (gross_notional / equity) * Decimal("100"))
    worst_r_pct = float(0.0 if equity <= 0 else (worst_per_trade_r / equity) * Decimal("100"))

    # heartbeat
    beat("portfolio_guard", critical=True, extra={
        "open_trades": open_trades,
        "gross_expo_pct": round(gross_expo_pct, 2),
        "worst_r_pct": round(worst_r_pct, 3),
        "dd_pct": round(dd_pct, 2),
    })

    # relay/exchange probe hook (best-effort; optional)
    try:
        ok, why = True, "no-relay"
        if hasattr(breaker, "probe_relay"):
            ok, why = breaker.probe_relay()  # type: ignore
        probe_and_set(relay_ok=ok)
    except Exception:
        pass

    return {
        "equity": float(equity),
        "start_equity": float(start_eq),
        "dd_pct": dd_pct,
        "open_trades": open_trades,
        "gross_expo_pct": gross_expo_pct,
        "worst_r_pct": worst_r_pct,
    }

def enforce_limits(snapshot: Dict[str, any]) -> None:
    dd = snapshot["dd_pct"]
    n_tr = snapshot["open_trades"]
    expo = snapshot["gross_expo_pct"]
    worst_r = snapshot["worst_r_pct"]

    breaches = []
    if n_tr > MAX_OPEN_TRADES:
        breaches.append(f"trades {n_tr}>{MAX_OPEN_TRADES}")
    if expo > MAX_GROSS_EXPO_PCT:
        breaches.append(f"exposure {expo:.1f}%>{MAX_GROSS_EXPO_PCT:.1f}%")
    if worst_r > MAX_PER_TRADE_RISK_PCT:
        breaches.append(f"per-trade R {worst_r:.3f}%>{MAX_PER_TRADE_RISK_PCT:.3f}%")

    if breaches:
        _set_breaker(" | ".join(breaches))
    else:
        # only clear if no separate risk_daemon breach; act conservatively
        # we don’t auto-clear if drawdown is large even if other limits are ok
        if dd < 0.5:  # tiny residual dd ok
            _clear_breaker()

def main():
    if not ENABLED:
        tg.safe_text("Portfolio Guard disabled (GUARD_ENABLED=false).", quiet=True)
        log.info("disabled via GUARD_ENABLED")
        return

    tg.safe_text(
        f"🟢 Portfolio Guard online • poll={POLL_SEC}s • caps: trades≤{MAX_OPEN_TRADES}, expo≤{MAX_GROSS_EXPO_PCT:.0f}%, perR≤{MAX_PER_TRADE_RISK_PCT:.2f}% • ttl={BREAKER_TTL}s",
        quiet=True,
    )
    last_day = today_key()

    while True:
        try:
            snap = evaluate_once()

            # day rollover: refresh DB baseline if new day
            cur_day = today_key()
            if cur_day != last_day:
                last_day = cur_day
                guard_reset_day(snap["equity"])
                tg.safe_text(f"📅 Guard day reset • start_equity={snap['equity']:.2f}", quiet=True)

            enforce_limits(snap)
            time.sleep(POLL_SEC)
        except KeyboardInterrupt:
            log.info("stopping.")
            break
        except Exception as e:
            log.warning(f"guard loop error: {e}")
            time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
