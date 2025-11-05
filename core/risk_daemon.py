#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

# --- Load env & ensure imports work no matter how you run this ---
try:
    # Preferred when run as a module from repo root:
    #   python -m core.risk_daemon
    from core.env_bootstrap import *  # loads config/.env automatically
except Exception:
    # Fallback if run as a plain script:
    import os, sys
    from pathlib import Path
    here = Path(__file__).resolve()
    repo_root = here.parent.parent  # <repo> that contains /core
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from core.env_bootstrap import *  # loads config/.env automatically

"""
Base44 Risk Daemon (v1.9 — relay-native, breaker-aware, whitelist, SL-ensure)

Key upgrades vs your v1.8:
- Queries Bybit via your RELAY proxy with memberId for BOTH balance and positions
  so we don't get "retCode=None" empties.
- Default RISK_MAX_DD_PCT raised to 10.0 (you asked for 10% daily cap).
- Whitelist loader accepts both {allowed_symbols/blocked_symbols} and
  {symbols_whitelist/symbols_blocklist}.
- Always ensure a last-resort SL (3 ticks) if a position is naked.
- Manual breaker respected via .state/risk_state.json.
- Optional auto-fix (cancel-all + reduce-only flatten) remains gated by env.

Important env (in .env):
  RISK_POLL_SEC=30
  RISK_MAX_DD_PCT=10.0
  RISK_AUTOFIX=false
  RISK_AUTOFIX_DRY_RUN=true
  RISK_NOTIFY_EVERY_MIN=10
  RISK_STARTUP_GRACE_SEC=15
  RISK_EWMA_ALPHA=0.25
  RISK_BREACH_HYSTERESIS_PCT=0.3
  RISK_LOG_CSV=
  RISK_STATE_DIR=.state
  TRADE_GUARD_PATH=config/trade_guard.json
  ENFORCE_WHITELIST=true
  RISK_CLOSE_ON_VIOLATION=false
"""

import os, json, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN

# Prefer notifier_bot’s tg_send; fall back to base44_client; console if all else fails
def _resolve_tg_send():
    try:
        from core import notifier_bot as _nb
        if hasattr(_nb, "tg_send"):
            return _nb.tg_send
    except Exception:
        try:
            import notifier_bot as _nb  # type: ignore
            if hasattr(_nb, "tg_send"):
                return _nb.tg_send
        except Exception:
            pass
    try:
        from core import base44_client as _b44
        if hasattr(_b44, "tg_send"):
            return _b44.tg_send
    except Exception:
        try:
            import base44_client as _b44  # type: ignore
            if hasattr(_b44, "tg_send"):
                return _b44.tg_send
        except Exception:
            pass
    def _console_only(msg: str, **_):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{ts}] {msg}")
    return _console_only

tg_send = _resolve_tg_send()

# Base44 client imports (we still use load_sub_uids/pretty_name/proxy)
try:
    from core.base44_client import (
        load_sub_uids, pretty_name, bybit_proxy
    )
except Exception:
    from base44_client import (  # type: ignore
        load_sub_uids, pretty_name, bybit_proxy
    )

# ─────────── Config from env ───────────
def _getf(name, default):
    try: return float(os.getenv(name, str(default)))
    except Exception: return default

def _geti(name, default):
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

def _getb(name, default):
    v = (os.getenv(name, "") or "").strip().lower()
    if v in ("1","true","yes","y","on"): return True
    if v in ("0","false","no","n","off"): return False
    return default

POLL_SEC               = _geti("RISK_POLL_SEC", 30)
MAX_DD_PCT             = _getf("RISK_MAX_DD_PCT", 10.0)  # default 10 now
AUTOFIX                = _getb("RISK_AUTOFIX", False)
DRY_RUN                = _getb("RISK_AUTOFIX_DRY_RUN", True)
NOTIFY_EVERY_MIN       = _geti("RISK_NOTIFY_EVERY_MIN", 10)
STARTUP_GRACE_SEC      = _geti("RISK_STARTUP_GRACE_SEC", 15)
EWMA_ALPHA             = _getf("RISK_EWMA_ALPHA", 0.25)
HYSTERESIS_PCT         = _getf("RISK_BREACH_HYSTERESIS_PCT", 0.3)
CSV_PATH               = os.getenv("RISK_LOG_CSV", "").strip()
STATE_DIR              = Path(os.getenv("RISK_STATE_DIR", ".state"))
TRADE_GUARD_PATH       = Path(os.getenv("TRADE_GUARD_PATH", "config/trade_guard.json"))
ENFORCE_WHITELIST      = _getb("ENFORCE_WHITELIST", True)
CLOSE_ON_VIOLATION     = _getb("RISK_CLOSE_ON_VIOLATION", False)

STATE_DIR.mkdir(parents=True, exist_ok=True)
ANCHORS_PATH = STATE_DIR / "risk_anchors.json"
BREAKER_PATH = STATE_DIR / "risk_state.json"

CATEGORY = "linear"
SETTLE_COINS = ["USDT", "USDC"]

# ─────────── State ───────────
anchors: dict[str, float] = {}
equity_ewma: dict[str, float] = {}
last_alert: dict[str, datetime] = {}

def _now_utc() -> datetime: return datetime.now(timezone.utc)
def _utc_day_key(dt: datetime) -> str: return dt.date().isoformat()

def _load_anchors() -> tuple[str, dict[str, float]]:
    if not ANCHORS_PATH.exists(): return _utc_day_key(_now_utc()), {}
    try:
        data = json.loads(ANCHORS_PATH.read_text(encoding="utf-8"))
        return data.get("day",""), {k: float(v) for k,v in (data.get("anchors") or {}).items()}
    except Exception:
        return _utc_day_key(_now_utc()), {}

def _save_anchors(day: str, anchors_map: dict[str, float]) -> None:
    try:
        ANCHORS_PATH.write_text(json.dumps({"day": day, "anchors": anchors_map}, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[risk_daemon] warn: persist anchors: {e}")

def _read_breaker() -> dict:
    if not BREAKER_PATH.exists():
        return {}
    try:
        return json.loads(BREAKER_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_breaker(is_breached: bool, details: dict | None = None) -> None:
    payload = {
        "breach": bool(is_breached),
        "updated_at": _now_utc().isoformat(),
        "max_dd_pct": MAX_DD_PCT,
    }
    if details: payload["details"] = details
    try:
        BREAKER_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[risk_daemon] warn: write breaker: {e}")

# ─────────── Relay-backed helpers ───────────
def _wallet_balance_unified(uid: str, coin: str = "USDT") -> dict:
    return bybit_proxy("GET", "/v5/account/wallet-balance",
                       params={"accountType": "UNIFIED", "coin": coin, "memberId": uid})

def _positions_linear(uid: str, settle_coin: str | None = None) -> dict:
    params = {"category": CATEGORY, "memberId": uid}
    if settle_coin: params["settleCoin"] = settle_coin
    return bybit_proxy("GET", "/v5/position/list", params=params)

def _instrument_meta(symbol: str) -> tuple[Decimal, Decimal]:
    r = bybit_proxy("GET", "/v5/market/instruments-info", params={"category": CATEGORY, "symbol": symbol})
    lst = (r.get("result") or {}).get("list") or []
    if not lst: raise RuntimeError(f"instrument meta not found: {symbol}")
    tick = Decimal(lst[0]["priceFilter"]["tickSize"])
    step = Decimal(lst[0]["lotSizeFilter"]["qtyStep"])
    return tick, step

def _set_trading_stop(uid: str, symbol: str, pos_idx: int, stop_loss: str) -> dict:
    body = {"category": CATEGORY, "symbol": symbol, "positionIdx": str(pos_idx), "stopLoss": stop_loss, "memberId": uid}
    return bybit_proxy("POST", "/v5/position/trading-stop", body=body)

def _cancel_all_orders(uid: str) -> tuple[bool, str]:
    r = bybit_proxy("POST", "/v5/order/cancel-all", body={"category": CATEGORY, "memberId": uid})
    ok = r.get("retCode") in (0, "0")
    return ok, f"cancel-all retCode={r.get('retCode')}"

def _place_reduce_market(uid: str, symbol: str, close_side: str, qty: str) -> dict:
    body = {"category": CATEGORY, "symbol": symbol, "side": close_side,
            "orderType": "Market", "qty": qty, "reduceOnly": True, "memberId": uid}
    return bybit_proxy("POST", "/v5/order/create", body=body)

def _round_to_tick(p: Decimal, tick: Decimal) -> Decimal:
    steps = (p / tick).quantize(Decimal("1."), rounding=ROUND_DOWN)
    return steps * tick

# ─────────── Value parsing ───────────
def _total_equity_from_resp(resp: dict) -> float | None:
    try:
        if not resp or resp.get("retCode") not in (0, "0", 0): return None
        lst = (resp.get("result") or {}).get("list") or []
        if not lst: return None
        eq = lst[0].get("totalEquity")
        return float(eq) if eq is not None else None
    except Exception:
        return None

# ---- Last-resort SL ensure (3 ticks away) ----
def _ensure_stop(uid: str, symbol: str, side: str, entry: float | int | str, pos_idx: int) -> None:
    try:
        r = _positions_linear(uid)
        rows = (r.get("result") or {}).get("list") or []
        for row in rows:
            if row.get("symbol") != symbol: continue
            if Decimal(str(row.get("stopLoss") or "0")) > 0: return
        tick, _ = _instrument_meta(symbol)
        entry_d = Decimal(str(entry))
        sl = _round_to_tick(entry_d - tick*Decimal(3), tick) if (side or "").lower().startswith("b") else _round_to_tick(entry_d + tick*Decimal(3), tick)
        _set_trading_stop(uid, symbol, pos_idx, str(sl))
        tg_send(f"🛑 SL ensured for {symbol} at {sl}", priority="info")
    except Exception as e:
        print(f"[ensure_stop] {symbol} error: {e}")

# ─────────── Whitelist ───────────
def _load_trade_guard() -> tuple[set[str], set[str]]:
    try:
        js = json.loads(TRADE_GUARD_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set(), set()
    # Accept both legacy and new keys
    allowed = (js.get("allowed_symbols") or js.get("symbols_whitelist") or []) or []
    blocked = (js.get("blocked_symbols") or js.get("symbols_blocklist") or []) or []
    return {s.upper() for s in allowed}, {s.upper() for s in blocked}

# ─────────── Alerts ───────────
def _maybe_alert(uid: str, text: str, priority: str = "warn") -> None:
    now = _now_utc()
    last = last_alert.get(uid)
    if last and (now - last) < timedelta(minutes=NOTIFY_EVERY_MIN):
        return
    try:
        tg_send(text, priority=priority)
    except Exception:
        pass
    last_alert[uid] = now

# ─────────── Core actions ───────────
def _handle_breach(uid: str, label: str, dd_pct: float, eq_now: float, reason: str) -> None:
    _write_breaker(True, {"uid": uid, "label": label, "dd_pct": dd_pct, "equity": eq_now, "reason": reason})
    msg = f"🧯 RISK BREACH — {label}\nReason: {reason}\nDD {dd_pct:.2f}% ≥ {MAX_DD_PCT:.2f}% • Eq {eq_now:.4f}\nAUTOFIX={AUTOFIX} DRY_RUN={DRY_RUN}"
    print(msg)
    _maybe_alert(uid, msg, priority="error")

    if AUTOFIX:
        ok, cancel_msg = _cancel_all_orders(uid)
        _maybe_alert(uid, f"🔪 cancel-all: {label} • {cancel_msg}", priority="warn")
        # flatten positions
        pos = _positions_linear(uid)
        rows = (pos.get("result") or {}).get("list") or []
        for p in rows:
            try:
                size = float(p.get("size") or 0)
                if size <= 0: continue
                symbol = p.get("symbol"); side = p.get("side") or "Buy"
                close_side = "Sell" if side == "Buy" else "Buy"
                if DRY_RUN:
                    _maybe_alert(uid, f"[DRY] {label}: would close {symbol} qty {p.get('size')}", priority="warn")
                else:
                    r2 = _place_reduce_market(uid, symbol, close_side, str(p.get("size")))
                    _maybe_alert(uid, f"🏁 flatten {label}:{symbol} retCode={r2.get('retCode')}", priority="warn")
                    time.sleep(0.1)
            except Exception as e:
                print(f"[flatten] {label}:{p.get('symbol')} err {e}")

def _maybe_clear_breaker():
    _write_breaker(False, {"note": "cleared; no active breaches"})

# ─────────── Main loop ───────────
def main():
    print(f"Risk Daemon • poll {POLL_SEC}s • maxDD {MAX_DD_PCT:.2f}% • autofix={AUTOFIX} • dry={DRY_RUN}")
    uids, name_map = load_sub_uids()
    if not uids:
        msg = "⚠️ Risk Daemon: sub_uids.csv missing or empty."
        print(msg); _maybe_alert("global", msg, priority="warn"); return

    # Load anchors if same UTC day; else start new day
    now = _now_utc()
    current_day = _utc_day_key(now)
    saved_day, saved_anchors = _load_anchors()
    if saved_day == current_day and saved_anchors:
        anchors.update(saved_anchors)
        print(f"[anchors] loaded for {current_day} → {len(anchors)} subs")
    else:
        anchors.clear()
        print(f"[anchors] new UTC day {current_day}: starting fresh")

    boot_ts = time.time()
    try:
        tg_send("🛡️ Base44 Risk Daemon online.", priority="success")
    except Exception:
        pass

    while True:
        try:
            loop_ts = _now_utc()
            day = _utc_day_key(loop_ts)
            if day != current_day:
                anchors.clear()
                equity_ewma.clear()
                current_day = day
                _save_anchors(current_day, anchors)
                print(f"🕛 New UTC day {day}: resetting anchors.")
                _maybe_alert("global", f"🕛 New UTC day {day}: reset risk anchors.", priority="info")

            any_breach = False
            allowed, blocked = _load_trade_guard() if ENFORCE_WHITELIST else (set(), set())
            manual_breach = bool(_read_breaker().get("breach"))

            for uid in uids:
                label = pretty_name(uid, name_map)

                # Equity via relay
                bal = _wallet_balance_unified(uid, "USDT")
                eq_raw = _total_equity_from_resp(bal)
                if eq_raw is None:
                    print(f"[{label}] equity fetch err retCode={bal.get('retCode') if isinstance(bal, dict) else None} msg={bal.get('retMsg') if isinstance(bal, dict) else None}")
                    continue

                if uid not in anchors or anchors[uid] is None:
                    anchors[uid] = eq_raw
                    _save_anchors(current_day, anchors)
                    print(f"[{label}] anchor set: {eq_raw:.6f}")

                # EWMA smoothing
                e_prev = equity_ewma.get(uid, eq_raw)
                e_now = EWMA_ALPHA * eq_raw + (1 - EWMA_ALPHA) * e_prev
                equity_ewma[uid] = e_now

                anchor = anchors[uid]
                dd_pct_smoothed = (max(0.0, anchor - e_now) / anchor) * 100.0 if anchor > 0 else 0.0
                print(f"[{label}] eq {eq_raw:.6f} (ewma {e_now:.6f}) • anchor {anchor:.6f} • DD {dd_pct_smoothed:.2f}%")

                # Positions via relay; ensure SL; whitelist
                rows_all = []
                for coin in SETTLE_COINS:
                    try:
                        resp = _positions_linear(uid, coin)
                        rows_all.extend((resp.get("result") or {}).get("list") or [])
                    except Exception as e:
                        print(f"[{label}] list positions ({coin}) err: {e}")

                # Ensure SL + whitelist enforcement
                for p in rows_all:
                    try:
                        size = float(p.get("size") or 0)
                        if size <= 0: continue
                        symbol = p.get("symbol") or ""
                        if not symbol: continue
                        side = p.get("side") or "Buy"
                        entry = p.get("avgPrice") or p.get("entryPrice") or "0"
                        pos_idx = int(p.get("positionIdx") or 0)

                        _ensure_stop(uid, symbol, side, entry, pos_idx)

                        if ENFORCE_WHITELIST:
                            s_up = symbol.upper()
                            ok = True
                            if allowed:
                                ok = s_up in allowed
                            if s_up in blocked:
                                ok = False
                            if not ok:
                                reason = f"whitelist violation ({symbol})"
                                _maybe_alert(uid, f"⚠️ {label}: {reason}", priority="warn")
                                if CLOSE_ON_VIOLATION:
                                    close_side = "Sell" if side == "Buy" else "Buy"
                                    if DRY_RUN:
                                        _maybe_alert(uid, f"[DRY] {label}: would close {symbol} due to {reason}", priority="warn")
                                    else:
                                        r = _place_reduce_market(uid, symbol, close_side, str(p.get("size")))
                                        _maybe_alert(uid, f"❌ {label}: closed {symbol} retCode={r.get('retCode')}", priority="warn")
                    except Exception as e:
                        print(f"[{label}] protect/whitelist err: {e}")

                in_grace = (time.time() - boot_ts) < STARTUP_GRACE_SEC
                threshold = MAX_DD_PCT
                clear_lvl = max(0.0, MAX_DD_PCT - HYSTERESIS_PCT)

                if manual_breach:
                    any_breach = True
                    _handle_breach(uid, label, dd_pct_smoothed, eq_raw, reason="manual")
                elif not in_grace and dd_pct_smoothed >= threshold:
                    any_breach = True
                    _handle_breach(uid, label, dd_pct_smoothed, eq_raw, reason="drawdown")
                else:
                    # recovered region check could be added if needed
                    pass

                # clear-region is global below

            if any_breach:
                _write_breaker(True, {"note": "active breach across one or more UIDs"})
            else:
                _maybe_clear_breaker()

            # Persist anchors periodically
            _save_anchors(current_day, anchors)

            # Optional CSV snapshot
            if CSV_PATH:
                try:
                    p = Path(CSV_PATH); p.parent.mkdir(parents=True, exist_ok=True)
                    new = not p.exists()
                    with p.open("a", encoding="utf-8", newline="") as f:
                        if new:
                            f.write("timestamp,uid,label,equity,anchor,dd_pct\n")
                        for uid in uids:
                            eqv = equity_ewma.get(uid)
                            if eqv is None: continue
                            anc = anchors.get(uid, 0.0)
                            dd = (max(0.0, anc - eqv) / anc) * 100.0 if anc > 0 else 0.0
                            f.write(f"{loop_ts.isoformat()},{uid},{pretty_name(uid, name_map)},{eqv:.6f},{anc:.6f},{dd:.4f}\n")
                except Exception as e:
                    print(f"[risk_daemon] warn: csv log failed: {e}")

            time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            print("Risk Daemon stopping by user request.")
            break
        except Exception as e:
            print(f"[loop] exception: {e}")
            time.sleep(min(5, POLL_SEC))

if __name__ == "__main__":
    main()
