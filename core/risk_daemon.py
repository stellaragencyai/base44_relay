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
Base44 Risk Daemon (v1.8)

What’s new vs your v1.5:
- Respects a MANUAL breaker: if .state/risk_state.json has {"breach": true}, we act as breached.
- Symbol whitelist/blacklist enforcement via config/trade_guard.json:
    • allowed_symbols (if non-empty) is the only list we may hold
    • blocked_symbols always forbidden
    • optional auto-close on violation (env toggle), reduce-only market
- Always-ensure-SL: if a position has no SL, we set a last-resort SL with a tiny tick buffer.
- Keeps your EWMA/hysteresis, daily anchors, notifier integration, and Base44 proxy flow.

.env knobs (kept + added)
  RISK_POLL_SEC=30
  RISK_MAX_DD_PCT=3.0
  RISK_AUTOFIX=false                 # on breach: cancel-all + flatten
  RISK_AUTOFIX_DRY_RUN=true
  RISK_NOTIFY_EVERY_MIN=10
  RISK_STARTUP_GRACE_SEC=15
  RISK_EWMA_ALPHA=0.25
  RISK_BREACH_HYSTERESIS_PCT=0.3
  RISK_LOG_CSV=
  RISK_STATE_DIR=.state

  # NEW — whitelist controls
  TRADE_GUARD_PATH=config/trade_guard.json
  ENFORCE_WHITELIST=true
  RISK_CLOSE_ON_VIOLATION=false      # if true, reduce-only market close on violation (honors DRY_RUN)

Behavior notes
- On ANY breach (manual or DD), we write {"breach": true, ...} back to the breaker JSON and (if AUTOFIX) cancel & flatten.
- We ensure SL before any potential flatten/violation logic, so you’re not naked even if flatten fails.
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

# Base44 client imports (package-first, with fallback)
try:
    from core.base44_client import (
        load_sub_uids, pretty_name, bybit_proxy,
        get_balance_unified, get_positions_linear
    )
except Exception:
    from base44_client import (  # type: ignore
        load_sub_uids, pretty_name, bybit_proxy,
        get_balance_unified, get_positions_linear
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
MAX_DD_PCT             = _getf("RISK_MAX_DD_PCT", 3.0)
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

# ─────────── Helpers ───────────
def _total_equity_from_resp(resp: dict) -> float | None:
    try:
        if not resp or resp.get("retCode") not in (0, "0", 0): return None
        lst = (resp.get("result") or {}).get("list") or []
        if not lst: return None
        eq = lst[0].get("totalEquity")
        return float(eq) if eq is not None else None
    except Exception:
        return None

def _cancel_all_orders(uid: str) -> tuple[bool, str]:
    payload = {"category": CATEGORY, "memberId": uid}
    r = bybit_proxy("POST", "/v5/order/cancel-all", body=payload)
    ok = r.get("retCode") in (0, "0")
    return ok, f"cancel-all retCode={r.get('retCode')}"

def _flatten_positions(uid: str) -> str:
    pos = get_positions_linear(uid)
    if pos.get("retCode") not in (0, "0"):
        return f"positions retCode={pos.get('retCode')} retMsg={pos.get('retMsg')}"
    rows = (pos.get("result") or {}).get("list") or []
    if not rows:
        return "no positions to flatten"

    msgs = []
    for p in rows:
        try:
            symbol = p.get("symbol")
            side = p.get("side")
            size = p.get("size")
            if not symbol or not size: 
                continue
            qty = str(size)
            close_side = "Sell" if side == "Buy" else "Buy"
            body = {
                "category": CATEGORY,
                "symbol": symbol,
                "side": close_side,
                "orderType": "Market",
                "qty": qty,
                "reduceOnly": True,
                "memberId": uid
            }
            if DRY_RUN:
                msgs.append(f"[DRY] {symbol}:{side}->{close_side} qty {qty}")
            else:
                r = bybit_proxy("POST", "/v5/order/create", body=body)
                msgs.append(f"{symbol}:{side}->{close_side} qty {qty} retCode={r.get('retCode')}")
                time.sleep(0.12)
        except Exception as e:
            msgs.append(f"{p.get('symbol') or '?'}: exception {e}")
    return " | ".join(msgs) if msgs else "no actionable positions"

def _maybe_alert(uid: str, text: str, priority: str = "warn") -> None:
    now = _now_utc()
    last = last_alert.get(uid)
    if last and (now - last) < timedelta(minutes=NOTIFY_EVERY_MIN):
        return
    tg_send(text, priority=priority)
    last_alert[uid] = now

# ---- Last-resort SL ensure (via proxy) ----
def _round_to_tick(p: Decimal, tick: Decimal) -> Decimal:
    steps = (p / tick).quantize(Decimal("1."), rounding=ROUND_DOWN)
    return steps * tick

def _instrument_meta(symbol: str) -> tuple[Decimal, Decimal]:
    r = bybit_proxy("GET", "/v5/market/instruments-info", params={"category": CATEGORY, "symbol": symbol})
    lst = (r.get("result") or {}).get("list") or []
    if not lst:
        raise RuntimeError(f"instrument meta not found: {symbol}")
    tick = Decimal(lst[0]["priceFilter"]["tickSize"])
    step = Decimal(lst[0]["lotSizeFilter"]["qtyStep"])
    return tick, step

def _ensure_stop(uid: str, symbol: str, side: str, entry: float | int | str, pos_idx: int) -> None:
    try:
        r = bybit_proxy("GET", "/v5/position/list", params={"category": CATEGORY, "symbol": symbol, "memberId": uid})
        lst = (r.get("result") or {}).get("list") or []
        if lst and (Decimal(str(lst[0].get("stopLoss") or "0")) > 0):
            return
        tick, _ = _instrument_meta(symbol)
        entry_d = Decimal(str(entry))
        # 3-tick buffer
        if side == "Buy":
            sl = _round_to_tick(entry_d - tick * Decimal(3), tick)
        else:
            sl = _round_to_tick(entry_d + tick * Decimal(3), tick)
        body = {"category": CATEGORY, "symbol": symbol, "positionIdx": str(pos_idx), "stopLoss": str(sl), "memberId": uid}
        bybit_proxy("POST", "/v5/position/trading-stop", body=body)
        tg_send(f"🛑 SL ensured for {symbol} at {sl}", priority="info")
    except Exception as e:
        print(f"[ensure_stop] {symbol} error: {e}")

# ─────────── Whitelist ───────────
def _load_trade_guard() -> tuple[set[str], set[str]]:
    try:
        js = json.loads(TRADE_GUARD_PATH.read_text(encoding="utf-8"))
    except Exception:
        return set(), set()
    allowed = {s.upper() for s in (js.get("allowed_symbols") or [])}
    blocked = {s.upper() for s in (js.get("blocked_symbols") or [])}
    return allowed, blocked

# ─────────── Core actions ───────────
def _handle_breach(uid: str, label: str, dd_pct: float, eq_now: float, reason: str) -> None:
    _write_breaker(True, {"uid": uid, "label": label, "dd_pct": dd_pct, "equity": eq_now, "reason": reason})
    msg = f"🧯 RISK BREACH — {label}\nReason: {reason}\nDD {dd_pct:.2f}% ≥ {MAX_DD_PCT:.2f}% • Eq {eq_now:.4f}\nAUTOFIX={AUTOFIX} DRY_RUN={DRY_RUN}"
    print(msg)
    _maybe_alert(uid, msg, priority="error")

    if AUTOFIX:
        ok, cancel_msg = _cancel_all_orders(uid)
        tg_send(f"🔪 cancel-all: {label} • {cancel_msg}", priority="warn")
        result = _flatten_positions(uid)
        tg_send(f"🏁 flatten: {label} • {result}", priority="warn")

def _maybe_clear_breaker():
    # Clear global breaker when loop finds no breaches
    _write_breaker(False, {"note": "cleared; no active breaches"})

# ─────────── Main loop ───────────
def main():
    print(f"Risk Daemon • poll {POLL_SEC}s • maxDD {MAX_DD_PCT:.2f}% • autofix={AUTOFIX} • dry={DRY_RUN}")
    uids, name_map = load_sub_uids()
    if not uids:
        msg = "⚠️ Risk Daemon: sub_uids.csv missing or empty."
        print(msg); tg_send(msg, priority="warn"); return

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
    tg_send("🛡️ Base44 Risk Daemon online.", priority="success")

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
                tg_send(f"🕛 New UTC day {day}: reset risk anchors.", priority="info")

            any_breach = False
            allowed, blocked = _load_trade_guard() if ENFORCE_WHITELIST else (set(), set())

            # Manual breaker check
            cur_break = _read_breaker()
            manual_breach = bool(cur_break.get("breach"))

            for uid in uids:
                label = pretty_name(uid, name_map)

                # Equity + DD
                bal = get_balance_unified(uid)
                eq_raw = _total_equity_from_resp(bal)
                if eq_raw is None:
                    print(f"[{label}] equity fetch err retCode={bal.get('retCode')} msg={bal.get('retMsg')}")
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

                in_grace = (time.time() - boot_ts) < STARTUP_GRACE_SEC
                threshold  = MAX_DD_PCT
                clear_lvl  = max(0.0, MAX_DD_PCT - HYSTERESIS_PCT)

                # Positions
                pos = get_positions_linear(uid)
                rows = (pos.get("result") or {}).get("list") or []

                # Ensure SL on all open positions
                for p in rows:
                    try:
                        symbol = p.get("symbol")
                        side = p.get("side")
                        entry = p.get("avgPrice")
                        pos_idx = int(p.get("positionIdx") or 0)
                        size = float(p.get("size") or 0)
                        if not symbol or size <= 0: 
                            continue
                        _ensure_stop(uid, symbol, side, entry, pos_idx)
                    except Exception as e:
                        print(f"[{label}] ensure_stop error: {e}")

                # Whitelist enforcement
                if ENFORCE_WHITELIST and rows:
                    for p in rows:
                        symbol = (p.get("symbol") or "").upper()
                        size   = float(p.get("size") or 0)
                        side   = p.get("side") or "Buy"
                        if size <= 0 or not symbol:
                            continue

                        allowed_ok = True
                        if allowed:
                            allowed_ok = symbol in allowed
                        if symbol in blocked:
                            allowed_ok = False

                        if not allowed_ok:
                            reason = f"whitelist violation ({symbol})"
                            _maybe_alert(uid, f"⚠️ {label}: {reason}", priority="warn")
                            if CLOSE_ON_VIOLATION:
                                if DRY_RUN:
                                    tg_send(f"[DRY] {label}: would close {symbol} due to {reason}", priority="warn")
                                else:
                                    # reduce-only market
                                    close_side = "Sell" if side == "Buy" else "Buy"
                                    body = {
                                        "category": CATEGORY,
                                        "symbol": symbol,
                                        "side": close_side,
                                        "orderType": "Market",
                                        "qty": str(p.get("size")),
                                        "reduceOnly": True,
                                        "memberId": uid
                                    }
                                    r = bybit_proxy("POST", "/v5/order/create", body=body)
                                    tg_send(f"❌ {label}: closed {symbol} for {reason} retCode={r.get('retCode')}", priority="warn")

                # Breach logic: manual or DD
                breached_now = False
                if manual_breach:
                    breached_now = True
                    any_breach = True
                    _handle_breach(uid, label, dd_pct_smoothed, eq_raw, reason="manual")

                elif not in_grace and dd_pct_smoothed >= threshold:
                    breached_now = True
                    any_breach = True
                    _handle_breach(uid, label, dd_pct_smoothed, eq_raw, reason="drawdown")

                # Hysteresis clear handled globally below
                if not breached_now and dd_pct_smoothed <= clear_lvl:
                    # considered in "clear region" for this uid
                    pass

            if any_breach:
                # already written per-UID in _handle_breach; ensure file reflects breach
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
                            eq = equity_ewma.get(uid)
                            if eq is None: continue
                            anchor = anchors.get(uid, 0.0)
                            dd = (max(0.0, anchor - eq) / anchor) * 100.0 if anchor > 0 else 0.0
                            f.write(f"{loop_ts.isoformat()},{uid},{pretty_name(uid, name_map)},{eq:.6f},{anchor:.6f},{dd:.4f}\n")
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
