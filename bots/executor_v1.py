#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Auto Executor (merged v1)
Breaker-aware, ownership-tagged, risk-sized; keeps your Bayesian nudge, feature logging,
DB hooks, corr gate, and spread checks. Plays nice with TP/SL Manager and Portfolio Guard.

Signal source: signals/observed.jsonl (append-only)
"""

from __future__ import annotations
import json
import os
import time
import datetime as _dt
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from core.config import settings
from core.logger import get_logger, bind_context
from core.bybit_client import Bybit
from core.notifier_bot import tg_send

# Enhancements (feature memory, classifier, corr gate, sizing helpers)
from core.feature_store import log_features
from core.trade_classifier import classify as classify_trade
from core.corr_gate import allow as corr_allow
from core.sizing import bayesian_size, risk_capped_qty

# Preferred global guard
try:
    from core.guard import guard_blocking_reason  # (blocked: bool, reason: str)
except Exception:
    guard_blocking_reason = None  # type: ignore

# Optional legacy breaker fallback
try:
    from core.breaker import is_active as legacy_breaker_is_active  # type: ignore
except Exception:
    legacy_breaker_is_active = None  # type: ignore

# Optional structured decision log (soft dep)
try:
    from core.decision_log import log_event
except Exception:
    def log_event(*_, **__):  # type: ignore
        pass

# Optional Portfolio Guard risk pool (register opens/closes)
_pg_ok = True
try:
    from core.portfolio_guard import guard as pg_guard  # type: ignore
except Exception:
    _pg_ok = False
    pg_guard = None  # type: ignore

# DB hooks (optional; fall back cleanly if Core/ vs core/ casing differs or DB not present)
try:
    import importlib
    try:
        _db_mod = importlib.import_module("Core.db")
    except Exception:
        _db_mod = importlib.import_module("core.db")
    insert_order = getattr(_db_mod, "insert_order", None)
    set_order_state = getattr(_db_mod, "set_order_state", None)
    insert_execution = getattr(_db_mod, "insert_execution", None)
except Exception:
    insert_order = set_order_state = insert_execution = None

# Ownership tagging (prefer core.order_tag)
def _owner_tag_build() -> str:
    try:
        from core.order_tag import build_tag
        sub = str(getattr(settings, "OWNERSHIP_SUB_UID", "") or os.getenv("OWNERSHIP_SUB_UID") or "sub")
        strat = str(getattr(settings, "OWNERSHIP_STRATEGY", "") or os.getenv("OWNERSHIP_STRATEGY") or "A?")
        return build_tag(sub, strat)
    except Exception:
        base = (str(getattr(settings, "TP_MANAGED_TAG", "B44") or os.getenv("TP_MANAGED_TAG") or "B44").strip() or "B44")[:12]
        sid = os.environ.get("B44_SESSION_ID") or time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        sub = str(getattr(settings, "OWNERSHIP_SUB_UID", "") or os.getenv("OWNERSHIP_SUB_UID") or "sub")
        strat = str(getattr(settings, "OWNERSHIP_STRATEGY", "") or os.getenv("OWNERSHIP_STRATEGY") or "A?")
        return f"{base}:{sub}:{strat}:{sid}"

def _attach_link_id(base: str, tag: str) -> str:
    try:
        from core.order_tag import attach_to_client_order_id
        return attach_to_client_order_id(base, tag)
    except Exception:
        base_clean = (base or "B44").replace(" ", "")[:24]
        tail = tag.replace(":", "-")
        return f"{base_clean}|{tail}"[:64]

OWNER_TAG = _owner_tag_build()

log = bind_context(get_logger("bots.auto_executor"), comp="executor")

# ------------------------
# Config
# ------------------------

ROOT: Path = settings.ROOT
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SIGNALS_DIR: Path = getattr(settings, "DIR_SIGNALS", ROOT / "signals")
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_PATH = SIGNALS_DIR / (getattr(settings, "SIGNAL_QUEUE_FILE", "observed.jsonl"))

# Env helpers
def _get_env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        try:
            return bool(getattr(settings, name))
        except Exception:
            return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _get_env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        try:
            return float(getattr(settings, name))
        except Exception:
            return default
    try:
        return float(v)
    except Exception:
        return default

def _get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        try:
            return int(getattr(settings, name))
        except Exception:
            return default
    try:
        return int(v)
    except Exception:
        return default

# Maker/Tag/Spread from signal-engine defaults
MAKER_ONLY      = _get_env_bool("SIG_MAKER_ONLY", True)
SPREAD_MAX_BPS  = _get_env_float("SIG_SPREAD_MAX_BPS", 8.0)
TAG             = (getattr(settings, "SIG_TAG", None) or os.getenv("SIG_TAG") or "B44").strip() or "B44"
SIG_DRY_DEFAULT = _get_env_bool("SIG_DRY_RUN", True)

# Executor-specific
EXEC_DRY_RUN         = _get_env_bool("EXEC_DRY_RUN", SIG_DRY_DEFAULT) or _get_env_bool("EXEC_REALLY_DRY_RUN", SIG_DRY_DEFAULT)
EXEC_QTY_USDT        = _get_env_float("EXEC_QTY_USDT", 5.0)
EXEC_QTY_BASE        = _get_env_float("EXEC_QTY_BASE", 0.0)
EXEC_POST_ONLY       = _get_env_bool("EXEC_POST_ONLY", True)
EXEC_POLL_SEC        = _get_env_int("EXEC_POLL_SEC", 2)
EXEC_MAX_SIGNAL_AGE  = _get_env_int("EXEC_MAX_SIGNAL_AGE_SEC", 120)
EXEC_ACCOUNT_UID     = (os.getenv("EXEC_ACCOUNT_UID") or "").strip() or None

# Bayesian defaults if no estimates provided by features
PRIOR_WIN_P          = _get_env_float("EXEC_PRIOR_WIN_P", 0.55)
EVIDENCE_WIN_P_FALLB = _get_env_float("EXEC_EVIDENCE_WIN_P", 0.55)
BAYES_K              = _get_env_float("EXEC_BAYES_GAIN", 0.8)

# Optional symbol allowlist
_raw_allow = (os.getenv("EXEC_SYMBOL_LIST") or getattr(settings, "EXEC_SYMBOLS", "") or "").strip()
EXEC_SYMBOLS: Optional[List[str]] = [s.strip().upper() for s in _raw_allow.split(",") if s.strip()] or None

# Exposure cap (new)
EX_MAX_GROSS_PCT = _get_env_float("EX_MAX_GROSS_PCT", 0.60)

# persistent registries
SEEN_FILE   = STATE_DIR / "executor_seen.json"      # orderLinkId registry
OFFSET_FILE = STATE_DIR / "executor_offset.json"    # queue offset, for resilience
REGIME_FILE = STATE_DIR / "regime_state.json"       # shared regime context

# Risk buckets + session windows config (new)
EX_BUCKETS_JSON = os.getenv("EX_BUCKETS_JSON", "cfg/risk_buckets.json")
EX_SESSIONS_JSON = os.getenv("EX_SESSIONS_JSON", "cfg/sessions.json")

# Bybit client
by = Bybit()
try:
    by.sync_time()  # best-effort
except Exception as e:
    log.warning("time sync failed: %s", e)

# ------------------------
# Helpers
# ------------------------

def _fallback_breaker_active_file() -> bool:
    path = STATE_DIR / "risk_state.json"
    try:
        if not path.exists():
            return False
        js = json.loads(path.read_text(encoding="utf-8"))
        return bool(js.get("breach") or js.get("breaker") or js.get("active"))
    except Exception:
        return False

def breaker_active() -> Tuple[bool, str]:
    if callable(guard_blocking_reason):
        try:
            blocked, why = guard_blocking_reason()  # type: ignore
            return bool(blocked), str(why or "")
        except Exception:
            pass
    if callable(legacy_breaker_is_active):
        try:
            if bool(legacy_breaker_is_active()):
                return True, "legacy_breaker"
        except Exception:
            pass
    return (_fallback_breaker_active_file(), "file_breaker")

def _load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("failed reading %s: %s", path.name, e)
    return default

def _save_json(path: Path, obj) -> None:
    try:
        path.write_text(json.dumps(obj, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log.error("failed writing %s: %s", path.name, e)

def _mk_link_id(symbol: str, ts_ms: int, signal: str, tag: str) -> str:
    base = f"{tag}-{symbol}-{int(ts_ms/1000)}-{signal}".replace(" ", "")[:24]
    return _attach_link_id(base, OWNER_TAG)

def _fetch_best_prices(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    ok, data, err = by.get_tickers(category="linear", symbol=symbol)
    if not ok:
        log.warning("ticker fail %s: %s", symbol, err)
        return None, None, None
    try:
        items = (data.get("result") or {}).get("list") or []
        if not items:
            return None, None, None
        item = items[0]
        bid = float(item.get("bid1Price"))
        ask = float(item.get("ask1Price"))
        if bid <= 0 or ask <= 0:
            return None, None, None
        mid = (bid + ask) / 2.0
        return bid, ask, mid
    except Exception as e:
        log.warning("ticker parse fail %s: %s", symbol, e)
        return None, None, None

def _spread_bps(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 1e9
    return (ask - bid) / mid * 10000.0

def _wallet_equity() -> float:
    ok, data, err = by._request_private_json("/v5/account/wallet-balance", params={"accountType":"UNIFIED"})
    if not ok or not isinstance(data, dict):
        raise RuntimeError(err or "wallet-balance error")
    total = 0.0
    for acc in (data.get("result") or {}).get("list") or []:
        try:
            total += float(acc.get("totalEquity") or 0)
        except Exception:
            pass
    return total

def _gross_exposure_usdt() -> float:
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        return 0.0
    gross = 0.0
    for p in (data.get("result") or {}).get("list") or []:
        try:
            sz = float(p.get("size") or 0)
            px = float(p.get("avgPrice") or 0)
            gross += abs(sz * px)
        except Exception:
            continue
    return gross

# ---- risk buckets + sessions (no news) ----

def _load_json_safe(path_str: str, default):
    p = Path(path_str)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default

_BUCKETS = _load_json_safe(EX_BUCKETS_JSON, {"default_risk_pct": 0.003, "by_class": {}})
_SESS    = _load_json_safe(EX_SESSIONS_JSON, {})

def _risk_pct_for_class(cls: str) -> float:
    dflt = float(_BUCKETS.get("default_risk_pct", 0.003))
    return float((_BUCKETS.get("by_class") or {}).get((cls or "default"), dflt))

def _now_hhmm_local() -> str:
    return _dt.datetime.now().strftime("%H:%M")

def _in_ranges(hhmm: str, ranges: List[str]) -> bool:
    try:
        h, m = map(int, hhmm.split(":"))
        cur = h * 60 + m
        for r in ranges:
            a, b = r.split("-")
            ha, ma = map(int, a.split(":"))
            hb, mb = map(int, b.split(":"))
            start = ha * 60 + ma
            end = hb * 60 + mb
            if (start <= end and start <= cur <= end) or (start > end and (cur >= start or cur <= end)):
                return True
    except Exception:
        return True
    return False

def can_trade_now(sym: str, cls: str) -> bool:
    cfg = _SESS.get(sym) or {}
    ranges = cfg.get(cls) or cfg.get("default") or ["00:00-23:59"]
    return _in_ranges(_now_hhmm_local(), ranges)

# ------------------------
# Sizing
# ------------------------

def _qty_core(price: float, params: Dict) -> float:
    """
    Sizing precedence:
      1) EXEC_QTY_BASE (>0) → fixed base qty
      2) If params.stop_dist present → risk_capped sizing (uses remaining_risk_usd if available)
      3) Fallback to notional: EXEC_QTY_USDT / price
    Then scale by per-class risk_pct if provided (default 0.003).
    """
    risk_pct = float(params.get("risk_pct") or 0.003)
    base_qty_usd = max(0.0, EXEC_QTY_USDT * (risk_pct / 0.003))

    if EXEC_QTY_BASE > 0:
        qty = EXEC_QTY_BASE
    elif params.get("stop_dist"):
        try:
            px_delta = float(params["stop_dist"])
            remaining_risk_usd = None
            try:
                if _pg_ok and hasattr(pg_guard, "current_risk_value"):
                    remaining_risk_usd = float(pg_guard.current_risk_value())
            except Exception:
                pass
            qty = risk_capped_qty(
                remaining_risk_usd=(remaining_risk_usd if remaining_risk_usd is not None else base_qty_usd),
                stop_dist_px=max(px_delta, 1e-9),
                px=max(price, 1e-9),
                min_qty=0.0
            )
        except Exception:
            qty = base_qty_usd / max(price, 1e-9)
    else:
        qty = base_qty_usd / max(price, 1e-9)

    return max(0.0, qty)

def _format_qty(qty: float) -> str:
    txt = f"{qty:.10f}".rstrip("0").rstrip(".")
    return txt or "0"

def _read_offset() -> int:
    obj = _load_json(OFFSET_FILE, {"pos": 0})
    try:
        return int(obj.get("pos", 0))
    except Exception:
        return 0

def _write_offset(pos: int) -> None:
    _save_json(OFFSET_FILE, {"pos": int(pos)})

def _load_seen() -> Dict[str, int]:
    return _load_json(SEEN_FILE, {})

def _save_seen(seen: Dict[str, int]) -> None:
    _save_json(SEEN_FILE, seen)

def _tail_queue(path: Path, start_pos: int) -> Tuple[int, List[str]]:
    if not path.exists():
        return start_pos, []
    size = path.stat().st_size
    pos = start_pos if 0 <= start_pos <= size else 0

    new_pos = pos
    lines: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        fh.seek(pos, 0)
        for line in fh:
            line = line.strip()
            if not line:
                continue
            lines.append(line)
        new_pos = fh.tell()
    return new_pos, lines

def _load_regime_snapshot() -> Dict:
    try:
        return json.loads(REGIME_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

# ------------------------
# DB recorders
# ------------------------

def _record_order_state(link_id: str, symbol: str, side: str, qty_val: float, px: Optional[float],
                        state: str, exchange_id: Optional[str] = None,
                        err_code: Optional[str] = None, err_msg: Optional[str] = None) -> None:
    if insert_order is None or set_order_state is None:
        return
    try:
        if state == "NEW":
            insert_order(link_id, symbol, side, qty_val, px, OWNER_TAG, state="NEW")
        else:
            set_order_state(link_id, state, exchange_id=exchange_id, err_code=err_code, err_msg=err_msg)
    except Exception as e:
        log.warning("DB write failed (%s %s): %s", link_id, state, e)

def _record_execution(link_id: str, qty_val: float, px: float, fee: float = 0.0) -> None:
    if insert_execution is None:
        return
    try:
        insert_execution(link_id, qty_val, px, fee=fee)
    except Exception as e:
        log.warning("DB exec write failed (%s): %s", link_id, e)

# ------------------------
# Outcome-watcher hook: persist entry metadata
# ------------------------

def _save_entry_meta(link_id: str, symbol: str, side: str, setup_tag: Optional[str], stop_dist: Optional[float]) -> None:
    """
    Store entry metadata so bots/outcome_watcher.py can compute realized R later.
    """
    try:
        p = STATE_DIR / "entries_meta.json"
        obj = {}
        if p.exists():
            obj = json.loads(p.read_text(encoding="utf-8"))
        obj[link_id or symbol] = {
            "symbol": symbol,
            "side": side,
            "setup_tag": (setup_tag or "Unknown"),
            "stop_dist": (float(stop_dist) if stop_dist is not None else None),
            "ts": int(time.time() * 1000),
        }
        p.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    except Exception:
        pass

# ------------------------
# Placement
# ------------------------

def _place_entry(symbol: str, side: str, link_id: str, params: Dict, price_hint: Optional[float],
                 features: Dict) -> Tuple[bool, str]:
    bid, ask, mid = _fetch_best_prices(symbol)
    if bid is None or ask is None:
        return False, "no orderbook"

    spr_bps = _spread_bps(bid, ask)
    max_bps = float(params.get("spread_max_bps", SPREAD_MAX_BPS))
    if spr_bps > max_bps:
        return False, f"spread {spr_bps:.2f} bps > max {max_bps}"

    maker_only = bool(params.get("maker_only", MAKER_ONLY))

    if maker_only:
        px = bid if side == "Buy" else ask
        if isinstance(price_hint, (int, float)) and price_hint > 0:
            if side == "Buy":
                px = min(px, float(price_hint))
            else:
                px = max(px, float(price_hint))
    else:
        px = None  # market

    base_qty = _qty_core(price=px or mid, params=params)
    prior_p  = float(features.get("prior_win_p", PRIOR_WIN_P))
    edge_p   = float(features.get("edge_prob", EVIDENCE_WIN_P_FALLB))
    qty_val  = bayesian_size(base_qty, prior_win_p=prior_p, evidence_win_p=edge_p, k=BAYES_K)

    qty_txt = _format_qty(qty_val)
    tif = "PostOnly" if (EXEC_POST_ONLY and maker_only and px is not None) else ("ImmediateOrCancel" if px is None else "GoodTillCancel")

    # NEW: record entry meta for outcome watcher (setup tag + stop distance)
    setup_tag = features.get("class") or params.get("tag") or "Unknown"
    stop_dist = None
    try:
        if params.get("stop_dist"):
            stop_dist = float(params["stop_dist"])
    except Exception:
        stop_dist = None
    _save_entry_meta(link_id, symbol, side, setup_tag, stop_dist)

    _record_order_state(link_id, symbol, side, qty_val, px, state="NEW")

    # initial risk in USD if stop_dist present
    initial_risk_usd = None
    try:
        if params.get("stop_dist"):
            initial_risk_usd = max(0.0, float(qty_val) * float(params["stop_dist"]))
    except Exception:
        initial_risk_usd = None

    if EXEC_DRY_RUN:
        msg = f"🟡 DRY • {symbol} • {side} qty≈{qty_txt} @ {px if px is not None else 'MKT'} • spr {spr_bps:.2f}bps • tif={tif} • link={link_id}"
        tg_send(msg, priority="info")
        log_event("executor", "entry_dry", symbol, EXEC_ACCOUNT_UID or "MAIN",
                  {"side": side, "qty": qty_txt, "px": px, "spr_bps": spr_bps, "tif": tif, "link": link_id, "features": features})

        # Risk pool integration: open+immediate close in DRY so pool isn’t pinned
        try:
            if _pg_ok and initial_risk_usd and hasattr(pg_guard, "register_open") and hasattr(pg_guard, "register_close"):
                pg_guard.register_open(trade_id=link_id, symbol=symbol, initial_risk_usd=float(initial_risk_usd))
                pg_guard.register_close(trade_id=link_id, realized_pnl_usd=0.0, released_risk_usd=float(initial_risk_usd))
        except Exception:
            pass

        _record_order_state(link_id, symbol, side, qty_val, px, state="FILLED", exchange_id="DRY-RUN")
        _record_execution(link_id, qty_val, float(px or mid), fee=0.0)
        return True, "dry-run"

    req = dict(
        category="linear",
        symbol=symbol,
        side=side,  # Buy|Sell
        orderType=("Limit" if px is not None else "Market"),
        qty=str(qty_txt),
        reduceOnly=False,
        timeInForce=("PostOnly" if px is not None and EXEC_POST_ONLY and maker_only else ("IOC" if px is None else "GoodTillCancel")),
        orderLinkId=link_id,
        tpslMode=None,
    )
    if px is not None:
        req["price"] = f"{px}"

    ok, data, err = by.place_order(**req)
    if not ok:
        _record_order_state(link_id, symbol, side, qty_val, px, state="REJECTED", err_code="bybit_err", err_msg=str(err or "place_order failed"))
        return False, (err or "place_order failed")

    try:
        result = (data.get("result") or {})
        exch_id = result.get("orderId") or result.get("order_id")
    except Exception:
        exch_id = None

    _record_order_state(link_id, symbol, side, qty_val, px, state="SENT", exchange_id=exch_id)
    _record_order_state(link_id, symbol, side, qty_val, px, state="ACKED", exchange_id=exch_id)

    # Risk pool integration: register the initial risk once order is acknowledged
    try:
        if _pg_ok and initial_risk_usd and hasattr(pg_guard, "register_open"):
            pg_guard.register_open(trade_id=link_id, symbol=symbol, initial_risk_usd=float(initial_risk_usd))
    except Exception:
        pass

    return True, "ok"

# ------------------------
# Main loop
# ------------------------

def main() -> None:
    tg_send(
        f"🟢 Executor online • maker={MAKER_ONLY} • postOnly={EXEC_POST_ONLY} • dry={EXEC_DRY_RUN} • queue={QUEUE_PATH.name}",
        priority="success"
    )
    log.info("online • maker=%s postOnly=%s dry=%s queue=%s", MAKER_ONLY, EXEC_POST_ONLY, EXEC_DRY_RUN, QUEUE_PATH)

    seen = _load_seen()
    pos = _read_offset()

    while True:
        try:
            new_pos, lines = _tail_queue(QUEUE_PATH, pos)
            if not lines:
                time.sleep(max(1, EXEC_POLL_SEC))
                if new_pos < pos:
                    pos = new_pos
                    _write_offset(pos)
                continue

            for raw in lines:
                try:
                    obj = json.loads(raw)
                except Exception:
                    short = (raw[:200] + "…") if len(raw) > 200 else raw
                    log.warning("bad jsonl line (skip): %s", short)
                    continue

                ts_ms   = int(obj.get("ts", 0) or 0)
                now_ms  = int(time.time() * 1000)
                symbol  = str(obj.get("symbol", "")).upper()
                signal  = str(obj.get("signal", "")).upper() or str(obj.get("dir", "")).upper()
                params  = dict(obj.get("params") or {})
                features= dict(obj.get("features") or {})
                hint_px = None
                if "entry_price" in params:
                    try:
                        hint_px = float(params["entry_price"])
                    except Exception:
                        hint_px = None

                if EXEC_SYMBOLS and symbol not in EXEC_SYMBOLS:
                    continue

                if ts_ms and EXEC_MAX_SIGNAL_AGE:
                    if now_ms - ts_ms > EXEC_MAX_SIGNAL_AGE * 1000:
                        log.info("stale signal %s dropped (age=%ds)", symbol, int((now_ms - ts_ms)/1000))
                        continue

                regime = _load_regime_snapshot()
                if regime:
                    for k in ("realized_vol_bps","trend_slope","vol_z"):
                        if k in regime:
                            features.setdefault(k, regime[k])

                features["class"] = features.get("class") or classify_trade(features)
                features.setdefault("prior_win_p", PRIOR_WIN_P)
                features.setdefault("edge_prob",   EVIDENCE_WIN_P_FALLB)

                if not corr_allow(symbol):
                    tg_send(f"⏸️ Corr gate block • {symbol}", priority="warn")
                    log_event("executor", "block_corr", symbol, EXEC_ACCOUNT_UID or "MAIN", {"features": features})
                    continue

                tag = str(params.get("tag", TAG) or "B44").strip() or "B44"
                link_id = _mk_link_id(symbol, ts_ms or now_ms, ("LONG" if "LONG" in signal else "SHORT"), tag)

                if link_id in seen:
                    continue

                blocked, why = breaker_active()
                if blocked:
                    tg_send(f"⛔ Breaker ON • {why or 'guard'} • skip {symbol} {signal}", priority="warn")
                    log_event("executor", "block_breaker", symbol, EXEC_ACCOUNT_UID or "MAIN", {"signal": signal, "reason": why})
                    continue

                try:
                    eq = _wallet_equity()
                    gross = _gross_exposure_usdt()
                    if eq > 0 and (gross / eq) > EX_MAX_GROSS_PCT:
                        tg_send(f"⏸️ Gross exposure {gross/eq:.1%} > cap {EX_MAX_GROSS_PCT:.0%} • {symbol}", priority="warn")
                        log_event("executor", "block_gross", symbol, EXEC_ACCOUNT_UID or "MAIN", {"gross_pct": gross/eq})
                        continue
                except Exception as e:
                    log.warning("exposure check failed: %s", e)

                cls = features.get("class") or "default"
                if not can_trade_now(symbol, cls):
                    tg_send(f"⏸️ Session closed • {symbol} • {cls}", priority="warn")
                    log_event("executor", "block_session", symbol, EXEC_ACCOUNT_UID or "MAIN", {"class": cls})
                    continue

                params["risk_pct"] = _risk_pct_for_class(cls)

                try:
                    log_features(link_id, symbol, EXEC_ACCOUNT_UID or "MAIN", dict(features))
                except Exception as e:
                    log.warning("feature_store log failed: %s", e)

                side = "Buy" if "LONG" in signal else "Sell"

                ok, msg = _place_entry(symbol, side, link_id, params, hint_px, features)
                seen[link_id] = int(time.time())
                _save_seen(seen)

                if ok:
                    tg_send(f"✅ ENTRY • {symbol} • {side} • link={link_id}", priority="success")
                    log_event("executor", "entry_ok", symbol, EXEC_ACCOUNT_UID or "MAIN", {"side": side, "link": link_id, "features": features})
                    log.info("entry ok %s %s link=%s", symbol, side, link_id)
                else:
                    tg_send(f"⚠️ ENTRY FAIL • {symbol} • {side} • {msg}", priority="warn")
                    log_event("executor", "entry_fail", symbol, EXEC_ACCOUNT_UID or "MAIN", {"side": side, "error": msg})
                    log.warning("entry fail %s %s: %s", symbol, side, msg)

            pos = new_pos
            _write_offset(pos)

        except KeyboardInterrupt:
            log.info("shutdown requested by user")
            break
        except Exception as e:
            log.error("loop error: %s", e)
            time.sleep(1.0)

if __name__ == "__main__":
    main()
