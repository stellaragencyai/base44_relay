#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Signal Engine (observe-first, automation-ready + heartbeat)
Refactored to core stack:
- Uses core.config.settings for envs/paths
- Uses core.logger for logging
- Uses tools.notifier_telegram.tg for notifications
- Talks to Bybit v5 directly (no pybit/dotenv)

What it does:
- Scans SIG_SYMBOLS on SIG_TIMEFRAMES with a higher-timeframe bias (SIG_BIAS_TF).
- Gates by ADX on bias TF, ATR%/Price and volume z-score on intraday TFs.
- Emits human-readable alerts to Telegram (optional).
- Appends machine-readable signals to signals/observed.jsonl for an executor.
- Rate-limits per (symbol, timeframe, direction) and dedupes once per bar.
- Sends startup ping and periodic heartbeats.
- Obeys global breaker: still heartbeats, but does NOT scan or emit signals when breaker is ON.
"""

from __future__ import annotations
import json
import time
import statistics
import datetime as dt
import threading
import urllib.parse
import urllib.request
import urllib.error
from collections import deque, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from core.config import settings
from core.logger import get_logger
from tools.notifier_telegram import tg

# Guard helpers (single source of truth for breaker state)
from core.guard import guard_blocking_reason, guard_gate

log = get_logger("bots.signal_engine")

# =========================
# Config from core.settings
# =========================

ENABLED: bool = settings.SIG_ENABLED
DRY_RUN: bool = settings.SIG_DRY_RUN
SYMS: List[str] = [s.strip().upper() for s in settings.SIG_SYMBOLS.split(",") if s.strip()]
INTRA_TFS: List[int] = [int(x) for x in settings.SIG_TIMEFRAMES.split(",") if x.strip()]
BIAS_TF: int = int(settings.SIG_BIAS_TF)
HEARTBEAT_MIN: int = int(settings.SIG_HEARTBEAT_MIN)

# Feature params (fall back to sane defaults if not in .env)
def _getfloat(name: str, default: float) -> float:
    try:
        v = getattr(settings, name)
    except AttributeError:
        return default
    try:
        return float(v)
    except Exception:
        return default

def _getint(name: str, default: int) -> int:
    try:
        v = getattr(settings, name)
    except AttributeError:
        return default
    try:
        return int(v)
    except Exception:
        return default

SIG_POLL_SEC     = _getint("SIG_POLL_SEC", 30)
SIG_ADX_LEN      = _getint("SIG_ADX_LEN", 14)
SIG_ATR_LEN      = _getint("SIG_ATR_LEN", 14)
SIG_VOL_Z_WIN    = _getint("SIG_VOL_Z_WIN", 60)
SIG_MIN_ADX      = _getfloat("SIG_MIN_ADX", 18.0)
SIG_MIN_ATR_PCT  = _getfloat("SIG_MIN_ATR_PCT", 0.25)
SIG_CD_SEC       = _getint("SIG_NOTIFY_COOLDOWN_SEC", 300)

# Emit options for the executor
SIG_TAG          = getattr(settings, "SIG_TAG", "B44")
SIG_MAKER_ONLY   = bool(getattr(settings, "SIG_MAKER_ONLY", True))
SIG_SPREAD_MAX_BPS = float(getattr(settings, "SIG_SPREAD_MAX_BPS", 8.0))

STOP_MODE        = str(getattr(settings, "SIG_STOP_DIST_MODE", "auto")).strip().lower()  # auto|atr_mult|pct
STOP_ATR_MULT    = _getfloat("SIG_STOP_ATR_MULT", 3.0)
STOP_PCT         = _getfloat("SIG_STOP_PCT", 1.2)

# Output paths
SIGNALS_DIR: Path = settings.DIR_SIGNALS
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_PATH: Path = SIGNALS_DIR / "observed.jsonl"

# Timezone
TZ_STR: str = settings.TZ or "Europe/London"

# Network/base
BYBIT_BASE_URL = settings.BYBIT_BASE_URL.rstrip("/")

# =========================
# HTTP: Bybit public kline
# =========================

def _http_get(url: str, timeout: int = 15) -> Tuple[bool, Dict, str]:
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        return False, {}, f"HTTP {e.code} {body[:300]}"
    except Exception as e:
        return False, {}, f"network error: {e}"
    try:
        data = json.loads(raw)
    except Exception:
        return False, {}, f"bad json: {raw[:300]}"
    if data.get("retCode") == 0:
        return True, data, ""
    return False, data, f"retCode={data.get('retCode')} retMsg={data.get('retMsg')}"

def _tf_to_interval(tf_min: int) -> str:
    return "D" if tf_min >= 1440 else str(tf_min)

def get_kline(symbol: str, tf_min: int, limit: int = 400) -> List[Tuple[float, float, float, float, float, float]]:
    """
    Returns list of (ts, open, high, low, close, volume) oldest->newest.
    """
    qs = urllib.parse.urlencode({
        "category": "linear",
        "symbol": symbol,
        "interval": _tf_to_interval(tf_min),
        "limit": str(limit),
    })
    url = f"{BYBIT_BASE_URL}/v5/market/kline?{qs}"
    ok, data, err = _http_get(url, timeout=settings.HTTP_TIMEOUT_S)
    if not ok:
        log.warning("kline error %s %sm: %s", symbol, tf_min, err)
        return []
    arr = ((data.get("result") or {}).get("list") or [])
    out = [(float(x[0]) / 1000.0, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])) for x in arr]
    out.reverse()  # newest-first -> oldest-first
    return out

# =========================
# Technicals
# =========================

def ema(values: List[float], length: int) -> List[float]:
    if not values or length <= 1: return values[:]
    k = 2 / (length + 1)
    out: List[float] = []
    val: Optional[float] = None
    for v in values:
        val = v if val is None else (v * k + val * (1 - k))
        out.append(val)
    return out

def _true_ranges(h: List[float], l: List[float], c: List[float]) -> List[float]:
    out: List[float] = []
    prev: Optional[float] = None
    for i in range(len(c)):
        if prev is None:
            out.append(h[i] - l[i])
        else:
            out.append(max(h[i] - l[i], abs(h[i] - prev), abs(l[i] - prev)))
        prev = c[i]
    return out

def sma(values: List[float], n: int) -> List[float]:
    out: List[float] = []
    run: deque = deque([], maxlen=n)
    for v in values:
        run.append(v)
        out.append(sum(run) / len(run))
    return out

def atr(h: List[float], l: List[float], c: List[float], n: int) -> List[float]:
    return sma(_true_ranges(h, l, c), n)

def adx(h: List[float], l: List[float], c: List[float], n: int) -> List[float]:
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, len(c)):
        up = h[i] - h[i - 1]
        dn = l[i - 1] - l[i]
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)
    tr_n = sma(_true_ranges(h, l, c), n)
    pdi, mdi = [0.0] * len(c), [0.0] * len(c)
    for i in range(len(c)):
        if tr_n[i] > 0:
            pdi[i] = 100.0 * (plus_dm[i] / tr_n[i])
            mdi[i] = 100.0 * (minus_dm[i] / tr_n[i])
    dx = [0.0] * len(c)
    for i in range(len(c)):
        s = pdi[i] + mdi[i]
        dx[i] = 100.0 * abs(pdi[i] - mdi[i]) / s if s > 0 else 0.0
    return sma(dx, n)

def vol_zscore(vol: List[float], win: int) -> List[float]:
    out: List[float] = []
    run: deque = deque([], maxlen=win)
    for v in vol:
        run.append(v)
        if len(run) < 5:
            out.append(0.0)
            continue
        mu = statistics.mean(run)
        sd = statistics.pstdev(run) or 1e-9
        out.append((v - mu) / sd)
    return out

def atr_pct(atr_vals: List[float], closes: List[float]) -> List[float]:
    return [0.0 if closes[i] == 0 else 100.0 * atr_vals[i] / closes[i] for i in range(len(closes))]

# =========================
# Feature calcs
# =========================

def bias_context(symbol: str, tf: int) -> Dict:
    k = get_kline(symbol, tf, 200)
    if len(k) < max(60, SIG_ADX_LEN + 5): return {}
    ts, o, h, l, c, v = list(zip(*k))
    c, h, l = list(c), list(h), list(l)
    a = adx(h, l, c, SIG_ADX_LEN)
    e50 = ema(c, 50)
    return {
        "adx": a[-1],
        "ema50": e50[-1],
        "close": c[-1],
        "trend_up": c[-1] > e50[-1],
        "trend_dn": c[-1] < e50[-1],
        "bar_ts": ts[-1],
    }

def intra_features(symbol: str, tf: int) -> Dict:
    k = get_kline(symbol, tf, 400)
    if len(k) < max(SIG_ATR_LEN, SIG_ADX_LEN, SIG_VOL_Z_WIN) + 10: return {}
    ts, o, h, l, c, v = list(zip(*k))
    h, l, c, v = list(h), list(l), list(c), list(v)
    a = adx(h, l, c, SIG_ADX_LEN)
    av = atr(h, l, c, SIG_ATR_LEN)
    ap = atr_pct(av, c)
    vz = vol_zscore(v, SIG_VOL_Z_WIN)
    e20, e50, e200 = ema(c, 20), ema(c, 50), ema(c, 200)
    recent = c[-3:]
    return {
        "adx": a[-1],
        "atrp": ap[-1],
        "vz": vz[-1],
        "close": c[-1],
        "ema20": e20[-1],
        "ema50": e50[-1],
        "ema200": e200[-1],
        "pullback_ok": (e20[-1] > e50[-1] > e200[-1]) and (c[-1] >= e50[-1]),
        "breakout_ok": (c[-1] > max(recent)) and (vz[-1] > 0.8),
        "trend_dn_ok": (e20[-1] < e50[-1] < e200[-1]) and (c[-1] <= e50[-1]),
        "breakdown_ok": (c[-1] < min(recent)) and (vz[-1] > 0.8),
        "atr": av[-1],
        "bar_ts": ts[-1],
    }

# =========================
# Decisioning
# =========================

def decide(symbol: str, tf: int, bias: Dict, f: Dict) -> Tuple[bool, str, str, float]:
    """
    Returns (ok, direction, why, confidence[0..1]).
    """
    if not bias: return (False, "", "insufficient bias", 0.0)
    if not f:    return (False, "", "insufficient intraday", 0.0)

    score = 0.0
    if bias["adx"] >= SIG_MIN_ADX:
        score += 0.25
    else:
        return (False, "", f"bias ADX {bias['adx']:.1f} < {SIG_MIN_ADX}", 0.0)

    if f["atrp"] >= SIG_MIN_ATR_PCT:
        score += 0.25
    else:
        return (False, "", f"ATR% {f['atrp']:.2f} < {SIG_MIN_ATR_PCT}", 0.0)

    long_ok = bias["trend_up"] and (f["pullback_ok"] or f["breakout_ok"])
    short_ok = bias["trend_dn"] and (f["trend_dn_ok"] or f["breakdown_ok"])

    reasons = []
    if long_ok:
        reasons.append("bias-up + pullback/breakout")
        score += 0.25
    if short_ok:
        reasons.append("bias-down + continuation/breakdown")
        score += 0.25
    if not (long_ok or short_ok):
        return (False, "", "no edge", 0.0)

    direction = "long" if long_ok and (not short_ok or bias["trend_up"]) else "short"
    vz = f.get("vz", 0.0)
    score += max(0.0, min(0.25, (vz - 0.5) / 4.0))  # tiny boost from volume energy

    conf = max(0.0, min(1.0, score))
    return (True, direction, "; ".join(reasons), conf)

# =========================
# Emit & queue
# =========================

def compute_stop_dist(last: float, f: Dict) -> float:
    if STOP_MODE == "atr_mult" and f.get("atr"):
        return float(f["atr"] * STOP_ATR_MULT)
    if STOP_MODE == "pct":
        return float(last * (STOP_PCT / 100.0))
    return 0.0  # auto → let executor decide

_queue_lock = threading.Lock()
_last_alert = defaultdict(float)      # cooldown per (sym, tf, dir)
_last_bar_emit: Dict[Tuple[str, int, str], float] = {}
_last_hb = 0.0

def _safe_append_jsonl(path: Path, line: str) -> None:
    with _queue_lock:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

def _now_local() -> dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo(TZ_STR))
    except Exception:
        return dt.datetime.now()

def human_alert(symbol: str, tf: int, direction: str, why: str, bias: Dict, f: Dict, conf: float, mode_str: str) -> None:
    trend = "UP" if bias.get("trend_up") else ("DOWN" if bias.get("trend_dn") else "FLAT")
    lines = [
        f"🟢 Signal • {symbol} • {tf}m • {direction.upper()} • conf {conf:.2f}",
        f"Why: {why}",
        f"Bias {BIAS_TF}m: ADX {bias['adx']:.1f} • trend {trend}",
        f"Intra: ADX {f['adx']:.1f} • ATR% {f['atrp']:.2f} • VolZ {f['vz']:.2f}",
        f"Close {f['close']:.6g} • EMA20/50/200 {f['ema20']:.6g}/{f['ema50']:.6g}/{f['ema200']:.6g}",
        mode_str,
    ]
    tg.safe_text("\n".join(lines), parse_mode=None, quiet=True)

def maybe_emit(symbol: str, tf: int, direction: str, why: str, bias: Dict, f: Dict, conf: float) -> None:
    now = time.time()
    key = (symbol, tf, direction)

    # once-per-bar dedupe
    bar_ts = float(f.get("bar_ts") or 0.0)
    last_bar = _last_bar_emit.get(key)
    if last_bar is not None and bar_ts <= last_bar:
        return

    # cooldown
    if now - _last_alert[key] < SIG_CD_SEC:
        return

    # guard again right before emission (breaker could have flipped mid-iteration)
    blocked, why_break = guard_blocking_reason()
    if blocked:
        log.info("guarded (emit): %s %sm %s conf=%.2f • %s", symbol, tf, direction, conf, why_break)
        return

    _last_alert[key] = now
    _last_bar_emit[key] = bar_ts

    last = float(f["close"])
    stop_dist = compute_stop_dist(last, f)

    params = {
        "maker_only": bool(SIG_MAKER_ONLY),
        "spread_max_bps": float(SIG_SPREAD_MAX_BPS),
        "tag": SIG_TAG,
    }
    if stop_dist > 0:
        params["stop_dist"] = float(stop_dist)

    signal_type = "LONG_BREAKOUT" if direction == "long" else "SHORT_BREAKDOWN"

    payload = {
        "ts": int(now * 1000),
        "symbol": symbol.upper(),
        "timeframe": tf,
        "signal": signal_type,
        "why": why,
        "confidence": round(conf, 4),
        "params": params,
        "features": {
            "bias_adx": round(float(bias["adx"]), 4),
            "bias_trend_up": bool(bias["trend_up"]),
            "bias_trend_dn": bool(bias["trend_dn"]),
            "intra_adx": round(float(f["adx"]), 4),
            "intra_atrp": round(float(f["atrp"]), 6),
            "intra_vz": round(float(f["vz"]), 6),
            "ema20": round(float(f["ema20"]), 10),
            "ema50": round(float(f["ema50"]), 10),
            "ema200": round(float(f["ema200"]), 10),
            "close": round(float(f["close"]), 10),
        },
    }

    line = json.dumps(payload, separators=(",", ":"))
    _safe_append_jsonl(QUEUE_PATH, line)

    mode_str = "Mode: OBSERVE (executor consumes queue)"
    human_alert(symbol, tf, direction, why, bias, f, conf, mode_str)

# =========================
# Main loop
# =========================

def loop_once() -> bool:
    # If breaker is ON, don’t waste API calls; keep heartbeating elsewhere.
    blocked, why = guard_blocking_reason()
    if blocked:
        log.info("guarded (scan): %s — skipping scan cycle", why)
        return False

    any_signal = False
    for sym in SYMS:
        try:
            bias = bias_context(sym, BIAS_TF)
            for tf in INTRA_TFS:
                f = intra_features(sym, tf)
                ok, direction, why_dec, conf = decide(sym, tf, bias, f)
                if ok:
                    any_signal = True
                    # guard again at emission granularity
                    with guard_gate(bot="signal_engine", action=f"emit/{sym}/{tf}") as allowed:
                        if not allowed:
                            continue
                        maybe_emit(sym, tf, direction, why_dec, bias, f, conf)
        except Exception as e:
            log.warning("loop %s error: %s", sym, e)
    return any_signal

def main() -> None:
    if not ENABLED:
        tg.safe_text("Signal Engine disabled (SIG_ENABLED=false).", quiet=True)
        log.info("Signal Engine disabled via SIG_ENABLED.")
        return

    tg.safe_text(
        f"🟢 Signal Engine online • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • Queue={QUEUE_PATH.name}",
        quiet=True,
    )
    log.info("startup SYMS=%s TFs=%s Bias=%sm queue=%s", ",".join(SYMS), INTRA_TFS, BIAS_TF, QUEUE_PATH)

    global _last_hb
    _last_hb = 0.0

    while True:
        try:
            any_signal = loop_once()
        except Exception as e:
            log.error("scan error: %s", e)
            any_signal = False

        # heartbeat (always, even if breaker blocks emits/scans)
        now = time.time()
        if HEARTBEAT_MIN > 0 and (now - _last_hb) >= HEARTBEAT_MIN * 60:
            _last_hb = now
            blocked, why = guard_blocking_reason()
            tg.safe_text(
                f"💓 Signal Engine heartbeat • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • queue={QUEUE_PATH.name} • signals={'yes' if any_signal else 'no'} • guard={'ON' if blocked else 'OFF'}{(' • '+why) if blocked else ''}",
                quiet=True,
            )
        time.sleep(max(5, SIG_POLL_SEC))

if __name__ == "__main__":
    main()
