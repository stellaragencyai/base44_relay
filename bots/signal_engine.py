#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Signal Engine (observe-first, automation-ready + heartbeat)

What it does:
- Scans SIG_SYMBOLS on SIG_TIMEFRAMES with a higher-timeframe bias (SIG_BIAS_TF).
- Regime gates: min ADX on bias TF, min ATR%/Price on intraday TF, volume z-score.
- Emits human-readable alerts to Telegram (optional).
- Writes machine-readable signals to signals/observed.jsonl for the executor.
- Includes confidence score and optional stop distance hint for the executor.
- Rate-limits per (symbol, timeframe, direction).
- Sends startup ping and periodic heartbeats.

.env keys (ensure they exist; many you already have):
  # Core
  SIG_ENABLED=true
  SIG_DRY_RUN=true
  SIG_HEARTBEAT_MIN=10
  TZ=America/Phoenix

  # Universe & cadence
  SIG_SYMBOLS=BTCUSDT,ETHUSDT
  SIG_TIMEFRAMES=5,15
  SIG_BIAS_TF=60
  SIG_POLL_SEC=30

  # Feature params
  SIG_ADX_LEN=14
  SIG_ATR_LEN=14
  SIG_VOL_Z_WIN=60
  SIG_MIN_ADX=18
  SIG_MIN_ATR_PCT=0.25
  SIG_NOTIFY_COOLDOWN_SEC=300
  SIG_SEND_CHART_LINKS=false

  # Emit options (for executor)
  SIG_TAG=B44
  SIG_MAKER_ONLY=true
  SIG_SPREAD_MAX_BPS=8
  SIG_STOP_DIST_MODE=auto     # auto | atr_mult | pct
  SIG_STOP_ATR_MULT=3.0
  SIG_STOP_PCT=1.2

  # Output
  SIGNAL_OUT_DIR=signals
  SIGNAL_QUEUE_FILE=observed.jsonl

  # Bybit
  BYBIT_API_KEY=
  BYBIT_API_SECRET=
  BYBIT_ENV=mainnet

  # Decision logging (optional helper)
  DECISION_LOG_DIR=./logs/decisions
  DECISION_LOG_FORMATS=jsonl,parquet
  DECISION_LOG_FLUSH_SEC=2
  DECISION_LOG_ROTATE_DAYS=7
"""

from __future__ import annotations
import os, time, statistics, logging, datetime, threading, json
from collections import deque, defaultdict
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

# Telegram notifier (soft dep)
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

# Decision logger (soft dep)
try:
    from core.decision_log import log_event
except Exception:
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        print(f"[DECLOG/{component}/{event}] {symbol} @{account_uid} {payload or {}}")


log = logging.getLogger("signal")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------- env/root ----------
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

def env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def env_int(k: str, default: int) -> int:
    try: return int((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def env_float(k: str, default: float) -> float:
    try: return float((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def env_csv(k: str, default: str="") -> List[str]:
    raw = os.getenv(k, default) or default
    return [s.strip().upper() for s in raw.split(",") if s.strip()]

# ---------- config ----------
ENABLED = env_bool("SIG_ENABLED", True)
SYMS = env_csv("SIG_SYMBOLS", "BTCUSDT,ETHUSDT")
INTRA_TFS = [int(x) for x in (os.getenv("SIG_TIMEFRAMES","5,15").split(",")) if x.strip()]
BIAS_TF = env_int("SIG_BIAS_TF", 60)
POLL_SEC = env_int("SIG_POLL_SEC", 30)

ADX_LEN = env_int("SIG_ADX_LEN", 14)
ATR_LEN = env_int("SIG_ATR_LEN", 14)
VOL_Z_WIN = env_int("SIG_VOL_Z_WIN", 60)

MIN_ADX = env_float("SIG_MIN_ADX", 18.0)
MIN_ATR_PCT = env_float("SIG_MIN_ATR_PCT", 0.25)
COOLDOWN = env_int("SIG_NOTIFY_COOLDOWN_SEC", 300)
SEND_CHART_LINKS = env_bool("SIG_SEND_CHART_LINKS", False)
DRY_RUN = env_bool("SIG_DRY_RUN", True)
HEARTBEAT_MIN = env_int("SIG_HEARTBEAT_MIN", 10)

SIG_TAG = (os.getenv("SIG_TAG","B44") or "B44").strip()
SIG_MAKER_ONLY = env_bool("SIG_MAKER_ONLY", True)
SIG_SPREAD_MAX_BPS = env_float("SIG_SPREAD_MAX_BPS", 8.0)

STOP_MODE = (os.getenv("SIG_STOP_DIST_MODE","auto") or "auto").strip().lower()   # auto|atr_mult|pct
STOP_ATR_MULT = env_float("SIG_STOP_ATR_MULT", 3.0)
STOP_PCT = env_float("SIG_STOP_PCT", 1.2)

SIGNAL_OUT_DIR = Path(os.getenv("SIGNAL_OUT_DIR", "signals"))
SIGNAL_QUEUE_FILE = os.getenv("SIGNAL_QUEUE_FILE", "observed.jsonl")
SIGNAL_OUT_DIR.mkdir(parents=True, exist_ok=True)
QUEUE_PATH = SIGNAL_OUT_DIR / SIGNAL_QUEUE_FILE

TZ = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"

BYBIT_KEY = os.getenv("BYBIT_API_KEY","")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET","")
BYBIT_ENV = (os.getenv("BYBIT_ENV","mainnet") or "mainnet").lower().strip()
if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env")
http = HTTP(testnet=(BYBIT_ENV=="testnet"), api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)

# simple append lock for JSONL queue
_queue_lock = threading.Lock()

# ---------- helpers ----------
def _tf_to_interval(tf_min: int) -> str:
    return "D" if tf_min >= 1440 else str(tf_min)

def _kline(symbol: str, tf_min: int, limit: int=300):
    """
    Return list of (ts, open, high, low, close, volume) oldest->newest.
    Bybit v5 returns newest-first; we reverse it.
    """
    try:
        res = http.get_kline(category="linear", symbol=symbol, interval=_tf_to_interval(tf_min), limit=limit)
        arr = (res.get("result") or {}).get("list") or []
        out = [(float(x[0])/1000.0, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])) for x in arr]
        out.reverse()
        return out
    except Exception as e:
        log.warning(f"kline error {symbol} {tf_min}m: {e}")
        return []

def ema(values: List[float], length: int) -> List[float]:
    if not values or length <= 1: return values[:]
    k = 2/(length+1)
    out, val = [], None
    for v in values:
        val = v if val is None else (v*k + val*(1-k))
        out.append(val)
    return out

def _true_ranges(h, l, c):
    out, prev = [], None
    for i in range(len(c)):
        if prev is None:
            out.append(h[i]-l[i])
        else:
            out.append(max(h[i]-l[i], abs(h[i]-prev), abs(l[i]-prev)))
        prev = c[i]
    return out

def sma(values: List[float], n: int) -> List[float]:
    out, run = [], deque([], maxlen=n)
    for v in values:
        run.append(v); out.append(sum(run)/len(run))
    return out

def atr(h, l, c, n): return sma(_true_ranges(h,l,c), n)

def adx(h, l, c, n):
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, len(c)):
        up = h[i]-h[i-1]; dn = l[i-1]-l[i]
        plus_dm.append(max(up,0.0) if up > dn else 0.0)
        minus_dm.append(max(dn,0.0) if dn > up else 0.0)
    tr_n = sma(_true_ranges(h,l,c), n)
    pdi, mdi = [0.0]*len(c), [0.0]*len(c)
    for i in range(len(c)):
        if tr_n[i] > 0:
            pdi[i] = 100.0*(plus_dm[i]/tr_n[i])
            mdi[i] = 100.0*(minus_dm[i]/tr_n[i])
    dx = [0.0]*len(c)
    for i in range(len(c)):
        s = pdi[i]+mdi[i]
        dx[i] = 100.0*abs(pdi[i]-mdi[i])/s if s>0 else 0.0
    return sma(dx, n)

def vol_zscore(vol: List[float], win: int) -> List[float]:
    out, run = [], deque([], maxlen=win)
    for v in vol:
        run.append(v)
        if len(run) < 5: out.append(0.0); continue
        mu = statistics.mean(run); sd = statistics.pstdev(run) or 1e-9
        out.append((v-mu)/sd)
    return out

def atr_pct(atr_vals: List[float], closes: List[float]) -> List[float]:
    return [0.0 if closes[i]==0 else 100.0*atr_vals[i]/closes[i] for i in range(len(closes))]

# ---------- features ----------
def bias_context(symbol: str, tf: int):
    k = _kline(symbol, tf, 200)
    if len(k) < max(60, ADX_LEN+5): return {}
    ts,o,h,l,c,v = list(zip(*k))
    c,h,l = list(c), list(h), list(l)
    a = adx(h,l,c, ADX_LEN)
    e50 = ema(c, 50)
    return {"adx": a[-1], "ema50": e50[-1], "close": c[-1],
            "trend_up": c[-1] > e50[-1], "trend_dn": c[-1] < e50[-1]}

def intra_features(symbol: str, tf: int):
    k = _kline(symbol, tf, 400)
    if len(k) < max(ATR_LEN, ADX_LEN, VOL_Z_WIN)+10: return {}
    ts,o,h,l,c,v = list(zip(*k))
    h,l,c,v = list(h), list(l), list(c), list(v)
    a = adx(h,l,c, ADX_LEN)
    av = atr(h,l,c, ATR_LEN)
    ap = atr_pct(av, c)
    vz = vol_zscore(v, VOL_Z_WIN)
    e20, e50, e200 = ema(c,20), ema(c,50), ema(c,200)
    recent = c[-3:]
    return {"adx": a[-1], "atrp": ap[-1], "vz": vz[-1], "close": c[-1],
            "ema20": e20[-1], "ema50": e50[-1], "ema200": e200[-1],
            "pullback_ok": (e20[-1] > e50[-1] > e200[-1]) and (c[-1] >= e50[-1]),
            "breakout_ok": (c[-1] > max(recent)) and (vz[-1] > 0.8),
            "trend_dn_ok": (e20[-1] < e50[-1] < e200[-1]) and (c[-1] <= e50[-1]),
            "breakdown_ok": (c[-1] < min(recent)) and (vz[-1] > 0.8),
            "atr": av[-1]}

# ---------- decision ----------
def decide(symbol: str, tf: int, bias: dict, f: dict):
    """
    Returns (ok, direction, why, confidence[0..1]).
    """
    if not bias: return (False, "", "insufficient bias", 0.0)
    if not f:    return (False, "", "insufficient intraday", 0.0)

    score = 0.0
    if bias["adx"] >= MIN_ADX: score += 0.25
    else: return (False, "", f"bias ADX {bias['adx']:.1f} < {MIN_ADX}", 0.0)

    if f["atrp"]  >= MIN_ATR_PCT: score += 0.25
    else: return (False, "", f"ATR% {f['atrp']:.2f} < {MIN_ATR_PCT}", 0.0)

    long_ok  = bias["trend_up"] and (f["pullback_ok"] or f["breakout_ok"])
    short_ok = bias["trend_dn"] and (f["trend_dn_ok"] or f["breakdown_ok"])

    reasons = []
    if long_ok:  reasons.append("bias-up + pullback/breakout"); score += 0.25
    if short_ok: reasons.append("bias-down + continuation/breakdown"); score += 0.25
    if not (long_ok or short_ok): return (False, "", "no edge", 0.0)

    direction = "long" if long_ok and (not short_ok or bias["trend_up"]) else "short"
    vz = f.get("vz", 0.0)
    score += max(0.0, min(0.25, (vz - 0.5) / 4.0))   # mild boost from volume energy

    conf = max(0.0, min(1.0, score))
    return (True, direction, "; ".join(reasons), conf)

# ---------- helpers for executor params ----------
def compute_stop_dist(last: float, f: dict) -> float:
    if STOP_MODE == "atr_mult" and f.get("atr"):
        return float(f["atr"] * STOP_ATR_MULT)
    if STOP_MODE == "pct":
        return float(last * (STOP_PCT / 100.0))
    return 0.0  # auto → let executor use its internal fallback

# ---------- alerts & queue ----------
_last_alert = defaultdict(float)
_last_hb = 0.0

def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(TZ))
    except Exception:
        return datetime.datetime.now()

def human_alert(symbol: str, tf: int, direction: str, why: str, bias: dict, f: dict, conf: float, mode_str: str):
    trend = "UP" if bias.get("trend_up") else ("DOWN" if bias.get("trend_dn") else "FLAT")
    lines = [
        f"🟢 Signal • {symbol} • {tf}m • {direction.upper()} • conf {conf:.2f}",
        f"Why: {why}",
        f"Bias {BIAS_TF}m: ADX {bias['adx']:.1f} • trend {trend}",
        f"Intra: ADX {f['adx']:.1f} • ATR% {f['atrp']:.2f} • VolZ {f['vz']:.2f}",
        f"Close {f['close']:.6g} • EMA20/50/200 {f['ema20']:.6g}/{f['ema50']:.6g}/{f['ema200']:.6g}",
        mode_str
    ]
    if SEND_CHART_LINKS:
        pass  # add TV links if you really want to tempt fate
    tg_send("\n".join(lines), priority="info")

def maybe_emit(symbol: str, tf: int, direction: str, why: str, bias: dict, f: dict, conf: float):
    now = time.time()
    key = (symbol, tf, direction)
    if now - _last_alert[key] < COOLDOWN:
        return
    _last_alert[key] = now

    last = float(f["close"])
    stop_dist = compute_stop_dist(last, f)

    params = {
        "maker_only": bool(SIG_MAKER_ONLY),
        "spread_max_bps": float(SIG_SPREAD_MAX_BPS),
        "tag": SIG_TAG
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
        }
    }

    line = json.dumps(payload, separators=(",", ":"))
    with _queue_lock:
        with open(QUEUE_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    log_event("signal", "emit", symbol.upper(), "MAIN",
              {"tf": tf, "dir": direction, "conf": round(conf,4), "stop_mode": STOP_MODE})

    mode_str = "Mode: OBSERVE (executor consumes queue)"
    human_alert(symbol, tf, direction, why, bias, f, conf, mode_str)

# ---------- main loop ----------
def loop_once() -> bool:
    any_signal = False
    for sym in SYMS:
        try:
            bias = bias_context(sym, BIAS_TF)
            for tf in INTRA_TFS:
                f = intra_features(sym, tf)
                ok, direction, why, conf = decide(sym, tf, bias, f)
                if ok:
                    any_signal = True
                    maybe_emit(sym, tf, direction, why, bias, f, conf)
        except Exception as e:
            log.warning(f"loop {sym} error: {e}")
            log_event("signal", "loop_error", sym, "MAIN", {"error": str(e)})
    return any_signal

def main():
    if not ENABLED:
        tg_send("Signal Engine disabled (SIG_ENABLED=false).", priority="warn")
        return

    tg_send(
        f"🟢 Signal Engine online • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • Queue={QUEUE_PATH.name}",
        priority="success"
    )
    log_event("signal", "startup", "", "MAIN",
              {"syms": SYMS, "tfs": INTRA_TFS, "bias_tf": BIAS_TF, "queue": str(QUEUE_PATH)})

    global _last_hb
    _last_hb = 0.0

    while True:
        try:
            any_signal = loop_once()
        except Exception as e:
            log.warning(f"scan error: {e}")
            log_event("signal", "scan_error", "", "MAIN", {"error": str(e)})
            any_signal = False

        # heartbeat
        now = time.time()
        if HEARTBEAT_MIN > 0 and (now - _last_hb) >= HEARTBEAT_MIN * 60:
            _last_hb = now
            tg_send(
                f"🟢 Signal Engine heartbeat • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • queue={QUEUE_PATH.name} • signals={'yes' if any_signal else 'no'}",
                priority="success"
            )

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
