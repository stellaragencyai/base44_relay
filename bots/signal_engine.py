#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Signal Engine (observe-only, with heartbeat)

- Scans SIG_SYMBOLS on SIG_TIMEFRAMES with higher-timeframe bias (SIG_BIAS_TF).
- Regime gates: min ADX on bias TF, min ATR%/Price on intraday TF, volume z-score.
- Emits "would take" signals to Telegram with feature readouts.
- Rate-limits repeated alerts (symbol, timeframe, direction).
- ALWAYS sends a startup ping and a periodic heartbeat even if no signals.

.env keys:
  SIG_ENABLED=true|false
  SIG_SYMBOLS=BTCUSDT,ETHUSDT
  SIG_TIMEFRAMES=5,15
  SIG_BIAS_TF=60
  SIG_POLL_SEC=30
  SIG_ADX_LEN=14
  SIG_ATR_LEN=14
  SIG_VOL_Z_WIN=60
  SIG_MIN_ADX=18
  SIG_MIN_ATR_PCT=0.25
  SIG_NOTIFY_COOLDOWN_SEC=300
  SIG_SEND_CHART_LINKS=false
  SIG_DRY_RUN=true
  SIG_HEARTBEAT_MIN=10
  TZ=America/Phoenix
"""

from __future__ import annotations
import os, time, statistics, logging, datetime
from collections import deque, defaultdict
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

# notifier
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

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

TZ = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"

BYBIT_KEY = os.getenv("BYBIT_API_KEY","")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET","")
BYBIT_ENV = (os.getenv("BYBIT_ENV","mainnet") or "mainnet").lower().strip()
if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env")
http = HTTP(testnet=(BYBIT_ENV=="testnet"), api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)

# ---------- helpers ----------
def _tf_to_interval(tf_min: int) -> str:
    return "D" if tf_min >= 1440 else str(tf_min)

def _kline(symbol: str, tf_min: int, limit: int=300):
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
            "breakdown_ok": (c[-1] < min(recent)) and (vz[-1] > 0.8)}

# ---------- decision ----------
def decide(symbol: str, tf: int, bias: dict, f: dict):
    if not bias: return (False, "", "insufficient bias")
    if not f:    return (False, "", "insufficient intraday")
    if bias["adx"] < MIN_ADX:      return (False, "", f"bias ADX {bias['adx']:.1f} < {MIN_ADX}")
    if f["atrp"]  < MIN_ATR_PCT:   return (False, "", f"ATR% {f['atrp']:.2f} < {MIN_ATR_PCT}")
    if bias["trend_up"] and (f["pullback_ok"] or f["breakout_ok"]):
        return (True, "long", "bias-up + pullback/breakout")
    if bias["trend_dn"] and (f["trend_dn_ok"] or f["breakdown_ok"]):
        return (True, "short", "bias-down + continuation/breakdown")
    return (False, "", "no edge")

# ---------- alerts ----------
_last_alert = defaultdict(float)

def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(TZ))
    except Exception:
        return datetime.datetime.now()

def maybe_alert(symbol: str, tf: int, direction: str, why: str, bias: dict, f: dict):
    now = time.time()
    key = (symbol, tf, direction)
    if now - _last_alert[key] < COOLDOWN: return
    _last_alert[key] = now
    trend = "UP" if bias.get("trend_up") else ("DOWN" if bias.get("trend_dn") else "FLAT")
    lines = [
        f"🟢 Signal (observe-only) • {symbol} • {tf}m • {direction.upper()}",
        f"Why: {why}",
        f"Bias {BIAS_TF}m: ADX {bias['adx']:.1f} • trend {trend}",
        f"Intra: ADX {f['adx']:.1f} • ATR% {f['atrp']:.2f} • VolZ {f['vz']:.2f}",
        f"Close {f['close']:.6g} • EMA20/50/200 {f['ema20']:.6g}/{f['ema50']:.6g}/{f['ema200']:.6g}",
        "Mode: DRY (no orders)" if DRY_RUN else "Mode: LIVE (executor may enter)"
    ]
    tg_send("\n".join(lines), priority="info")

# ---------- main loop with heartbeat ----------
_last_hb = 0.0

def loop_once():
    global _last_hb
    any_signal = False
    for sym in SYMS:
        bias = bias_context(sym, BIAS_TF)
        for tf in INTRA_TFS:
            f = intra_features(sym, tf)
            ok, direction, why = decide(sym, tf, bias, f)
            if ok:
                any_signal = True
                maybe_alert(sym, tf, direction, why, bias, f)
    # heartbeat
    now = time.time()
    if HEARTBEAT_MIN > 0 and (now - _last_hb) >= HEARTBEAT_MIN*60:
        _last_hb = now
        tg_send(f"🟢 Signal Engine heartbeat • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • Mode={'DRY' if DRY_RUN else 'LIVE'} • signals={'yes' if any_signal else 'no'}",
                priority="success")

def main():
    if not ENABLED:
        tg_send("Signal Engine disabled (SIG_ENABLED=false).", priority="warn")
        return
    tg_send(f"🟢 Signal Engine online • SYMS={','.join(SYMS)} • TFs={INTRA_TFS} • Bias={BIAS_TF}m • Mode={'DRY' if DRY_RUN else 'LIVE'}",
            priority="success")
    while True:
        try:
            loop_once()
        except Exception as e:
            log.warning(f"scan error: {e}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    main()
