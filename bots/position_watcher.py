#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Position & Wallet Watcher (automation-ready, throttled alerts)

What it does
- Connects to Bybit v5 private WS (mainnet/testnet) and subscribes: position, wallet, order.
- Emits compact JSON lines to an append-only stream file for downstream consumers.
- Maintains last-known snapshots in .state/ (positions.json, wallet.json).
- Sends Telegram alerts (throttled) for:
    • liquidation proximity (per position)
    • equity drawdown vs session baseline (wallet)
    • WS reconnects/errors (sane cadence)
- Can flip global breaker file if drawdown exceeds threshold.
- Decision logging hooks to core.decision_log if present.

Env (.env)
  # Bybit
  BYBIT_ENV=mainnet|testnet
  BYBIT_API_KEY=
  BYBIT_API_SECRET=
  BYBIT_RECV_WINDOW=10000

  # Watcher runtime
  LOG_LEVEL=INFO
  WS_RECONNECT_BACKOFF=2,4,8,16,32
  WS_PING_INTERVAL=20
  WS_PING_TIMEOUT=10

  # Streams & state
  STREAM_OUT_DIR=./logs/stream
  STREAM_FILE=private.jsonl
  STATE_DIR=.state

  # Alerts & risk
  LIQ_WARN_BPS=200               # alert if mark within 200 bps of liq
  DD_WARN_PCT=3.0                # warn if totalEquity down this % from session baseline
  DD_BREAKER_PCT=5.0             # if reached, set breaker breach=true
  ALERT_COOLDOWN_SEC=120         # per key
  WD_SET_BREAKER=true
  WD_BREAKER_FILE=.state/risk_state.json

  # Telegram (via core.notifier_bot)
  TELEGRAM_BOT_TOKEN=
  TELEGRAM_CHAT_ID=
"""

from __future__ import annotations
import os, sys, json, time, hmac, hashlib, logging, threading
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from websocket import WebSocketApp

# ----- project root + soft deps
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):
        print(f"[notify/{priority}] {msg}")

try:
    from core.decision_log import log_event
except Exception:
    def log_event(component, event, symbol, account_uid, payload=None, trade_id=None, level="info"):
        print(f"[DECLOG/{component}/{event}] {payload or {}}")

# ----- env & logging
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("pos_watch")

ENV     = (os.getenv("BYBIT_ENV") or "mainnet").strip().lower()
KEY     = (os.getenv("BYBIT_API_KEY") or "").strip()
SECRET  = (os.getenv("BYBIT_API_SECRET") or "").strip()
RECVW   = (os.getenv("BYBIT_RECV_WINDOW") or "10000").strip()
BACKOFF = [int(x) for x in (os.getenv("WS_RECONNECT_BACKOFF") or "2,4,8,16,32").split(",")]

WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT  = int(os.getenv("WS_PING_TIMEOUT", "10"))

STREAM_OUT_DIR = Path(os.getenv("STREAM_OUT_DIR", "./logs/stream"))
STREAM_FILE    = os.getenv("STREAM_FILE", "private.jsonl")
STATE_DIR      = Path(os.getenv("STATE_DIR", ".state"))

LIQ_WARN_BPS       = float(os.getenv("LIQ_WARN_BPS", "200"))  # basis points = 2% default
DD_WARN_PCT        = float(os.getenv("DD_WARN_PCT", "3.0"))
DD_BREAKER_PCT     = float(os.getenv("DD_BREAKER_PCT", "5.0"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "120"))

SET_BREAKER   = (os.getenv("WD_SET_BREAKER", "true").strip().lower() in {"1","true","yes","on"})
BREAKER_FILE  = Path(os.getenv("WD_BREAKER_FILE", ".state/risk_state.json"))

if not KEY or not SECRET:
    raise SystemExit("Missing BYBIT_API_KEY / BYBIT_API_SECRET in .env")

WS_PRIVATE = "wss://stream.bybit.com/v5/private" if ENV == "mainnet" else "wss://stream-testnet.bybit.com/v5/private"

# ----- dirs
STREAM_OUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)

STREAM_PATH = STREAM_OUT_DIR / STREAM_FILE
STATE_POS   = STATE_DIR / "positions.json"
STATE_WAL   = STATE_DIR / "wallet.json"
STATE_BASE  = STATE_DIR / "wallet_baseline.json"

# ----- shared state
_stream_lock = threading.Lock()
_alert_last: Dict[str, float] = {}
_session_baseline_equity: Optional[float] = None

def _now() -> float:
    return time.time()

def _cool_ok(key: str) -> bool:
    t = _alert_last.get(key, 0.0)
    if _now() - t >= ALERT_COOLDOWN_SEC:
        _alert_last[key] = _now()
        return True
    return False

def _bps(a: float, b: float) -> float:
    """basis points of difference rel to average of a and b"""
    if a <= 0 and b <= 0: return 0.0
    try:
        return abs((a-b) / ((a+b)/2.0)) * 10000.0
    except ZeroDivisionError:
        return 0.0

def _write_json(path: Path, obj: Any):
    try:
        path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception as e:
        log.debug(f"write {path.name} failed: {e}")

def _append_stream(obj: Dict[str, Any]):
    line = json.dumps(obj, separators=(",",":"))
    with _stream_lock:
        with open(STREAM_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

def _read_json(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _set_breaker(breach: bool, reason: str):
    if not SET_BREAKER:
        return
    data = _read_json(BREAKER_FILE) or {}
    data.update({"breach": bool(breach), "source": "position_watcher", "reason": reason, "ts": int(_now())})
    _write_json(BREAKER_FILE, data)

# ----- auth & ws handlers
def sign_auth(ts_ms: int) -> str:
    payload = str(ts_ms) + KEY + RECVW
    return hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _on_open(ws: WebSocketApp):
    log.info(f"WS open → {WS_PRIVATE}")
    ts = int(time.time()*1000)
    signature = sign_auth(ts)
    auth = {"op":"auth","args":[KEY, str(ts), signature, RECVW]}
    ws.send(json.dumps(auth))
    log.info("auth sent")
    if _cool_ok("ws_open"):
        tg_send("🟢 position_watcher connected & authenticating", priority="success")
        log_event("watcher", "ws_open", "", "MAIN", {"url": WS_PRIVATE})

def _on_message(ws: WebSocketApp, message: str):
    global _session_baseline_equity
    try:
        data = json.loads(message)
    except Exception:
        log.debug(f"non-json: {message[:120]}")
        return

    # control frames
    if "op" in data or data.get("success") is True:
        if data.get("op") == "auth" and data.get("success"):
            sub = {"op":"subscribe", "args":["position","wallet","order"]}
            ws.send(json.dumps(sub))
            log.info("subscribed: position, wallet, order")
            if _cool_ok("ws_authed"):
                tg_send("🔔 position_watcher authenticated; subscriptions active", priority="info")
                log_event("watcher", "ws_authed", "", "MAIN", {})
        return

    topic = data.get("topic")
    ts = data.get("ts", int(time.time()*1000))

    if topic == "position":
        # store a compact latest snapshot and stream deltas
        pos_rows = []
        for item in data.get("data", []):
            sym   = item.get("symbol")
            side  = item.get("side")
            size  = float(item.get("size") or 0)
            entry = float(item.get("avgPrice") or 0)
            mark  = float(item.get("markPrice") or 0)
            liq   = float(item.get("liqPrice") or 0)
            unp   = float(item.get("unrealisedPnl") or 0)

            out = {"t": ts, "topic":"position", "symbol":sym, "side":side,
                   "size":size, "entry":entry, "mark":mark, "liq":liq, "unPnl":unp}
            _append_stream(out)
            pos_rows.append(out)

            # liquidation proximity alert
            if liq > 0 and mark > 0 and size > 0:
                gap_bps = _bps(mark, liq)
                if gap_bps <= LIQ_WARN_BPS and _cool_ok(f"liq:{sym}"):
                    tg_send(f"⚠️ {sym}: mark {mark:.6g} is {gap_bps:.1f} bps from liq {liq:.6g} (size {size}).", priority="warn")
                    log_event("watcher", "liq_proximity", sym, "MAIN", {"gap_bps": gap_bps, "mark": mark, "liq": liq})

        if pos_rows:
            _write_json(STATE_POS, {"t": ts, "positions": pos_rows})

    elif topic == "wallet":
        # there can be multiple accounts, but for unified we care about totalEquity
        wal_rows = []
        for item in data.get("data", []):
            acct  = item.get("accountType")
            teq   = float(item.get("totalEquity") or 0)
            avail = float(item.get("availableBalance") or 0)
            out = {"t": ts, "topic":"wallet", "accountType":acct,
                   "totalEquity":teq, "availableBalance":avail}
            _append_stream(out)
            wal_rows.append(out)

            # session baseline & drawdown checks (first seen becomes baseline)
            if teq > 0:
                if _session_baseline_equity is None:
                    # try load previous baseline persisted this session
                    persisted = _read_json(STATE_BASE)
                    if persisted and isinstance(persisted.get("totalEquity"), (int, float)):
                        _session_baseline_equity = float(persisted["totalEquity"])
                    else:
                        _session_baseline_equity = teq
                        _write_json(STATE_BASE, {"t": ts, "totalEquity": teq})

                if _session_baseline_equity:
                    dd_pct = 0.0 if _session_baseline_equity == 0 else 100.0 * (max(0.0, _session_baseline_equity - teq) / _session_baseline_equity)
                    if dd_pct >= DD_BREAKER_PCT and _cool_ok("dd_breaker"):
                        tg_send(f"⛔ Equity drawdown {dd_pct:.2f}% ≥ {DD_BREAKER_PCT:.2f}% — breaker asserted.", priority="warn")
                        _set_breaker(True, f"dd {dd_pct:.2f}% >= {DD_BREAKER_PCT:.2f}%")
                        log_event("watcher", "dd_breaker", "", "MAIN", {"dd_pct": dd_pct, "baseline": _session_baseline_equity, "eq": teq})
                    elif dd_pct >= DD_WARN_PCT and _cool_ok("dd_warn"):
                        tg_send(f"⚠️ Equity drawdown {dd_pct:.2f}% (baseline {_session_baseline_equity:.2f} → {teq:.2f}).", priority="warn")
                        log_event("watcher", "dd_warn", "", "MAIN", {"dd_pct": dd_pct, "baseline": _session_baseline_equity, "eq": teq})

        if wal_rows:
            _write_json(STATE_WAL, {"t": ts, "wallet": wal_rows})

    elif topic == "order":
        for item in data.get("data", []):
            out = {
                "t": ts, "topic":"order",
                "symbol": item.get("symbol"),
                "orderId": item.get("orderId"),
                "status": item.get("orderStatus"),
                "side": item.get("side"),
                "price": item.get("price"),
                "qty": item.get("qty"),
                "linkId": item.get("orderLinkId"),
                "reduceOnly": item.get("reduceOnly"),
            }
            _append_stream(out)
            # lightweight fill notice
            st = (item.get("orderStatus") or "").lower()
            if st in {"filled","partially_filled"} and _cool_ok(f"fill:{item.get('symbol')}"):
                tg_send(f"🎯 Order {st}: {item.get('symbol')} {item.get('side')} {item.get('qty')} @ {item.get('price')}", priority="info")
                log_event("watcher", "order_fill", item.get("symbol") or "", "MAIN",
                          {"status": st, "qty": item.get("qty"), "price": item.get("price"), "linkId": item.get("orderLinkId")})
    else:
        # ignore other topics silently
        pass

def _on_error(ws: WebSocketApp, err):
    log.warning(f"WS error: {err}")
    if _cool_ok("ws_err"):
        tg_send(f"⚠️ position_watcher WS error: {err}", priority="warn")
        log_event("watcher", "ws_error", "", "MAIN", {"err": str(err)})

def _on_close(ws: WebSocketApp, code, msg):
    log.warning(f"WS close code={code} msg={msg}")
    if _cool_ok("ws_close"):
        tg_send(f"🔌 position_watcher closed (code={code})", priority="warn")
        log_event("watcher", "ws_close", "", "MAIN", {"code": code, "msg": msg})

def run_forever():
    attempt = 0
    while True:
        try:
            ws = WebSocketApp(
                WS_PRIVATE,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=WS_PING_INTERVAL, ping_timeout=WS_PING_TIMEOUT)
        except Exception as e:
            log.error(f"fatal ws exception: {e}")
            if _cool_ok("ws_fatal"):
                tg_send(f"❌ position_watcher fatal WS exception: {e}", priority="error")
                log_event("watcher", "ws_fatal", "", "MAIN", {"err": str(e)})
        delay = BACKOFF[min(attempt, len(BACKOFF)-1)]
        log.info(f"reconnect in {delay}s")
        time.sleep(delay)
        attempt += 1

if __name__ == "__main__":
    log.info(f"position_watcher starting → {WS_PRIVATE} • stream={STREAM_PATH}")
    tg_send("🟢 position_watcher starting", priority="success")
    run_forever()
