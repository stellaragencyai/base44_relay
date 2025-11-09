# core/health_watchdog.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from core.env_bootstrap import *  # loads config/.env automatically

"""
Health Watchdog (core)

What it does:
- Pings RELAY_BASE /health on a schedule with jitter and backoff.
- Retries transient failures; EWMA latency tracking; Telegram alerts with cooldowns.
- Optionally probes Bybit public endpoint to detect exchange reachability.
- Reads critical bot heartbeats from .state/effective/<bot>.json.
- Pulls session drawdown from portfolio_guard if available.
- Emits consolidated health snapshot to .state/health.json.
- Optionally invokes breaker.auto_tick() to let breaker auto-trip/clear via health.

Existing env (Relay probe):
  RELAY_BASE                      : base URL (default http://127.0.0.1:5000)
  RELAY_TOKEN / HW_RELAY_TOKEN    : bearer/x-relay-token for protected endpoints
  HW_POLL_SEC                     : seconds between probes (default 20)
  HW_JITTER_MS                    : random jitter added to each poll (default 250)
  HW_TIMEOUT_SEC                  : HTTP timeout per attempt (default 10)
  HW_RETRIES                      : retry attempts per probe (default 2)
  HW_BACKOFF_BASE_MS              : first backoff step (default 300ms)
  HW_METHOD                       : GET or HEAD (default GET)
  HW_PATH                         : path to hit (default /health)
  HW_HEADERS_JSON                 : JSON object of headers to send (default {})
  HW_EXPECT_STATUS                : expected HTTP status (default 200)
  HW_EXPECT_BODY_SUBSTR           : substring that must appear in body (optional)
  HW_EXPECT_JSON_KEY              : JSON key to check (dot.notation ok)
  HW_EXPECT_JSON_VALUE            : expected string value
  HW_LATENCY_WARN_MS              : warn threshold (default 800)
  HW_LATENCY_CRIT_MS              : critical threshold (default 2000)
  HW_EWMA_ALPHA                   : smoothing for EWMA latency (default 0.25)
  HW_ALERT_COOLDOWN_SEC           : seconds between same alert (default 120)
  HW_RECOVERY_COOLDOWN_SEC        : seconds before re-announcing recovery (default 15)
  HW_WARN_CONSEC_FAILS            : # consecutive failures before DOWN (default 1)
  HW_STARTUP_GRACE_SEC            : suppress DOWN alerts for first N seconds (default 10)
  HW_VERIFY_TLS                   : 1 verify TLS, 0 skip (default 1)
  HW_LOG_CSV                      : path to CSV log (e.g., logs/health.csv)

New env (system health + breaker):
  PROBE_RELAY                     : 1/0 (default 1)
  PROBE_EXCHANGE                  : 1/0 (default 1)
  BYBIT_BASE_URL                  : default https://api.bybit.com
  HEALTH_INTERVAL_SEC             : main loop cadence for health.json writes (default = HW_POLL_SEC)
  HEALTH_PATH                     : where to write health json (default .state/health.json)
  HEALTH_REQUIRE_BOTS             : CSV of critical bot names (default signal_engine,auto_executor,tp_sl_manager,reconciler)
  HEARTBEAT_STALE_SEC             : consider a bot stale if last > N sec ago (optional)
  TELEGRAM_HEALTH_PING_MIN        : minutes between periodic OK pings (0 disables; default 10)
  TELEGRAM_HEALTH_ALERT_COOLDOWN  : seconds between consolidated health alerts (default 120)
  BREAKER_AUTO_TICK               : 1/0 (default 1) call breaker.auto_tick() each loop
"""

import os, time, json, random, socket, sys, importlib
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# ----- import tg_send safely ---------------------------------------------------
def _try_import_tg_send():
    try:
        from core.notifier_bot import tg_send  # type: ignore
        return tg_send
    except Exception:
        pass
    try:
        from core.base44_client import tg_send  # type: ignore
        return tg_send
    except Exception:
        pass

    def _console_only(msg: str, **_):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{ts}] notifier {msg}")
    return _console_only

tg_send = _try_import_tg_send()

# ----- logging (prefer core.logger) -------------------------------------------
try:
    from core.logger import get_logger, bind_context
    _log = bind_context(get_logger("core.health_watchdog"), comp="health")
    def log_info(*a): _log.info(*a)
    def log_warn(*a): _log.warning(*a)
    def log_err(*a):  _log.error(*a)
except Exception:
    def log_info(*a): print("[INFO]", *a)
    def log_warn(*a): print("[WARN]", *a)
    def log_err(*a):  print("[ERR ]", *a)

# ----- breaker auto ------------------------------------------------------------
AUTO_TICK = (os.getenv("BREAKER_AUTO_TICK","true").strip().lower() in {"1","true","yes","on"})
try:
    from core import breaker
except Exception:
    breaker = None  # graceful if missing

# ----- paths / state -----------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
HEALTH_PATH = Path(os.getenv("HEALTH_PATH") or (STATE_DIR / "health.json"))

# ----- helpers to read env -----------------------------------------------------
def _env_int(key, default):
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default

def _env_float(key, default):
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default

def _env_json_obj(key, default):
    raw = os.getenv(key, "")
    if not raw:
        return default
    try:
        val = json.loads(raw)
        return val if isinstance(val, dict) else default
    except Exception:
        return default

# ----- relay probe config ------------------------------------------------------
RELAY_BASE            = (os.getenv("RELAY_BASE") or "http://127.0.0.1:5000").rstrip("/")
POLL_SEC              = _env_int("HW_POLL_SEC", 20)
JITTER_MS             = _env_int("HW_JITTER_MS", 250)
TIMEOUT_SEC           = _env_int("HW_TIMEOUT_SEC", 10)
RETRIES               = _env_int("HW_RETRIES", 2)
BACKOFF_BASE_MS       = _env_int("HW_BACKOFF_BASE_MS", 300)
METHOD                = (os.getenv("HW_METHOD") or "GET").upper().strip()
PATH_                  = (os.getenv("HW_PATH") or "/health").strip() or "/health"
CUSTOM_HEADERS        = _env_json_obj("HW_HEADERS_JSON", {})
EXPECT_STATUS         = _env_int("HW_EXPECT_STATUS", 200)
EXPECT_BODY_SUBSTR    = os.getenv("HW_EXPECT_BODY_SUBSTR", "")
EXPECT_JSON_KEY       = os.getenv("HW_EXPECT_JSON_KEY", "").strip()
EXPECT_JSON_VALUE     = os.getenv("HW_EXPECT_JSON_VALUE", "").strip()
WARN_MS               = _env_int("HW_LATENCY_WARN_MS", 800)
CRIT_MS               = _env_int("HW_LATENCY_CRIT_MS", 2000)
EWMA_ALPHA            = _env_float("HW_EWMA_ALPHA", 0.25)
ALERT_COOLDOWN_SEC    = _env_int("HW_ALERT_COOLDOWN_SEC", 120)
RECOVERY_COOLDOWN_SEC = _env_int("HW_RECOVERY_COOLDOWN_SEC", 15)
WARN_CONSEC_FAILS     = _env_int("HW_WARN_CONSEC_FAILS", 1)
STARTUP_GRACE_SEC     = _env_int("HW_STARTUP_GRACE_SEC", 10)
VERIFY_TLS            = _env_int("HW_VERIFY_TLS", 1) == 1
LOG_CSV               = os.getenv("HW_LOG_CSV", "").strip()

# Optional auth headers
RELAY_TOKEN           = (os.getenv("HW_RELAY_TOKEN") or os.getenv("RELAY_TOKEN") or os.getenv("RELAY_SECRET") or "").strip()
DEFAULT_HEADERS = {
    "User-Agent": "Base44-HealthWatchdog/1.3",
    "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
}
HEADERS = {**DEFAULT_HEADERS, **CUSTOM_HEADERS}
if RELAY_TOKEN:
    lk = {k.lower() for k in HEADERS}
    if "authorization" not in lk:
        HEADERS["Authorization"] = f"Bearer {RELAY_TOKEN}"
    if "x-relay-token" not in lk:
        HEADERS["x-relay-token"] = RELAY_TOKEN

# ----- system health config ----------------------------------------------------
PROBE_RELAY = os.getenv("PROBE_RELAY","true").strip().lower() in {"1","true","yes","on"}
PROBE_EXCHANGE = os.getenv("PROBE_EXCHANGE","true").strip().lower() in {"1","true","yes","on"}
BYBIT_BASE = (os.getenv("BYBIT_BASE_URL") or "https://api.bybit.com").rstrip("/")
HEALTH_INTERVAL_SEC = _env_int("HEALTH_INTERVAL_SEC", POLL_SEC)
REQUIRED_BOTS = [s.strip() for s in (os.getenv("HEALTH_REQUIRE_BOTS",
                   "signal_engine,auto_executor,tp_sl_manager,reconciler") or "").split(",") if s.strip()]
HEARTBEAT_STALE_SEC = _env_int("HEARTBEAT_STALE_SEC", 0)
TELE_HEALTH_PING_MIN = _env_int("TELEGRAM_HEALTH_PING_MIN", 10)
TELE_ALERT_COOLDOWN = _env_int("TELEGRAM_HEALTH_ALERT_COOLDOWN", 120)

_host = socket.gethostname()
_last_ok_ping = 0.0
_last_alert_ts = 0.0
_last_alert_sig = ""

# ----- CSV log -----------------------------------------------------------------
def _log_csv(ts_iso: str, status: int, ms: int, ok: bool, note: str):
    if not LOG_CSV:
        return
    try:
        p = Path(LOG_CSV)
        p.parent.mkdir(parents=True, exist_ok=True)
        new = not p.exists()
        with p.open("a", encoding="utf-8", newline="") as f:
            if new:
                f.write("timestamp,status,latency_ms,ok,note\n")
            note_s = note.replace(",", " ").replace("\n", " ").strip()
            f.write(f"{ts_iso},{status},{ms},{1 if ok else 0},{note_s}\n")
    except Exception as e:
        print(f"[health_watchdog] csv log error: {e}")

# ----- small helpers -----------------------------------------------------------
def _json_get(d: dict, dotted_key: str):
    cur = d
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur

def _http_probe_once():
    url = f"{RELAY_BASE}{PATH_ if PATH_.startswith('/') else '/'+PATH_}"
    method = METHOD if METHOD in ("GET","HEAD") else "GET"
    t0 = time.perf_counter()
    if method == "HEAD":
        resp = requests.head(url, headers=HEADERS, timeout=TIMEOUT_SEC, verify=VERIFY_TLS)
        body_text = ""
        json_obj = None
        ct = resp.headers.get("content-type","")
    else:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SEC, verify=VERIFY_TLS)
        ct = resp.headers.get("content-type","")
        json_obj = None
        if ct and ct.startswith("application/json"):
            try:
                json_obj = resp.json()
                body_text = json.dumps(json_obj, ensure_ascii=False)
            except Exception:
                body_text = resp.text or ""
        else:
            body_text = resp.text or ""
    ms = int((time.perf_counter() - t0) * 1000)
    return resp.status_code, ms, body_text, json_obj

def _http_probe():
    attempt = 0
    while True:
        try:
            return _http_probe_once()
        except Exception:
            if attempt >= RETRIES:
                raise
            delay = min(4.0, (BACKOFF_BASE_MS/1000.0) * (2 ** attempt))
            time.sleep(delay)
            attempt += 1

def _probe_relay() -> Dict[str, Any]:
    if not PROBE_RELAY:
        return {"ok": True, "latency_ms": 0, "code": 0}
    code, ms, body, j = _http_probe()
    bad_status = code != EXPECT_STATUS
    bad_body = bool(EXPECT_BODY_SUBSTR) and EXPECT_BODY_SUBSTR not in (body or "")
    bad_json = False
    if EXPECT_JSON_KEY:
        value = None
        try:
            if j is None and body:
                j = json.loads(body)
        except Exception:
            j = None
        if isinstance(j, dict):
            value = _json_get(j, EXPECT_JSON_KEY)
        bad_json = (str(value) != EXPECT_JSON_VALUE) if EXPECT_JSON_VALUE else (value is None)
    ok = not (bad_status or bad_body or bad_json)
    return {"ok": ok, "latency_ms": ms, "code": code, "bad_status": bad_status, "bad_body": bad_body, "bad_json": bad_json}

def _probe_exchange() -> bool:
    if not PROBE_EXCHANGE:
        return True
    try:
        url = f"{BYBIT_BASE}/v5/market/instruments-info"
        r = requests.get(url, params={"category":"linear","symbol":"BTCUSDT"}, timeout=6)
        return r.status_code == 200
    except Exception:
        return False

def _read_bot_heartbeats() -> Dict[str, Any]:
    """
    Read heartbeats written by bots into .state/effective/<name>.json
    Expected: {"last": 1731012200, "critical": true}
    """
    out: Dict[str, Any] = {}
    d = STATE_DIR / "effective"
    if d.exists():
        for name in REQUIRED_BOTS:
            p = d / f"{name}.json"
            try:
                if p.exists():
                    js = json.loads(p.read_text(encoding="utf-8"))
                    last = int(js.get("last", 0) or 0)
                    crit = bool(js.get("critical", True))
                    out[name] = {"last": last, "critical": crit}
            except Exception:
                continue
    return out

def _session_drawdown_pct() -> float:
    try:
        from core.portfolio_guard import guard  # type: ignore
        hb = guard.heartbeat()
        return float(hb.get("dd_pct", 0.0) or 0.0)
    except Exception:
        return 0.0

def _save_health(obj: Dict[str, Any]) -> None:
    try:
        HEALTH_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEALTH_PATH.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass

def _sig_from(h: Dict[str, Any]) -> str:
    bits: List[str] = []
    for k in ("relay_unhealthy","exchange_unhealthy","news_active","funding_window"):
        bits.append(f"{k}={int(bool(h.get(k, False)))}")
    now = int(time.time())
    stale = []
    hb_stale = HEARTBEAT_STALE_SEC
    bots = h.get("bots",{}) or {}
    if hb_stale > 0:
        for n, meta in bots.items():
            try:
                if meta.get("critical") and (now - int(meta.get("last", 0))) > hb_stale:
                    stale.append(n)
            except Exception:
                pass
    if stale:
        bits.append("stale=" + ",".join(sorted(stale)))
    # coarse DD bucket
    dd = float(h.get("drawdown_pct", 0.0) or 0.0)
    if dd >= 0.01:
        bits.append(f"dd={dd:.2f}%")
    return "|".join(bits)

def _notify_ok_maybe():
    global _last_ok_ping
    if TELE_HEALTH_PING_MIN <= 0:
        return
    now = time.time()
    if now - _last_ok_ping >= TELE_HEALTH_PING_MIN * 60:
        tg_send(f"ℹ️ Health OK • host={_host}", priority="info")
        _last_ok_ping = now

def _notify_alert_maybe(h: Dict[str, Any]):
    global _last_alert_ts, _last_alert_sig
    sig = _sig_from(h)
    now = time.time()
    if not sig:
        return
    if sig != _last_alert_sig or (now - _last_alert_ts) >= TELE_ALERT_COOLDOWN:
        _last_alert_sig = sig
        _last_alert_ts = now
        tg_send(f"⚠️ Health alert • {sig}", priority="warn")

# ----- main loop ---------------------------------------------------------------
def main():
    print(f"Health Watchdog → {RELAY_BASE}{PATH_} every {POLL_SEC}s ±{JITTER_MS}ms "
          f"(warn>{WARN_MS}ms, crit>{CRIT_MS}ms, timeout={TIMEOUT_SEC}s, retries={RETRIES}, verifyTLS={VERIFY_TLS}, auth={'on' if RELAY_TOKEN else 'off'})")

    boot_ts = time.time()
    consecutive_fail = 0
    relay_down = False
    last_alert = {"down": 0.0, "warn": 0.0, "crit": 0.0, "recover": 0.0}
    ewma_ms: Optional[float] = None

    # Co-schedule health.json write cadence
    next_health_write = 0.0

    while True:
        start_loop = time.time()
        try:
            # Relay probe
            relay_ok = True
            code = 0
            ms = 0
            if PROBE_RELAY:
                r = _probe_relay()
                relay_ok = bool(r["ok"])
                code = int(r["code"])
                ms = int(r["latency_ms"])
                # EWMA latency
                ewma_ms = ms if ewma_ms is None else (EWMA_ALPHA * ms + (1 - EWMA_ALPHA) * ewma_ms)

                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
                if not relay_ok:
                    consecutive_fail += 1
                    note = f"bad_status={r['bad_status']} bad_body={r['bad_body']} bad_json={r['bad_json']}"
                    _log_csv(ts, code, ms, False, note)

                    in_grace = (time.time() - boot_ts) < STARTUP_GRACE_SEC
                    if not relay_down and consecutive_fail >= max(1, WARN_CONSEC_FAILS) and not in_grace:
                        now = time.time()
                        if now - last_alert["down"] >= ALERT_COOLDOWN_SEC:
                            details = []
                            if r["bad_status"]: details.append(f"status={code}")
                            if r["bad_body"]:   details.append("body-mismatch")
                            if r["bad_json"]:   details.append("json-mismatch")
                            tg_send(f"❌ Relay unhealthy: {', '.join(details)}; latency={ms} ms, ewma={int(ewma_ms or 0)} ms")
                            last_alert["down"] = now
                        relay_down = True
                    print(f"[{ts}] UNHEALTHY code={code} ms={ms} ewma={int(ewma_ms or 0)}")
                else:
                    _log_csv(ts, code, ms, True, "")
                    if relay_down:
                        now = time.time()
                        if now - last_alert["recover"] >= RECOVERY_COOLDOWN_SEC:
                            tg_send(f"✅ Relay recovered: status={code}, latency={ms} ms, ewma={int(ewma_ms or 0)} ms")
                            last_alert["recover"] = now
                        relay_down = False
                    consecutive_fail = 0

                    # latency alerts with cooldowns
                    now = time.time()
                    if ms >= CRIT_MS and now - last_alert["crit"] >= ALERT_COOLDOWN_SEC:
                        tg_send(f"❌ Relay latency CRITICAL: {ms} ms (>{CRIT_MS}), ewma={int(ewma_ms or 0)} ms", priority="error")
                        last_alert["crit"] = now
                    elif ms >= WARN_MS and now - last_alert["warn"] >= ALERT_COOLDOWN_SEC:
                        tg_send(f"⚠️ Relay latency WARN: {ms} ms (>{WARN_MS}), ewma={int(ewma_ms or 0)} ms", priority="warn")
                        last_alert["warn"] = now

                    print(f"[{ts}] OK {ms} ms (ewma {int(ewma_ms or 0)} ms)")

            # Exchange probe
            exch_ok = _probe_exchange()

            # Health snapshot write (rate by HEALTH_INTERVAL_SEC)
            if time.time() >= next_health_write:
                bots = _read_bot_heartbeats()
                dd = _session_drawdown_pct()
                prev = {}
                try:
                    if HEALTH_PATH.exists():
                        prev = json.loads(HEALTH_PATH.read_text(encoding="utf-8"))
                except Exception:
                    prev = {}

                news = bool(prev.get("news_active", False))
                funding = bool(prev.get("funding_window", False))

                health = {
                    "ts": int(time.time()),
                    "relay_unhealthy": (not relay_ok) if PROBE_RELAY else False,
                    "exchange_unhealthy": (not exch_ok) if PROBE_EXCHANGE else False,
                    "news_active": news,
                    "funding_window": funding,
                    "drawdown_pct": float(dd),
                    "bots": bots
                }
                _save_health(health)

                # Notifications
                if health["relay_unhealthy"] or health["exchange_unhealthy"]:
                    _notify_alert_maybe(health)
                else:
                    _notify_ok_maybe()

                # Let breaker auto-decide if enabled
                if AUTO_TICK and breaker is not None:
                    try:
                        breaker.auto_tick()
                    except Exception as e:
                        log_warn("breaker.auto_tick error: %s", e)

                next_health_write = time.time() + max(2, HEALTH_INTERVAL_SEC)

        except Exception as e:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            consecutive_fail += 1
            _log_csv(ts, 0, 0, False, f"exception:{e}")
            now = time.time()
            in_grace = (now - boot_ts) < STARTUP_GRACE_SEC
            if PROBE_RELAY and (not relay_down) and consecutive_fail >= max(1, WARN_CONSEC_FAILS) and not in_grace and now - last_alert["down"] >= ALERT_COOLDOWN_SEC:
                tg_send(f"❌ Relay unreachable: {e}")
                last_alert["down"] = now
            relay_down = True
            print(f"[{ts}] ERROR {e}")

        # sleep with jitter anchored to relay poll cadence
        elapsed = time.time() - start_loop
        base_sleep = max(0.0, POLL_SEC - elapsed)
        jitter = random.uniform(0, max(0.0, JITTER_MS/1000.0))
        time.sleep(base_sleep + jitter)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Health Watchdog stopped by user.")
