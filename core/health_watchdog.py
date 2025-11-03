# core/health_watchdog.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from core.env_bootstrap import *  # loads config/.env automatically


"""
Health Watchdog (core)

What it does:
- Pings RELAY_BASE /health on a schedule with jitter to avoid thundering herds.
- Retries transient failures with exponential backoff.
- Alerts to Telegram (via notifier_bot.tg_send or base44_client.tg_send) on:
  • downtime, recovery
  • high latency (WARN/CRIT thresholds)
  • bad status or missing expected body substring / JSON key:value
- Tracks EWMA latency for noisy links and rate-limits repetitive alerts.
- Optional CSV logging of each probe.

Env vars (all optional unless noted):
- RELAY_BASE                      : base URL (default http://127.0.0.1:8080)
- HW_POLL_SEC                     : seconds between probes (default 20)
- HW_JITTER_MS                    : random jitter added to each poll (default 250)
- HW_TIMEOUT_SEC                  : HTTP timeout per attempt (default 10)
- HW_RETRIES                      : retry attempts per probe (default 2)
- HW_BACKOFF_BASE_MS              : first backoff step (default 300ms)
- HW_METHOD                       : GET or HEAD (default GET)
- HW_PATH                         : path to hit (default /health)
- HW_HEADERS_JSON                 : JSON object of headers to send (default {})
- HW_EXPECT_STATUS                : expected HTTP status (default 200)
- HW_EXPECT_BODY_SUBSTR           : if set, substring that must appear in body (default "")
- HW_EXPECT_JSON_KEY              : JSON key to check in body (dot.notation allowed)
- HW_EXPECT_JSON_VALUE            : expected string value for that key
- HW_LATENCY_WARN_MS              : warn threshold (default 800)
- HW_LATENCY_CRIT_MS              : critical threshold (default 2000)
- HW_EWMA_ALPHA                   : smoothing for EWMA latency (default 0.25)
- HW_ALERT_COOLDOWN_SEC           : min seconds between same-type alerts (default 120)
- HW_RECOVERY_COOLDOWN_SEC        : min seconds before re-announcing recovery (default 15)
- HW_WARN_CONSEC_FAILS            : # consecutive failures before DOWN alert (default 1)
- HW_STARTUP_GRACE_SEC            : suppress DOWN alert during initial warmup (default 10)
- HW_VERIFY_TLS                   : 1 to verify TLS, 0 to skip (default 1)
- HW_LOG_CSV                      : path to CSV log (e.g., logs/health.csv). If empty, no log.

Telegram:
- Uses core/notifier_bot.tg_send if available, else base44_client.tg_send, else console-only.
"""

import os, time, json, random
import requests
from pathlib import Path
from datetime import datetime, timezone
import importlib
import sys

# ----- import tg_send safely ---------------------------------------------------
def _try_import_tg_send():
    core_hint = os.getenv("BASE44_CORE_DIR", "").strip()
    candidates = []
    if core_hint:
        candidates.append(Path(core_hint))
    here = Path(__file__).resolve() if "__file__" in globals() else Path.cwd()
    core_in_repo = here.parent  # we are in core/, so parent is repo root
    candidates += [core_in_repo / "core", core_in_repo]

    for p in candidates:
        try:
            if p.exists() and str(p) not in sys.path:
                sys.path.insert(0, str(p))
            try:
                nb = importlib.import_module("notifier_bot")
                if hasattr(nb, "tg_send"):
                    return nb.tg_send
            except Exception:
                pass
            try:
                b44 = importlib.import_module("base44_client")
                if hasattr(b44, "tg_send"):
                    return b44.tg_send
            except Exception:
                pass
        except Exception:
            continue

    def _console_only(msg: str, **_):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[{ts}] notifier {msg}")
    return _console_only

tg_send = _try_import_tg_send()

# ----- env/config --------------------------------------------------------------
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

RELAY_BASE            = (os.getenv("RELAY_BASE") or "http://127.0.0.1:8080").rstrip("/")
POLL_SEC              = _env_int("HW_POLL_SEC", 20)
JITTER_MS             = _env_int("HW_JITTER_MS", 250)
TIMEOUT_SEC           = _env_int("HW_TIMEOUT_SEC", 10)
RETRIES               = _env_int("HW_RETRIES", 2)
BACKOFF_BASE_MS       = _env_int("HW_BACKOFF_BASE_MS", 300)
METHOD                = (os.getenv("HW_METHOD") or "GET").upper().strip()
PATH                  = (os.getenv("HW_PATH") or "/health").strip() or "/health"
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

DEFAULT_HEADERS = {
    "User-Agent": "Base44-HealthWatchdog/1.1",
    "Accept": "application/json, text/plain;q=0.8, */*;q=0.5",
}
HEADERS = {**DEFAULT_HEADERS, **CUSTOM_HEADERS}

# ----- logging -----------------------------------------------------------------
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

# ----- helpers -----------------------------------------------------------------
def _json_get(d: dict, dotted_key: str):
    cur = d
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur

# ----- probe -------------------------------------------------------------------
def _probe_once():
    url = f"{RELAY_BASE}{PATH if PATH.startswith('/') else '/'+PATH}"
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
        if ct.startswith("application/json"):
            try:
                json_obj = resp.json()
                body_text = json.dumps(json_obj, ensure_ascii=False)
            except Exception:
                body_text = resp.text
        else:
            body_text = resp.text
    ms = int((time.perf_counter() - t0) * 1000)
    return resp.status_code, ms, body_text, json_obj

def _probe():
    attempt = 0
    while True:
        try:
            return _probe_once()
        except Exception:
            if attempt >= RETRIES:
                raise
            delay = min(4.0, (BACKOFF_BASE_MS/1000.0) * (2 ** attempt))
            time.sleep(delay)
            attempt += 1

# ----- main loop ---------------------------------------------------------------
def main():
    print(f"Health Watchdog → {RELAY_BASE}{PATH} every {POLL_SEC}s ±{JITTER_MS}ms "
          f"(warn>{WARN_MS}ms, crit>{CRIT_MS}ms, timeout={TIMEOUT_SEC}s, retries={RETRIES}, verifyTLS={VERIFY_TLS})")

    boot_ts = time.time()
    consecutive_fail = 0
    is_down = False
    last_alert = {"down": 0.0, "warn": 0.0, "crit": 0.0, "recover": 0.0}
    ewma_ms = None

    while True:
        start_loop = time.time()
        try:
            code, ms, body, j = _probe()
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

            # EWMA
            ewma_ms = ms if ewma_ms is None else (EWMA_ALPHA * ms + (1 - EWMA_ALPHA) * ewma_ms)

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

            if bad_status or bad_body or bad_json:
                consecutive_fail += 1
                note = f"bad_status={code!=EXPECT_STATUS} bad_body={bad_body} bad_json={bad_json}"
                _log_csv(ts, code, ms, False, note)

                # Startup grace window to avoid noise while services boot
                in_grace = (time.time() - boot_ts) < STARTUP_GRACE_SEC

                if not is_down and consecutive_fail >= max(1, WARN_CONSEC_FAILS) and not in_grace:
                    now = time.time()
                    if now - last_alert["down"] >= ALERT_COOLDOWN_SEC:
                        details = []
                        if bad_status: details.append(f"status={code}")
                        if bad_body:   details.append("body-mismatch")
                        if bad_json:   details.append(f"json:{EXPECT_JSON_KEY}≠{EXPECT_JSON_VALUE or 'present'}")
                        tg_send(f"❌ Relay unhealthy: {', '.join(details)}; latency={ms} ms, ewma={int(ewma_ms)} ms")
                        last_alert["down"] = now
                    is_down = True
                print(f"[{ts}] UNHEALTHY code={code} ms={ms} ewma={int(ewma_ms)}")
            else:
                _log_csv(ts, code, ms, True, "")
                if is_down:
                    now = time.time()
                    if now - last_alert["recover"] >= RECOVERY_COOLDOWN_SEC:
                        tg_send(f"✅ Relay recovered: status={code}, latency={ms} ms, ewma={int(ewma_ms)} ms")
                        last_alert["recover"] = now
                    is_down = False
                consecutive_fail = 0

                # latency alerts with cooldowns
                now = time.time()
                if ms >= CRIT_MS and now - last_alert["crit"] >= ALERT_COOLDOWN_SEC:
                    tg_send(f"❌ Relay latency CRITICAL: {ms} ms (>{CRIT_MS}), ewma={int(ewma_ms)} ms", priority="error")
                    last_alert["crit"] = now
                elif ms >= WARN_MS and now - last_alert["warn"] >= ALERT_COOLDOWN_SEC:
                    tg_send(f"⚠️ Relay latency WARN: {ms} ms (>{WARN_MS}), ewma={int(ewma_ms)} ms", priority="warn")
                    last_alert["warn"] = now

                print(f"[{ts}] OK {ms} ms (ewma {int(ewma_ms)} ms)")

        except Exception as e:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
            consecutive_fail += 1
            _log_csv(ts, 0, 0, False, f"exception:{e}")
            now = time.time()
            in_grace = (now - boot_ts) < STARTUP_GRACE_SEC
            if not is_down and consecutive_fail >= max(1, WARN_CONSEC_FAILS) and not in_grace and now - last_alert["down"] >= ALERT_COOLDOWN_SEC:
                tg_send(f"❌ Relay unreachable: {e}")
                last_alert["down"] = now
            is_down = True
            print(f"[{ts}] ERROR {e}")

        # sleep with jitter
        elapsed = time.time() - start_loop
        base_sleep = max(0.0, POLL_SEC - elapsed)
        jitter = random.uniform(0, max(0.0, JITTER_MS/1000.0))
        time.sleep(base_sleep + jitter)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Health Watchdog stopped by user.")
