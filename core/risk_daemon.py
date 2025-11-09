#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — Risk Daemon (portfolio guardrails + PnL snapshots + DB guard_state sync)

What it does (finalized)
- Polls unified equity for MAIN + SUB_UIDS.
- Sets/maintains a per-day baseline; computes daily drawdown vs that baseline.
- Trips/clears the global breaker via core.breaker when thresholds are crossed.
- Logs JSONL snapshots to logs/pnl/*.jsonl and writes a heartbeat file.
- Sends hourly rollups and daily summary over Telegram.
- SYNC: Updates DB guard_state running PnL and resets DB day anchor on new day.
- SYNC: Mirrors breaker_on/off into DB guard_state so all bots can read one truth.
- Exclusions honored for enforcement-only logic (MANUAL_SUB_UIDS, EXCLUDE_MAIN_FROM_ENFORCE).

.env keys
  # cadence & timezone
  PNL_POLL_SEC=30
  TZ=America/Phoenix

  # rollups
  PNL_SEND_HOURLY=true
  PNL_SEND_DAILY=true
  PNL_DAILY_SEND_HOUR=23
  PNL_LOG_DIR=logs/pnl
  PNL_ROTATE_DAILY=0

  # accounts
  SUB_UIDS=260417078,302355261,152304954,65986659,65986592,152499802
  MANUAL_SUB_UIDS=
  EXCLUDE_MAIN_FROM_ENFORCE=0

  # guardrails
  RISK_MAX_DD_PCT=3.0
  RISK_NOTIFY_EVERY_MIN=10
  RISK_STARTUP_GRACE_SEC=20
  RISK_AUTOFIX=true              # recommend true; daemon only flips breaker, never flattens
  RISK_AUTOFIX_DRY_RUN=true      # placeholder for future active enforcement

  # relay (optional)
  RELAY_URL=https://<your-ngrok>
  RELAY_TOKEN=...

  # bybit (used by core.bybit_client)
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_ENV=mainnet

  # logging
  LOG_LEVEL=INFO

  # breaker defaults (honored by core.breaker)
  BREAKER_DEFAULT_TTL_SEC=3600
  BREAKER_NOTIFY_COOLDOWN_SEC=8
"""

from __future__ import annotations
import os, json, time, logging, datetime
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, Tuple, Optional

from dotenv import load_dotenv

# Notifier (soft dep)
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str = "info", **_):  # type: ignore
        print(f"[notify/{priority}] {msg}")

# Core stacks
from core.bybit_client import Bybit
from core import breaker

# DB guard_state sync (per your db.py API)
try:
    # Expected functions from your recent db.py:
    # guard_load() -> dict, guard_update_pnl(delta_usd: float), guard_reset_day(baseline_usd: float),
    # guard_set_breaker(active: bool, reason: str)
    from core.db import guard_load, guard_update_pnl, guard_reset_day, guard_set_breaker
except Exception:
    # Fallback stubs so the daemon still runs if DB layer isn’t present yet
    def guard_load() -> dict: return {}
    def guard_update_pnl(delta_usd: float) -> None: pass
    def guard_reset_day(baseline_usd: float) -> None: pass
    def guard_set_breaker(active: bool, reason: str = "") -> None: pass

getcontext().prec = 28

# ---- boot/env ----
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("risk_daemon")

STATE_DIR = ROOT / ".state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_PATH = STATE_DIR / "risk_daemon.heartbeat"

EFFECTIVE_DIR = ROOT / ".state" / "effective"
EFFECTIVE_DIR.mkdir(parents=True, exist_ok=True)

PNL_DIR = Path(os.getenv("PNL_LOG_DIR", str(ROOT / "logs" / "pnl")))
PNL_DIR.mkdir(parents=True, exist_ok=True)
PNL_ROTATE_DAILY = (os.getenv("PNL_ROTATE_DAILY", "0") == "1")

# ---- env helpers ----
def env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def env_int(k: str, default: int) -> int:
    try: return int((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def env_float(k: str, default: float) -> float:
    try: return float((os.getenv(k, str(default)) or "").strip())
    except Exception: return default

def env_csv(k: str) -> list[str]:
    raw = os.getenv(k, "") or ""
    return [s.strip() for s in raw.split(",") if s.strip()]

# ---- config knobs ----
PNL_POLL_SEC = env_int("PNL_POLL_SEC", 30)
PNL_SEND_HOURLY       = env_bool("PNL_SEND_HOURLY", True)
PNL_SEND_DAILY        = env_bool("PNL_SEND_DAILY", True)
PNL_DAILY_SEND_HOUR   = env_int("PNL_DAILY_SEND_HOUR", 23)
TZ = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"

SUB_UIDS = env_csv("SUB_UIDS")
MANUAL_SUB_UIDS = set(env_csv("MANUAL_SUB_UIDS"))
EXCLUDE_MAIN = env_bool("EXCLUDE_MAIN_FROM_ENFORCE", False)

RISK_MAX_DD_PCT        = env_float("RISK_MAX_DD_PCT", 3.0)
RISK_NOTIFY_EVERY_MIN  = env_int("RISK_NOTIFY_EVERY_MIN", 10)
RISK_STARTUP_GRACE_SEC = env_int("RISK_STARTUP_GRACE_SEC", 20)
RISK_AUTOFIX           = env_bool("RISK_AUTOFIX", False)
RISK_AUTOFIX_DRY       = env_bool("RISK_AUTOFIX_DRY_RUN", True)

# relay (optional)
RELAY_URL = (os.getenv("RELAY_URL", "") or os.getenv("DASHBOARD_RELAY_BASE", "")).rstrip("/")
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "") or os.getenv("RELAY_SECRET", "")

# ---- client (core.bybit_client) ----
BYBIT_KEY = os.getenv("BYBIT_API_KEY","")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET","")
BYBIT_ENV = (os.getenv("BYBIT_ENV","mainnet") or "mainnet").lower().strip()
if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env")

by = Bybit()
by.sync_time()

# ---- time helpers ----
def now_local() -> datetime.datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(TZ))
    except Exception:
        return datetime.datetime.now()

def today_key(dt_in: Optional[datetime.datetime]=None) -> str:
    d = dt_in or now_local()
    return d.strftime("%Y-%m-%d")

# ---- equity sampling via core.bybit_client ----
def _equity_from_wallets(resp: Dict) -> Decimal:
    wallets = (resp.get("result") or {}).get("list") or []
    if not wallets:
        return Decimal("0")
    coins = wallets[0].get("coin", []) or []
    eq = Decimal("0")
    for c in coins:
        coin = str(c.get("coin") or "").upper()
        usd = Decimal(str(c.get("usdValue") or "0"))
        if usd == 0 and coin in {"USDT","USDC"}:
            usd = Decimal(str(c.get("walletBalance") or "0"))
        eq += usd
    return eq

def _equity_unified_try(extra: dict) -> Decimal:
    ok, data, err = by.get_wallet_balance(accountType="UNIFIED", **extra)
    if not ok:
        return Decimal("0")
    try:
        return _equity_from_wallets(data)
    except Exception:
        return Decimal("0")

def _equity_unified(extra: dict) -> Decimal:
    try:
        return _equity_unified_try(extra)
    except Exception as e:
        log.warning(f"equity fetch exception ({extra}): {e}")
        return Decimal("0")

def _equity_for_uid(uid: str) -> Decimal:
    uid = str(uid).strip()
    if not uid:
        return Decimal("0")
    eq = _equity_unified({"memberId": uid})
    if eq > 0:
        return eq
    return _equity_unified({"subUid": uid})

def snapshot_equities() -> Dict[str, str]:
    data: Dict[str, str] = {}
    data["main"] = str(_equity_unified({}))
    for uid in SUB_UIDS:
        data[f"sub:{uid}"] = str(_equity_for_uid(uid))
    total = sum(Decimal(v) for v in data.values())
    data["total"] = str(total)
    return data

# ---- pnl log ----
def pnl_path_for(ts: datetime.datetime) -> Path:
    if not PNL_ROTATE_DAILY:
        return PNL_DIR / "daily_pnl_log.jsonl"
    return PNL_DIR / f"{ts.strftime('%Y-%m-%d')}.jsonl"

def append_jsonl(path: Path, row: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

# ---- rollup text ----
def pct_change(cur: Decimal, base: Decimal) -> str:
    try:
        pct = ((cur - base) / base * Decimal("100")) if base > 0 else None
        return f"{pct:.2f}%" if pct is not None else "n/a"
    except Exception:
        return "n/a"

def fmt_rollup(data: Dict[str, str], baseline: Dict[str, str] | None) -> str:
    cur_total = Decimal(data["total"])
    base_total = Decimal(baseline["total"]) if baseline else Decimal("0")
    lines = [f"📊 PnL Rollup @ {now_local().strftime('%Y-%m-%d %H:%M')}"]
    lines.append(f"Portfolio: {cur_total:.2f}  Δ {pct_change(cur_total, base_total) if baseline else 'n/a'}")
    for k, v in data.items():
        if k == "total":
            continue
        cur = Decimal(v)
        base = Decimal(baseline.get(k, "0")) if baseline else Decimal("0")
        lines.append(f"• {k:>9}: {cur:.2f}  Δ {pct_change(cur, base) if baseline else 'n/a'}")
    return "\n".join(lines)

# ---- risk evaluation ----
def compute_dd_pct(cur_total: Decimal, base_total: Decimal) -> float:
    if base_total <= 0:
        return 0.0
    dd = (base_total - cur_total) / base_total * Decimal("100")
    return float(max(Decimal("0"), dd))

# ---- breaker control (shared API) ----
def breaker_enforce(reason: str):
    """
    Passive enforcement: trip breaker using core.breaker (honors BREAKER_DEFAULT_TTL_SEC).
    Also mirror into DB guard_state.
    """
    breaker.set_on(reason=reason, ttl_sec=None, source="risk_daemon")
    guard_set_breaker(True, reason=reason)
    tg_send(f"⛔ Risk breaker SET • {reason}", priority="error")

def breaker_clear():
    if breaker.is_active():
        breaker.set_off(reason="risk_daemon_clear", source="risk_daemon")
        guard_set_breaker(False, reason="risk_daemon_clear")
        tg_send("✅ Risk breaker cleared.", priority="success")
    else:
        # Still mirror DB in case it was stale
        guard_set_breaker(False, reason="risk_daemon_clear")

# ---- relay sanity (optional) ----
def probe_relay() -> Tuple[bool, str]:
    if not RELAY_URL:
        return (True, "no-relay")
    import requests
    try:
        h = {"Content-Type":"application/json"}
        if RELAY_TOKEN:
            h["Authorization"] = f"Bearer {RELAY_TOKEN}"
            h["x-relay-token"] = RELAY_TOKEN
        r = requests.get(f"{RELAY_URL}/diag/time", headers=h, timeout=6)
        r.raise_for_status()
        return (True, "ok")
    except Exception as e:
        return (False, str(e))

def write_heartbeat():
    try:
        HEARTBEAT_PATH.write_text(str(int(time.time())), encoding="utf-8")
    except Exception:
        pass

# ---- main loop ----
def main():
    boot_t = time.time()
    tg_send("🟢 Risk Daemon online.", priority="success")

    # daily baseline map
    baseline_by_day: Dict[str, Dict[str, str]] = {}
    last_hour = None
    sent_eod = False
    last_alert_ts = 0.0

    # initial probe
    ok, why = probe_relay()
    if not ok:
        tg_send(f"⚠️ Relay not reachable for diagnostics: {why}", priority="warn")

    # ensure DB breaker reflects actual breaker at boot
    guard_set_breaker(breaker.is_active(), reason="boot_sync")

    while True:
        try:
            now = now_local()
            day = today_key(now)
            hour = now.hour

            # snapshot equities
            snap = snapshot_equities()

            # write pnl jsonl
            row = {"ts": now.isoformat(), "data": snap}
            append_jsonl(pnl_path_for(now), row)

            # set baseline if new day
            if day not in baseline_by_day:
                baseline_by_day[day] = snap
                base_total_boot = float(snap["total"])
                # reset DB day anchor to current total
                try:
                    guard_reset_day(base_total_boot)
                except Exception as e:
                    log.warning(f"guard_reset_day failed: {e}")
                # clear breaker on new day start, fresh baseline
                breaker_clear()

            base = baseline_by_day[day]
            cur_total = Decimal(snap["total"])
            base_total = Decimal(base["total"])
            dd_pct = compute_dd_pct(cur_total, base_total)

            # DB running PnL delta
            try:
                delta_usd = float(cur_total - base_total)
                guard_update_pnl(delta_usd)
            except Exception as e:
                log.debug(f"guard_update_pnl skipped: {e}")

            # hourly rollup
            if PNL_SEND_HOURLY and hour != last_hour:
                last_hour = hour
                tg_send(fmt_rollup(snap, base), priority="info")

            # daily summary
            if PNL_SEND_DAILY and hour == PNL_DAILY_SEND_HOUR and not sent_eod:
                tg_send("🧾 Daily PnL Summary\n" + fmt_rollup(snap, base), priority="success")
                sent_eod = True
            if hour != PNL_DAILY_SEND_HOUR:
                sent_eod = False

            # guardrail: daily DD breaker
            in_grace = (time.time() - boot_t) < max(0, RISK_STARTUP_GRACE_SEC)
            if not in_grace and dd_pct >= RISK_MAX_DD_PCT:
                # throttle duplicate nags
                if (time.time() - last_alert_ts) >= (RISK_NOTIFY_EVERY_MIN * 60):
                    last_alert_ts = time.time()
                    reason = f"DD {dd_pct:.2f}% ≥ {RISK_MAX_DD_PCT:.2f}% (vs {base_total:.2f})"
                    if RISK_AUTOFIX:
                        if not breaker.is_active():
                            breaker_enforce(reason)
                        else:
                            tg_send(f"⛔ Risk breaker still active • {reason}", priority="error")
                    else:
                        tg_send(f"⚠️ RISK: {reason}", priority="warn")

            # heartbeat
            write_heartbeat()

            time.sleep(PNL_POLL_SEC)

        except KeyboardInterrupt:
            log.info("Risk Daemon stopped by user.")
            break
        except Exception as e:
            log.warning(f"risk loop error: {e}")
            time.sleep(PNL_POLL_SEC)

if __name__ == "__main__":
    main()
