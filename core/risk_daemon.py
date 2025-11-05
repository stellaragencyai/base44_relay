#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — PnL Logger (portfolio + subaccounts)

What it does:
- Every PNL_POLL_SEC seconds, snapshots equity for main + each subUid.
- Writes JSONL to logs/pnl/<YYYY-MM-DD>.jsonl.
- Sends hourly Telegram rollup (if PNL_SEND_HOURLY=true).
- Sends end-of-day summary at PNL_DAILY_SEND_HOUR local time (PNL_SEND_DAILY=true).

Env:
  PNL_POLL_SEC=1800
  PNL_SEND_HOURLY=true
  PNL_SEND_DAILY=true
  PNL_DAILY_SEND_HOUR=23
  TZ=America/Phoenix
  SUB_UIDS=uid1,uid2,...
  BYBIT_* creds same as others
"""
from __future__ import annotations
import os, json, time, logging, datetime
from pathlib import Path
from decimal import Decimal, getcontext
from typing import Dict

from dotenv import load_dotenv
from pybit.unified_trading import HTTP

try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

getcontext().prec = 28
log = logging.getLogger("pnl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

LOG_DIR = ROOT / "logs" / "pnl"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

def env_int(name: str, default: int) -> int:
    try: return int((os.getenv(name, str(default)) or "").strip())
    except: return default

def env_decimal(name: str, default: str) -> Decimal:
    try: return Decimal((os.getenv(name, default) or default).strip())
    except: return Decimal(default)

PNL_POLL_SEC = env_int("PNL_POLL_SEC", 1800)
PNL_SEND_HOURLY = env_bool("PNL_SEND_HOURLY", True)
PNL_SEND_DAILY = env_bool("PNL_SEND_DAILY", True)
PNL_DAILY_SEND_HOUR = env_int("PNL_DAILY_SEND_HOUR", 23)
TZ = os.getenv("TZ", "America/Phoenix") or "America/Phoenix"

BYBIT_KEY = os.getenv("BYBIT_API_KEY","")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET","")
BYBIT_ENV = (os.getenv("BYBIT_ENV","mainnet") or "mainnet").lower().strip()
SUB_UIDS = [s.strip() for s in (os.getenv("SUB_UIDS","") or "").split(",") if s.strip()]

if not (BYBIT_KEY and BYBIT_SECRET):
    raise SystemExit("Missing BYBIT_API_KEY/BYBIT_API_SECRET in .env")

http = HTTP(testnet=(BYBIT_ENV=="testnet"), api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)

def _now_local():
    try:
        from zoneinfo import ZoneInfo
        return datetime.datetime.now(ZoneInfo(TZ))
    except Exception:
        return datetime.datetime.now()

def _equity(extra) -> Decimal:
    res = http.get_wallet_balance(accountType="UNIFIED", **extra)
    coins = (res.get("result") or {}).get("list", []) or []
    eq = Decimal("0")
    if coins:
        for c in coins[0].get("coin", []):
            usd = Decimal(str(c.get("usdValue") or "0"))
            if usd == 0 and c.get("coin") in {"USDT","USDC"}:
                usd = Decimal(str(c.get("walletBalance") or "0"))
            eq += usd
    return eq

def snapshot() -> Dict[str, str]:
    data: Dict[str, str] = {}
    data["main"] = str(_equity({}))
    for uid in SUB_UIDS:
        data[f"sub:{uid}"] = str(_equity({"subUid": uid}))
    total = sum(Decimal(v) for v in data.values())
    data["total"] = str(total)
    return data

def write_jsonl(day: str, row: dict):
    fp = LOG_DIR / f"{day}.jsonl"
    with fp.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def fmt_rollup(data: Dict[str, str], baseline: Dict[str, str] | None) -> str:
    def pct(cur: Decimal, base: Decimal) -> str:
        return f"{((cur-base)/base*Decimal('100')):.2f}%" if base > 0 else "n/a"
    cur_total = Decimal(data["total"])
    base_total = Decimal(baseline["total"]) if baseline else Decimal("0")
    lines = [f"📊 PnL Rollup @ {_now_local().strftime('%Y-%m-%d %H:%M')}",
             f"Portfolio: {cur_total:.2f}  Δ {pct(cur_total, base_total) if baseline else 'n/a'}"]
    for k,v in data.items():
        if k=="total": continue
        cur = Decimal(v)
        base = Decimal(baseline.get(k,"0")) if baseline else Decimal("0")
        lines.append(f"• {k:>9}: {cur:.2f}  Δ {pct(cur, base) if baseline else 'n/a'}")
    return "\n".join(lines)

def main():
    tg_send("🟢 PnL Logger online.", priority="success")
    last_hour = None
    sent_today = False
    baseline_by_day: Dict[str, Dict[str, str]] = {}
    while True:
        try:
            now = _now_local()
            day = now.strftime("%Y-%m-%d")
            hour = now.hour

            data = snapshot()
            row = {"ts": now.isoformat(), "data": data}
            write_jsonl(day, row)

            # baseline per day
            if day not in baseline_by_day:
                baseline_by_day[day] = data

            # hourly rollup
            if PNL_SEND_HOURLY and hour != last_hour:
                last_hour = hour
                tg_send(fmt_rollup(data, baseline_by_day[day]), priority="info")

            # daily summary at configured local hour
            if PNL_SEND_DAILY and hour == PNL_DAILY_SEND_HOUR and not sent_today:
                tg_send("🧾 Daily PnL Summary\n" + fmt_rollup(data, baseline_by_day[day]), priority="success")
                sent_today = True
            if hour != PNL_DAILY_SEND_HOUR:
                sent_today = False

        except Exception as e:
            log.warning(f"pnl loop error: {e}")
        time.sleep(PNL_POLL_SEC)

if __name__ == "__main__":
    main()
