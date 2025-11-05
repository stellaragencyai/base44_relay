#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backfill last 5 days realized PnL (UTC) from Bybit closed PnL endpoint.
Writes logs/pnl/daily_backfill.csv and prints a compact table.
"""

import os, csv, sys, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

BASE = Path(__file__).resolve().parents[1]
load_dotenv(BASE / ".env")
LOG_DIR = BASE / "logs" / "pnl"
LOG_DIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = LOG_DIR / "daily_backfill.csv"

API_KEY = os.getenv("BYBIT_API_KEY","")
API_SEC = os.getenv("BYBIT_API_SECRET","")
ENV     = os.getenv("BYBIT_ENV","mainnet").lower().strip()
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID","")

def tg_send(msg:str):
    if not (TG_TOKEN and TG_CHAT): return
    import requests
    requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                  json={"chat_id":TG_CHAT,"text":msg}, timeout=6)

http = HTTP(testnet=(ENV=="testnet"), api_key=API_KEY, api_secret=API_SEC)

def yyyymmdd(dt:datetime)->str: return dt.strftime("%Y-%m-%d")

def fetch_closed_pnl(start_ts:int, end_ts:int, symbol:str=None):
    """Call /v5/position/closed-pnl paginated; returns list rows."""
    out=[]
    cursor=None
    while True:
        params = {
            "category":"linear",
            "startTime": str(start_ts),
            "endTime":   str(end_ts),
            "limit": "200",
        }
        if cursor: params["cursor"]=cursor
        if symbol: params["symbol"]=symbol
        r = http.get_closed_pnl(**params)
        if r.get("retCode")!=0: break
        lst = (r.get("result") or {}).get("list") or []
        out.extend(lst)
        cursor = (r.get("result") or {}).get("nextPageCursor")
        if not cursor: break
        time.sleep(0.1)
    return out

def main():
    today = datetime.now(timezone.utc).date()
    start_day = max(today - timedelta(days=4), datetime(2025,11,1,tzinfo=timezone.utc).date())
    # build day buckets from start_day..today
    days = []
    d = start_day
    while d <= today:
        days.append(d)
        d += timedelta(days=1)

    results = []
    # one fetch for the whole window to reduce API calls
    start_ts = int(datetime.combine(days[0], datetime.min.time(), tzinfo=timezone.utc).timestamp()*1000)
    end_ts   = int(datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc).timestamp()*1000)
    rows = fetch_closed_pnl(start_ts, end_ts)

    # aggregate realized pnl by UTC date
    by_day = {d: 0.0 for d in days}
    for r in rows:
        try:
            ts   = int(r.get("updatedTime") or r.get("createdTime") or r.get("execTime"))
            dt   = datetime.fromtimestamp(ts/1000, tz=timezone.utc).date()
            rpnl = float(r.get("closedPnl") or r.get("realisedPnl") or 0.0)
            if dt in by_day:
                by_day[dt] += rpnl
        except Exception:
            continue

    # write CSV
    new_file = not CSV_PATH.exists()
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date_utc","realized_pnl"])
        for d in days:
            w.writerow([d.isoformat(), f"{by_day[d]:.6f}"])

    # print summary
    lines = ["Date (UTC)   Realized PnL"]
    for d in days:
        lines.append(f"{d.isoformat()}   {by_day[d]:+.2f}")
    table = "\n".join(lines)
    print(table)
    tg_send("📊 PnL backfill last 5 days:\n" + table)

if __name__=="__main__":
    main()
