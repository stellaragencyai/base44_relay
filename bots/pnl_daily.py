#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bots/pnl_daily.py — Daily equity PnL snapshot + report (main + sub-accounts)

Usage (from project root, venv active):
  # take today's baseline snapshot (run once after midnight local)
  python -m bots.pnl_daily --snapshot

  # produce end-of-day report and send to Telegram
  python -m bots.pnl_daily --report

What it does:
- Pulls UNIFIED equity for PNL_EQUITY_COIN across main + SUB_UIDS via relay /v5/account/wallet-balance
- Writes snapshot to logs/pnl/YYYY-MM-DD/baseline.json
- On report, compares current equity to baseline and prints + Telegram-sends a rollup
"""

from __future__ import annotations
import os, json, time, math, pathlib, argparse, datetime
from typing import Dict, Tuple, Optional, List

from dotenv import load_dotenv

# Relay client (must expose proxy(method, path, params/body))
import core.relay_client as rc

# Notifier (optional but we have it)
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_): print(f"[notify/{priority}] {msg}")

ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

LOG_DIR = ROOT / "logs" / "pnl"
TODAY_DIR = LOG_DIR / datetime.date.today().isoformat()
TODAY_DIR.mkdir(parents=True, exist_ok=True)

COIN = os.getenv("PNL_EQUITY_COIN", "USDT") or "USDT"
SUBS = [s.strip() for s in (os.getenv("PNL_SUB_UIDS","") or "").split(",") if s.strip()]
ACCOUNT_TYPE = "UNIFIED"  # bybit unified trading

def _wallet_equity(sub_uid: Optional[str]=None) -> float:
    """
    Fetch equity for COIN from wallet-balance endpoint via relay.
    Supports subUid param for sub-accounts.
    """
    params = {"accountType": ACCOUNT_TYPE, "coin": COIN}
    if sub_uid:
        params["subUid"] = str(sub_uid)

    resp = rc.proxy("GET", "/v5/account/wallet-balance", params=params)
    # accept dict or JSON string
    if isinstance(resp, str):
        try:
            resp = json.loads(resp)
        except Exception:
            return 0.0

    # Some relays wrap; normalize
    result = (resp.get("result") or {})
    lst = result.get("list") or []
    if not lst:
        return 0.0
    entry = lst[0]
    # bybit returns coin list as array of dicts under 'coin'
    coin_rows = entry.get("coin") or []
    if not coin_rows:
        return 0.0
    row = None
    for c in coin_rows:
        if str(c.get("coin")).upper() == COIN.upper():
            row = c; break
    if not row:
        return 0.0
    try:
        return float(row.get("equity") or 0.0)
    except Exception:
        return 0.0

def _snapshot_path() -> pathlib.Path:
    return TODAY_DIR / "baseline.json"

def _load_snapshot() -> Dict[str, float]:
    p = _snapshot_path()
    if not p.exists(): return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_snapshot(data: Dict[str, float]) -> None:
    p = _snapshot_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")

def collect_equities() -> Dict[str, float]:
    out: Dict[str, float] = {}
    out["main"] = _wallet_equity(None)
    for uid in SUBS:
        out[f"sub:{uid}"] = _wallet_equity(uid)
    return out

def fmt(num: float) -> str:
    # USDT-ish formatting
    return f"{num:,.2f}"

def sign(num: float) -> str:
    return "🟢" if num > 0 else ("🔴" if num < 0 else "⚪")

def mk_report(baseline: Dict[str, float], current: Dict[str, float]) -> Tuple[str, float, float]:
    lines: List[str] = []
    tot_base = 0.0; tot_cur = 0.0

    # stable ordering: main first, then subs numerically
    keys = ["main"] + sorted([k for k in current.keys() if k.startswith("sub:")],
                             key=lambda x: int(x.split(":")[1]))

    for k in keys:
        b = float(baseline.get(k, 0.0))
        c = float(current.get(k, 0.0))
        d = c - b
        pct = (d / b * 100.0) if b > 0 else 0.0
        tot_base += b; tot_cur += c
        lines.append(f"{sign(d)} {k:<10} Δ {fmt(d)}  ({pct:+.2f}%)   cur {fmt(c)}  base {fmt(b)}")

    tot_d = tot_cur - tot_base
    tot_pct = (tot_d / tot_base * 100.0) if tot_base > 0 else 0.0

    header = f"📊 Daily PnL — {datetime.date.today().isoformat()} • coin={COIN}"
    footer = f"—\n{sign(tot_d)} TOTAL Δ {fmt(tot_d)}  ({tot_pct:+.2f}%)   cur {fmt(tot_cur)}  base {fmt(tot_base)}"

    return "\n".join([header, *lines, footer]), tot_d, tot_pct

def do_snapshot() -> None:
    equities = collect_equities()
    _save_snapshot(equities)
    tg_send(f"🟢 PnL baseline recorded for {datetime.date.today().isoformat()} • {COIN}\n" +
            "\n".join([f"{k:<10} {fmt(v)}" for k,v in equities.items()] ),
            priority="success")

def do_report() -> None:
    base = _load_snapshot()
    if not base:
        tg_send("⚠️ No baseline found for today; run --snapshot first.", priority="warn")
        return
    cur = collect_equities()
    report, tot_d, tot_pct = mk_report(base, cur)
    tg_send(report, priority="info")
    # persist a copy of the report
    (TODAY_DIR / "report.txt").write_text(report, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Daily PnL rollup (snapshot/report)")
    ap.add_argument("--snapshot", action="store_true", help="Take baseline snapshot for today")
    ap.add_argument("--report", action="store_true", help="Send end-of-day PnL report")
    args = ap.parse_args()

    if args.snapshot:
        do_snapshot()
    elif args.report:
        do_report()
    else:
        print("Nothing to do. Try --snapshot or --report.")

if __name__ == "__main__":
    main()
