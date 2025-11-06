#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — PnL Daily/Rollup (Telegram pretty print)

- Pulls equity per account (main + SUB_UIDS)
- Computes deltas vs last snapshot of the same UTC day
- Counts open positions per account
- Prints tidy Telegram message with emojis and alignment
- Supports --snapshot to write a baseline file, otherwise prints live rollup

ENV (optional):
  PNL_TITLE=Portfolio PnL
  PNL_EQUITY_COIN=USDT
  PNL_SHOW_UNREAL=true
  PNL_SHOW_REAL=true
  PNL_EMOJI_MAIN=🧠
  PNL_EMOJI_DEFAULT=🟦
  PNL_EMOJI_MAP=uid:emoji,uid:emoji,...
"""

from __future__ import annotations
import os, json, time, math, pathlib, argparse, datetime
from typing import Dict, Any, List, Tuple

from dotenv import load_dotenv

# Relay + notifier
import core.relay_client as rc
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_):  # fallback
        print(f"[notify/{priority}] {msg}")

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOGDIR = ROOT / "logs" / "pnl"
LOGDIR.mkdir(parents=True, exist_ok=True)

load_dotenv(dotenv_path=ROOT / ".env")

def env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

TITLE          = os.getenv("PNL_TITLE", "Portfolio PnL")
COIN           = os.getenv("PNL_EQUITY_COIN", "USDT")
SHOW_UNREAL    = env_bool("PNL_SHOW_UNREAL", True)
SHOW_REAL      = env_bool("PNL_SHOW_REAL", True)
EMOJI_MAIN     = os.getenv("PNL_EMOJI_MAIN", "🧠")
EMOJI_DEFAULT  = os.getenv("PNL_EMOJI_DEFAULT", "🟦")

# Parse mapping like "260417078:🎯,302355261:🧪"
def parse_emoji_map(s: str) -> Dict[str, str]:
    out = {}
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        uid, emo = part.split(":", 1)
        out[uid.strip()] = emo.strip() or EMOJI_DEFAULT
    return out

EMOJI_MAP = parse_emoji_map(os.getenv("PNL_EMOJI_MAP", ""))

SUB_UIDS = [x.strip() for x in (os.getenv("SUB_UIDS","")).split(",") if x.strip()]

def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def day_key_utc(dt: datetime.datetime=None) -> str:
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")

def money(x: float) -> str:
    # friendlier fixed width for tiny accounts too
    if abs(x) >= 1000:
        return f"{x:,.2f}"
    return f"{x:.2f}"

def pct_safe(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return (a - b) / b * 100.0
    except Exception:
        return 0.0

def arrow(delta: float) -> str:
    if delta > 0.0005:
        return "▲"
    if delta < -0.0005:
        return "▼"
    return "●"

def _wallet_equity(extra: Dict[str, Any]|None, coin: str) -> Tuple[float, float, float]:
    """
    Returns (total_equity, unrealized_pnl, realized_pnl) for unified account.
    Falls back gracefully if fields are missing.
    """
    params = {"accountType": "UNIFIED"}
    try:
        resp = rc.proxy("GET", "/v5/account/wallet-balance", params=params, timeout=20) if extra else rc.proxy("GET", "/v5/account/wallet-balance", params=params, timeout=20)
        body = (resp.get("primary",{}) or {}).get("body",{}) if isinstance(resp, dict) else {}
        lst  = ((body.get("result",{}) or {}).get("list",[]) or [])
        total_equity = 0.0
        unreal = 0.0
        realized = 0.0
        for acct in lst:
            for c in (acct.get("coin",[]) or []):
                if (c.get("coin") or "").upper() == (coin or "USDT").upper():
                    try: total_equity += float(c.get("equity",0))
                    except: pass
                    try: unreal += float(c.get("unrealisedPnl",0))
                    except: pass
                    try: realized += float(c.get("cumRealisedPnl",0))
                    except: pass
        # If no coin breakdown, try top level totalEquity
        if total_equity <= 0:
            for acct in lst:
                try: total_equity += float(acct.get("totalEquity",0))
                except: pass
        return float(total_equity or 0.0), float(unreal or 0.0), float(realized or 0.0)
    except Exception:
        return 0.0, 0.0, 0.0

def _positions_count(extra: Dict[str,Any]|None) -> int:
    try:
        env = {"category":"linear"}
        if extra and "subUid" in extra:
            resp = rc.proxy("GET", "/v5/position/list", params=env | {"subUid": extra["subUid"]})
        else:
            resp = rc.proxy("GET", "/v5/position/list", params=env)
        body = (resp.get("primary",{}) or {}).get("body",{})
        rows = ((body.get("result",{}) or {}).get("list",[]) or [])
        # count only non-zero positions
        n = 0
        for p in rows:
            try:
                if float(p.get("size") or 0) > 0:
                    n += 1
            except Exception:
                pass
        return n
    except Exception:
        return 0

def collect_accounts() -> List[Tuple[str, Dict[str,Any]]]:
    out = [("main", {})]
    for uid in SUB_UIDS:
        out.append((f"sub:{uid}", {"subUid": uid}))
    return out

def snapshot_path(day: str) -> pathlib.Path:
    return LOGDIR / f"daily_{day}.json"

def load_snapshot(day: str) -> Dict[str, Any]:
    p = snapshot_path(day)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_snapshot(day: str, data: Dict[str, Any]) -> None:
    snapshot_path(day).write_text(json.dumps(data, indent=2), encoding="utf-8")

def emoji_for(account_key: str) -> str:
    if account_key == "main":
        return EMOJI_MAIN
    if account_key.startswith("sub:"):
        uid = account_key.split(":",1)[1]
        return EMOJI_MAP.get(uid, EMOJI_DEFAULT)
    return EMOJI_DEFAULT

def label_for(account_key: str) -> str:
    if account_key == "main":
        return "main"
    if account_key.startswith("sub:"):
        return account_key  # or resolve nickname from registry if you want
    return account_key

def rollup() -> None:
    day = day_key_utc()
    prev = load_snapshot(day)  # same-day baseline
    rows = []
    portfolio_total = 0.0
    prev_total = float(prev.get("_portfolio_total", 0.0))

    for acct_key, extra in collect_accounts():
        eq, unrl, rlzd = _wallet_equity(extra, COIN)
        pcnt = _positions_count(extra)
        portfolio_total += eq
        prev_eq = float(prev.get(acct_key, {}).get("equity", 0.0))
        rows.append({
            "key": acct_key,
            "equity": eq,
            "prev_eq": prev_eq,
            "unreal": unrl,
            "real": rlzd,
            "pos": pcnt
        })

    # Build message
    ts = now_utc().strftime("%Y-%m-%d %H:%M UTC")
    delta_port = pct_safe(portfolio_total, prev_total)
    hdr = f"📊 <b>{TITLE}</b> @ {ts}\nPortfolio: <b>{money(portfolio_total)}</b> {COIN}  Δ {delta_port:+.2f}% {arrow(delta_port)}"

    # Format rows monospaced
    # columns: emoji label equity delta pos [unreal/real]
    lines = []
    for r in rows:
        em = emoji_for(r["key"])
        lab = label_for(r["key"])
        dlt = pct_safe(r["equity"], r["prev_eq"])
        pos = r["pos"]
        extras = []
        if SHOW_UNREAL:
            extras.append(f"U:{money(r['unreal'])}")
        if SHOW_REAL:
            extras.append(f"R:{money(r['real'])}")
        extra_str = ("  " + " ".join(extras)) if extras else ""
        # pad label to same width for alignment
        labp = f"{lab:>14}"
        ln = f"{em} <code>{labp}</code>  <code>{money(r['equity']):>8}</code>  Δ <code>{dlt:+6.2f}%</code> {arrow(dlt)}  <code>pos:{pos:>2}</code>{extra_str}"
        lines.append(ln)

    # Simple highlights
    try:
        best = max(rows, key=lambda x: pct_safe(x["equity"], x["prev_eq"]))
        worst = min(rows, key=lambda x: pct_safe(x["equity"], x["prev_eq"]))
        bdl = pct_safe(best["equity"], best["prev_eq"])
        wdl = pct_safe(worst["equity"], worst["prev_eq"])
        hl = f"\n⭐ <i>Best:</i> {label_for(best['key'])} {bdl:+.2f}%   ❗ <i>Worst:</i> {label_for(worst['key'])} {wdl:+.2f}%"
    except Exception:
        hl = ""

    msg = hdr + "\n" + "\n".join(lines) + hl
    tg_send(msg, priority="success")

def do_snapshot() -> None:
    day = day_key_utc()
    snap = {"_ts": int(time.time()*1000)}
    total = 0.0
    for acct_key, extra in collect_accounts():
        eq, unrl, rlzd = _wallet_equity(extra, COIN)
        total += eq
        snap[acct_key] = {"equity": eq, "unreal": unrl, "real": rlzd}
    snap["_portfolio_total"] = total
    save_snapshot(day, snap)
    tg_send(f"🗂 Snapshot saved for {day}: total={money(total)} {COIN}", priority="info")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", action="store_true", help="write today's baseline")
    args = ap.parse_args()

    if args.snapshot:
        do_snapshot()
    else:
        rollup()

if __name__ == "__main__":
    main()
