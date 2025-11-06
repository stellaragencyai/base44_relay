#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 — PnL Daily/Rollup (Telegram pretty print, CSV, emoji map)

What it does
- Pulls equity per account (main + SUB_UIDS), plus unrealized and realized PnL
- Computes deltas vs the same-day baseline saved with --snapshot
- Counts open positions per account
- Sends a tidy Telegram roll-up with emojis, names, absolute Δ and % Δ, and pos counts
- Appends to CSV logs/pnl/daily_YYYY-MM-DD.csv on every roll-up

CLI
  python -m bots.pnl_daily --snapshot   # save today's baseline
  python -m bots.pnl_daily              # send roll-up vs baseline

ENV (optional)
  PNL_TITLE=Portfolio PnL
  PNL_EQUITY_COIN=USDT
  PNL_SHOW_UNREAL=true
  PNL_SHOW_REAL=true
  PNL_EMOJI_MAIN=🧠
  PNL_EMOJI_DEFAULT=🟦
  SUB_UIDS=comma,separated,uids

Emoji/label map (optional)
  registry/sub_map.json
  {
    "main": "💼",
    "260417078": "🎯",
    "labels": { "main": "Main", "260417078": "Sniper 42" }
  }

Notes
- Compatible with updated core/relay_client.py where proxy() returns primary.body directly.
- Falls back gracefully if relay returns an envelope instead.
"""

from __future__ import annotations
import os, json, csv, time, math, pathlib, argparse, datetime
from typing import Dict, Any, List, Tuple

from dotenv import load_dotenv

# Relay + notifier
import core.relay_client as rc
try:
    from core.notifier_bot import tg_send
except Exception:
    def tg_send(msg: str, priority: str="info", **_):  # fallback
        print(f"[notify/{priority}] {msg}")

ROOT   = pathlib.Path(__file__).resolve().parents[1]
LOGDIR = ROOT / "logs" / "pnl"
LOGDIR.mkdir(parents=True, exist_ok=True)

load_dotenv(dotenv_path=ROOT / ".env")

def env_bool(k: str, default: bool) -> bool:
    v = (os.getenv(k, str(int(default))) or "").strip().lower()
    return v in {"1","true","yes","on"}

# Config
TITLE          = os.getenv("PNL_TITLE", "Portfolio PnL")
COIN           = os.getenv("PNL_EQUITY_COIN", "USDT")
SHOW_UNREAL    = env_bool("PNL_SHOW_UNREAL", True)
SHOW_REAL      = env_bool("PNL_SHOW_REAL", True)
EMOJI_MAIN     = os.getenv("PNL_EMOJI_MAIN", "🧠")
EMOJI_DEFAULT  = os.getenv("PNL_EMOJI_DEFAULT", "🟦")
SUB_UIDS       = [x.strip() for x in (os.getenv("SUB_UIDS","")).split(",") if x.strip()]

# Optional emoji/label map file
MAP_FILE = ROOT / "registry" / "sub_map.json"

def load_map() -> tuple[Dict[str,str], Dict[str,str]]:
    emojis: Dict[str,str] = {}
    labels: Dict[str,str] = {}
    try:
        j = json.loads(MAP_FILE.read_text(encoding="utf-8"))
        # emojis: "main": "💼", "260417078": "🎯"
        for k, v in j.items():
            if k == "labels":
                continue
            emojis[str(k)] = str(v)
        # labels block
        for k, v in (j.get("labels", {}) or {}).items():
            labels[str(k)] = str(v)
    except Exception:
        pass
    if "main" not in emojis:
        emojis["main"] = EMOJI_MAIN
    return emojis, labels

def now_utc() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

def day_key_utc(dt: datetime.datetime|None=None) -> str:
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")

def money(x: float) -> str:
    if abs(x) >= 1000:
        return f"{x:,.2f}"
    return f"{x:.2f}"

def pct_change(cur: float, prev: float) -> float:
    if prev == 0:
        return 0.0
    try:
        return (cur/prev - 1.0) * 100.0
    except Exception:
        return 0.0

def emoji_for(account_key: str, emojis: Dict[str,str]) -> str:
    if account_key == "main":
        return emojis.get("main", EMOJI_MAIN)
    if account_key.startswith("sub:"):
        uid = account_key.split(":",1)[1]
        return emojis.get(uid, EMOJI_DEFAULT)
    return EMOJI_DEFAULT

def label_for(account_key: str, labels: Dict[str,str]) -> str:
    if account_key == "main":
        return labels.get("main", "Main")
    if account_key.startswith("sub:"):
        uid = account_key.split(":",1)[1]
        return labels.get(uid, account_key)
    return account_key

def _force_body(resp: dict) -> dict:
    """Handle both 'body directly' and 'envelope with primary.body' responses."""
    if not isinstance(resp, dict):
        return {}
    if "result" in resp or "retCode" in resp:
        # likely already body
        return resp
    prim = (resp.get("primary", {}) or {})
    return prim.get("body", resp)

def wallet_equity(member_id: str|None, coin: str) -> tuple[float,float,float]:
    """
    Returns (total_equity, unrealized_pnl, realized_pnl) for UNIFIED.
    Tries per-coin breakdown first, falls back to totalEquity.
    """
    params = {"accountType": "UNIFIED"}
    if member_id:
        params["memberId"] = member_id
        raw = rc.proxy("GET", "/v5/account/wallet-balance", params=params)
    else:
        # Prefer helper when no member_id
        raw = rc.get_wallet_balance(accountType="UNIFIED")
    body = _force_body(raw)

    total_equity = 0.0
    unreal = 0.0
    realized = 0.0

    try:
        lst = ((body.get("result",{}) or {}).get("list",[]) or [])
        # Try coin breakdown first
        wanted = (coin or "USDT").upper()
        for acct in lst:
            coins = (acct.get("coin", []) or [])
            for c in coins:
                if (c.get("coin") or "").upper() == wanted:
                    total_equity += float(c.get("equity", 0) or 0.0)
                    unreal       += float(c.get("unrealisedPnl", 0) or 0.0)
                    realized     += float(c.get("cumRealisedPnl", 0) or 0.0)
        # Fallback: totalEquity
        if total_equity <= 0:
            for acct in lst:
                total_equity += float(acct.get("totalEquity", 0) or 0.0)
    except Exception:
        pass

    return float(total_equity or 0.0), float(unreal or 0.0), float(realized or 0.0)

def positions_count(member_id: str|None) -> int:
    params = {"category":"linear"}
    if member_id:
        params["memberId"] = member_id
    raw = rc.proxy("GET", "/v5/position/list", params=params)
    body = _force_body(raw)
    rows = ((body.get("result",{}) or {}).get("list",[]) or [])
    n = 0
    for p in rows:
        try:
            if float(p.get("size") or 0) > 0:
                n += 1
        except Exception:
            pass
    return n

def accounts() -> List[tuple[str, str|None]]:
    out = [("main", None)]
    for uid in SUB_UIDS:
        out.append((f"sub:{uid}", uid))
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

def csv_path_for(day: str) -> pathlib.Path:
    return LOGDIR / f"daily_{day}.csv"

def append_csv(ts: datetime.datetime, row_items: List[Dict[str,Any]]) -> None:
    path = csv_path_for(day_key_utc(ts))
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        if new:
            wr.writerow(["timestamp","account","equity","unreal","real","positions"])
        for r in row_items:
            wr.writerow([
                ts.isoformat(),
                r["key"],
                f"{r['equity']:.8f}",
                f"{r['unreal']:.8f}",
                f"{r['real']:.8f}",
                r["pos"]
            ])

def arrow(delta_pct: float) -> str:
    if delta_pct >  0.0005: return "▲"
    if delta_pct < -0.0005: return "▼"
    return "●"

def do_snapshot() -> None:
    day = day_key_utc()
    snap: Dict[str, Any] = {"_ts": int(time.time()*1000)}
    total = 0.0
    rows_for_csv: List[Dict[str,Any]] = []

    for key, mid in accounts():
        eq, unrl, rlzd = wallet_equity(mid, COIN)
        pos = positions_count(mid)
        total += eq
        snap[key] = {"equity": eq, "unreal": unrl, "real": rlzd, "pos": pos}
        rows_for_csv.append({"key": key, "equity": eq, "unreal": unrl, "real": rlzd, "pos": pos})

    snap["_portfolio_total"] = total
    save_snapshot(day, snap)

    ts = now_utc()
    append_csv(ts, rows_for_csv)

    tg_send(f"🗂 Snapshot saved for {day}: total={money(total)} {COIN}", priority="info")

def rollup() -> None:
    day  = day_key_utc()
    base = load_snapshot(day)  # same-day baseline
    emojis, labels = load_map()

    # collect live
    rows: List[Dict[str,Any]] = []
    total_live = 0.0
    total_base = float(base.get("_portfolio_total", 0.0))

    for key, mid in accounts():
        eq, unrl, rlzd = wallet_equity(mid, COIN)
        pos = positions_count(mid)
        total_live += eq
        base_eq = float((base.get(key) or {}).get("equity", 0.0))
        rows.append({
            "key": key,
            "equity": eq,
            "base_eq": base_eq,
            "unreal": unrl,
            "real": rlzd,
            "pos": pos
        })

    # write CSV line for roll-up moment
    ts = now_utc()
    append_csv(ts, rows)

    # header
    abs_delta = total_live - total_base
    pct_delta = pct_change(total_live, total_base)
    header = (
        f"📊 <b>{TITLE}</b> @ {ts.strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Portfolio: <b>{money(total_live)}</b> {COIN}  "
        f"(Δ {money(abs_delta)} | {pct_delta:+.2f}%) {arrow(pct_delta)}"
    )

    # lines
    lines: List[str] = []
    # stable ordering: main then subs
    ordered = ["main"] + [f"sub:{uid}" for uid in SUB_UIDS]
    row_map = {r["key"]: r for r in rows}

    for key in ordered:
        r = row_map.get(key)
        if not r:
            continue
        em  = emoji_for(key, emojis)
        name = label_for(key, labels)
        d_abs = r["equity"] - r["base_eq"]
        d_pct = pct_change(r["equity"], r["base_eq"])
        extras = []
        if SHOW_UNREAL:
            extras.append(f"U:{money(r['unreal'])}")
        if SHOW_REAL:
            extras.append(f"R:{money(r['real'])}")
        extra_str = ("  " + " ".join(extras)) if extras else ""
        # alignment: label 14 wide, equity 10 wide, Δ% 7 wide, pos 2 wide
        lines.append(
            f"{em} <code>{name:>14}</code>  <code>{money(r['equity']):>10}</code>  "
            f"Δ <code>{money(d_abs):>8}</code> | <code>{d_pct:+6.2f}%</code> {arrow(d_pct)}  "
            f"<code>pos:{r['pos']:>2}</code>{extra_str}"
        )

    # highlights
    hl = ""
    try:
        best = max(rows, key=lambda x: pct_change(x["equity"], x["base_eq"]))
        worst = min(rows, key=lambda x: pct_change(x["equity"], x["base_eq"]))
        hl = (
            f"\n⭐ <i>Best:</i> {label_for(best['key'], labels)} {pct_change(best['equity'], best['base_eq']):+.2f}%   "
            f"❗ <i>Worst:</i> {label_for(worst['key'], labels)} {pct_change(worst['equity'], worst['base_eq']):+.2f}%"
        )
    except Exception:
        pass

    msg = header + "\n" + "\n".join(lines) + hl
    tg_send(msg, priority="success")

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
