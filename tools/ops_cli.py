#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/ops_cli.py — one-stop operational CLI for Base44

Examples:
  python tools/ops_cli.py breaker --status
  python tools/ops_cli.py breaker --on --ttl 600 --reason "manual_pause"
  python tools/ops_cli.py breaker --off --reason "resume"
  python tools/ops_cli.py gate BTCUSDT ETHUSDT
  python tools/ops_cli.py guard
  python tools/ops_cli.py funding
  python tools/ops_cli.py db
  python tools/ops_cli.py ping --symbol BTCUSDT
  python tools/ops_cli.py notify "deploy complete"

Nothing here mutates trading state except breaker on/off.
"""

from __future__ import annotations
import os, sys, json, argparse, time
from typing import List, Dict, Any

# --- optional imports wired to your project ---
def _import_optional(path: str, name: str):
    try:
        return __import__(path, fromlist=[name]).__dict__.get(name)
    except Exception:
        return None

# breaker
_br_status   = _import_optional("core.breaker", "status")
_br_isactive = _import_optional("core.breaker", "is_active")
_br_on       = _import_optional("core.breaker", "set_on")
_br_off      = _import_optional("core.breaker", "set_off")
_br_left     = _import_optional("core.breaker", "remaining_ttl")

# gates
_gate_check  = _import_optional("core.gates", "check_all")

# guard
_guard_obj   = None
try:
    from core.portfolio_guard import guard as _guard_obj  # type: ignore
except Exception:
    _guard_obj = None

# funding
_funding_lock = _import_optional("core.funding_gate", "is_lockout")
_funding_win  = _import_optional("core.funding_gate", "funding_window")

# notifier
_tg_send = _import_optional("core.notifier_bot", "tg_send")

# db quick stats
_db_counts = None
try:
    from core.db import count_rows  # type: ignore
    _db_counts = count_rows
except Exception:
    _db_counts = None

# bybit public ping
_bybit_client = None
try:
    from core.bybit_client import Bybit  # type: ignore
    _bybit_client = Bybit
except Exception:
    _bybit_client = None


def cmd_breaker(args: argparse.Namespace) -> int:
    if _br_status is None:
        print("[ERR] breaker module not available"); return 2

    if args.status:
        print(json.dumps(_br_status(), indent=2))
        return 0

    if args.on:
        ttl = None if args.ttl is None else int(args.ttl)
        reason = args.reason or "manual"
        _br_on(reason=reason, ttl_sec=ttl, source="ops_cli")
        print(json.dumps(_br_status(), indent=2))
        return 0

    if args.off:
        reason = args.reason or "manual_clear"
        try:
            _br_off(reason=reason, source="ops_cli")
            print(json.dumps(_br_status(), indent=2))
            return 0
        except Exception as e:
            print(f"[ERR] breaker clear blocked: {e}")
            return 1

    if args.left:
        left = _br_left() if _br_left else 0
        print(left)
        return 0

    print("[HINT] use --status, --on, or --off")
    return 0


def cmd_gate(args: argparse.Namespace) -> int:
    if _gate_check is None:
        print("[ERR] gates module not available"); return 2
    syms = args.symbols or ["BTCUSDT","ETHUSDT"]
    out: Dict[str, Any] = {}
    for s in syms:
        gr = _gate_check(s)
        out[s.upper()] = {"allow": gr.allow, "reasons": gr.reasons, "detail": gr.detail}
    print(json.dumps(out, indent=2))
    return 0


def cmd_guard(args: argparse.Namespace) -> int:
    if _guard_obj is None:
        print("[ERR] portfolio_guard not available"); return 2
    hb = _guard_obj.heartbeat()
    print(json.dumps(hb, indent=2))
    return 0


def cmd_funding(args: argparse.Namespace) -> int:
    if _funding_win is None:
        print("[ERR] funding_gate not available"); return 2
    win = _funding_win()
    res = {"next_epoch": win.next_epoch, "minutes_to_next": win.minutes_to_next}
    if _funding_lock:
        blocked, w2 = _funding_lock(args.symbol or "BTCUSDT")
        res["lockout"] = bool(blocked)
        res["symbol"] = (args.symbol or "BTCUSDT").upper()
    print(json.dumps(res, indent=2))
    return 0


def cmd_db(args: argparse.Namespace) -> int:
    if _db_counts is None:
        print("[ERR] DB bindings not available"); return 2
    try:
        counts = _db_counts()
        print(json.dumps(counts, indent=2))
        return 0
    except Exception as e:
        print(f"[ERR] DB check failed: {e}")
        return 1


def cmd_ping(args: argparse.Namespace) -> int:
    if _bybit_client is None:
        print("[ERR] bybit client not available"); return 2
    sym = (args.symbol or "BTCUSDT").upper()
    by = _bybit_client()
    try:
        by.sync_time()
        ok, data, err = by.get_tickers(category="linear", symbol=sym)
        if not ok:
            print(f"[ERR] ticker fetch failed: {err}")
            return 1
        lst = (data.get("result") or {}).get("list") or []
        if not lst:
            print("[ERR] empty list from ticker endpoint")
            return 1
        row = lst[0]
        print(json.dumps({"symbol": sym, "bid1": row.get("bid1Price"), "ask1": row.get("ask1Price")}, indent=2))
        return 0
    except Exception as e:
        print(f"[ERR] ping failed: {e}")
        return 1


def cmd_notify(args: argparse.Namespace) -> int:
    msg = args.message or "(empty)"
    if _tg_send is None:
        print(f"[stdout notify] {msg}")
        return 0
    _tg_send(f"ℹ️ {msg}")
    print("[OK] notify sent (or attempted)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Base44 ops CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # breaker
    b = sub.add_parser("breaker", help="breaker on/off/status")
    b.add_argument("--status", action="store_true")
    b.add_argument("--on", action="store_true")
    b.add_argument("--off", action="store_true")
    b.add_argument("--ttl", type=int, help="seconds for ON TTL")
    b.add_argument("--reason", type=str, help="reason string")
    b.add_argument("--left", action="store_true", help="print remaining TTL seconds")
    b.set_defaults(func=cmd_breaker)

    # gates
    g = sub.add_parser("gate", help="evaluate entry gates for symbol(s)")
    g.add_argument("symbols", nargs="*", help="symbols to check, e.g. BTCUSDT ETHUSDT")
    g.set_defaults(func=cmd_gate)

    # guard
    d = sub.add_parser("guard", help="portfolio guard heartbeat")
    d.set_defaults(func=cmd_guard)

    # funding
    f = sub.add_parser("funding", help="show funding window and lockout")
    f.add_argument("--symbol", type=str, help="symbol for lockout badge (default BTCUSDT)")
    f.set_defaults(func=cmd_funding)

    # db
    db = sub.add_parser("db", help="quick DB counts (orders, executions, positions)")
    db.set_defaults(func=cmd_db)

    # public ping
    pi = sub.add_parser("ping", help="public ticker ping (network/IP sanity)")
    pi.add_argument("--symbol", type=str, default="BTCUSDT")
    pi.set_defaults(func=cmd_ping)

    # notify
    no = sub.add_parser("notify", help="send a simple Telegram message (if configured)")
    no.add_argument("message", nargs="?", type=str)
    no.set_defaults(func=cmd_notify)

    return p


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)  # type: ignore


if __name__ == "__main__":
    raise SystemExit(main())
