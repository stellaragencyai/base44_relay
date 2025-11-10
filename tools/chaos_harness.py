#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools.chaos_harness — end-to-end drills against a mock exchange.

Run with:
  python -m tools.chaos_harness
or:
  # if you insist on direct-run
  # at the very top add project root to sys.path
"""

from __future__ import annotations
import os, sys, time, json, threading, random
from pathlib import Path

# ------------------------------------------------------------------
# Make sure imports resolve when running as a script
# ------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STATE = ROOT / "state";   STATE.mkdir(exist_ok=True, parents=True)
SIGNALS = ROOT / "signals"; SIGNALS.mkdir(exist_ok=True, parents=True)
QUEUE = SIGNALS / "observed.jsonl"

# ------------------------------------------------------------------
# FORCE CHAOS ENVS *BEFORE* IMPORTING ANY BOTS
# ------------------------------------------------------------------
# Trading modes
os.environ["EXEC_DRY_RUN"] = "false"     # real flows through the mock
os.environ["SIG_DRY_RUN"]  = "false"
os.environ["TP_DRY_RUN"]   = "false"
os.environ["TPSL_ENABLED"] = "true"

# Symbols and limits
os.environ.setdefault("TP_SYMBOL_WHITELIST", "")       # manage all
os.environ.setdefault("RECON_SYMBOL_WHITELIST", "")

# Silence Telegram no matter what your shell says
os.environ["TELEGRAM_NOTIFY"] = "0"
os.environ.pop("TELEGRAM_BOT_TOKEN", None)
os.environ.pop("TELEGRAM_CHAT_ID", None)
os.environ.pop("TELEGRAM_CHAT_IDS", None)

# Keep outcome bus quiet if you wired one
os.environ.setdefault("OUTCOME_NOTIFY", "false")

# ------------------------------------------------------------------
# Monkeypatch Bybit to mock BEFORE importing bots
# ------------------------------------------------------------------
import types
from core.bybit_mock import MockBybit
bybit_client_mod = types.ModuleType("core.bybit_client")
setattr(bybit_client_mod, "Bybit", MockBybit)
sys.modules["core.bybit_client"] = bybit_client_mod

# Now it's safe to import bots and guard
from bots.executor_v1 import main as executor_main
from bots.tp_sl_manager import main as tpsl_main
from bots.reconciler import main as reconciler_main
from bots.outcome_watcher import main as outcome_main
from core.guard import guard_set, guard_clear

# ------------------------------------------------------------------
# Chaos knobs
# ------------------------------------------------------------------
SYMS = [s.strip().upper() for s in (os.getenv("CHAOS_SYMBOLS","BTCUSDT,ETHUSDT")).split(",") if s.strip()]
RUNTIME = int(os.getenv("CHAOS_RUNTIME_SEC", "45"))
SIG_RATE = float(os.getenv("CHAOS_SIGNAL_RATE", "0.8"))
TICK_MS  = int(os.getenv("CHAOS_TICK_MS", "400"))
TREND_BPS = float(os.getenv("CHAOS_TREND_BPS", "5.0"))

_stop = threading.Event()

def _append_signal(sym: str, side: str, *, r_stop_dist: float = 0.004):
    ts = int(time.time()*1000)
    obj = {
        "ts": ts,
        "symbol": sym,
        "signal": "LONG" if side=="Buy" else "SHORT",
        "params": {
            "tag": "B44",
            "maker_only": True,
            "spread_max_bps": 12,
            "stop_dist": r_stop_dist
        },
        "features": {
            "class": "EqualR",
            "edge_prob": 0.55,
            "prior_win_p": 0.55
        }
    }
    with QUEUE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, separators=(",",":")) + "\n")

def _signal_feeder():
    rnd = random.Random(42)
    while not _stop.is_set():
        for sym in SYMS:
            if rnd.random() < SIG_RATE * (TICK_MS/1000.0):
                side = "Buy" if rnd.random() < 0.5 else "Sell"
                _append_signal(sym, side)
        time.sleep(max(0.05, TICK_MS/1000.0))

def _price_driver():
    # the patched Bybit is our MockBybit
    from core.bybit_client import Bybit
    by = Bybit()
    rnd = random.Random(7)
    mids = {s: (50000.0 if "BTC" in s else 3000.0) for s in SYMS}
    while not _stop.is_set():
        for s in SYMS:
            drift = 1.0 + (TREND_BPS/10000.0) * (1 if rnd.random() < 0.5 else -1)
            noise = 1.0 + rnd.uniform(-0.0008, 0.0008)
            mids[s] *= drift * noise
            try:
                by._tick(s, mids[s])  # cross resting reduce-only orders
            except Exception:
                pass
        time.sleep(TICK_MS/1000.0)

def _breaker_gremlin():
    rnd = random.Random(1337)
    # start with breaker OFF so we actually open positions
    guard_clear("chaos start")
    while not _stop.is_set():
        time.sleep(3.5)
        # 35% chance to flip the breaker with a short TTL
        if rnd.random() < 0.35:
            guard_set("chaos_breaker", ttl_sec=rnd.randint(3,8))

def _spawn(target, name):
    th = threading.Thread(target=target, name=name, daemon=True)
    th.start()
    return th

def _start_bots():
    threads = []
    threads.append(_spawn(executor_main, "executor"))
    threads.append(_spawn(tpsl_main, "tpsl"))
    threads.append(_spawn(reconciler_main, "reconciler"))
    threads.append(_spawn(outcome_main, "outcome"))
    return threads

def _summary():
    # cheap readouts from files
    outcomes = []
    model = {}
    try:
        p = STATE / "outcomes.jsonl"
        if p.exists():
            with p.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try: outcomes.append(json.loads(line))
                    except Exception: pass
    except Exception: pass
    try:
        mp = STATE / "model_state.json"
        if mp.exists():
            model = json.loads(mp.read_text(encoding="utf-8"))
    except Exception: pass

    ok = sum(1 for o in outcomes if o.get("won"))
    ng = sum(1 for o in outcomes if not o.get("won"))
    print("\n=== CHAOS SUMMARY ===")
    print(f"outcomes: total={len(outcomes)}  won={ok}  lost={ng}")
    if model:
        pri = {k: round(v.get('alpha',0)/(v.get('alpha',0)+v.get('beta',1)), 3) for k,v in model.items()}
        print("priors:", pri)
    print("state dir:", str(STATE))

def main():
    print("[chaos] starting bots with mock exchange...")
    # clean queue
    try: QUEUE.unlink()
    except FileNotFoundError: pass

    bots = _start_bots()
    feeders = [
        _spawn(_signal_feeder, "signal_feeder"),
        _spawn(_price_driver, "price_driver"),
        _spawn(_breaker_gremlin, "breaker"),
    ]

    t0 = time.time()
    while time.time() - t0 < RUNTIME:
        time.sleep(0.25)
    _stop.set()
    guard_clear("chaos done")
    time.sleep(1.0)
    _summary()

if __name__ == "__main__":
    main()
