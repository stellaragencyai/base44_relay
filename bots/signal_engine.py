#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_engine.py — loads sub_map, deep-merges defaults, validates,
runs strategies in observe-only and emits normalized signals.

ENV:
  LOG_LEVEL=INFO|DEBUG
  OBSERVE_ONLY=true (only print signals)
  SIGNAL_DIR=signals
"""

import os, json, time, importlib, logging, pathlib, copy
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("signal_engine")

ROOT = pathlib.Path(__file__).resolve().parents[1]
REG = ROOT / "registry" / "sub_map.json"
SIGDIR = ROOT / (os.getenv("SIGNAL_DIR") or "signals")
SIGDIR.mkdir(exist_ok=True, parents=True)
OBSERVE_ONLY = (os.getenv("OBSERVE_ONLY") or "true").strip().lower() == "true"

def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def validate_schema(doc: Dict[str, Any]):
    required_top = ["meta","defaults","subs"]
    for k in required_top:
        if k not in doc:
            raise SystemExit(f"sub_map.json missing top-level key '{k}'")
    for sub_key, sub in doc["subs"].items():
        for req in ["name","role","strategy","risk","symbols"]:
            if req not in sub:
                raise SystemExit(f"sub '{sub_key}' missing '{req}'")
        r = sub["risk"]
        for fld in ["initial_risk_pct","daily_loss_cap_pct","max_concurrent_initial_risk_pct"]:
            if fld not in r or not isinstance(r[fld], (int,float)):
                raise SystemExit(f"sub '{sub_key}' risk.{fld} missing or not number")
        wl = sub["symbols"].get("whitelist", [])
        if sub.get("enabled", True) and not wl:
            raise SystemExit(f"sub '{sub_key}' enabled but whitelist empty")

def resolve_uids(doc: Dict[str, Any]):
    book = doc.get("uid_book", {})
    for sub_key, sub in doc["subs"].items():
        uid = (sub.get("uid") or "").strip()
        label = (sub.get("uid_label") or "").strip()
        if not uid and label and label in book:
            sub["uid"] = book[label]
    return doc

def load_strategies():
    from strategies import REGISTRY
    loaded = {}
    for name, path_spec in REGISTRY.items():
        module_name, class_name = path_spec.split(":")
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        loaded[name] = cls
    return loaded

def make_signal(sub_key, sym, sig_type, strength, params, meta=None):
    return {
        "ts": int(time.time()*1000),
        "sub": sub_key,
        "symbol": sym,
        "signal": sig_type,
        "strength": float(strength),
        "params": params or {},
        "meta": meta or {}
    }

def run_observe(doc: Dict[str, Any], strategies: Dict[str, Any]):
    outf = open(SIGDIR / "observed.jsonl", "a", encoding="utf-8")
    count = 0
    for sub_key, sub in doc["subs"].items():
        if not sub.get("enabled", True): 
            continue
        strat_key = sub["strategy"]
        if strat_key not in strategies:
            log.warning(f"sub {sub_key} strategy '{strat_key}' not registered; skipping")
            continue
        Strategy = strategies[strat_key]
        s = Strategy()
        # Placeholder data fetch here — you can wire in klines/ticks later
        params = {
            "risk_per_trade_pct": sub["risk"]["initial_risk_pct"],
            "maker_only": sub.get("maker_only", True),
            "spread_max_bps": sub.get("spread_max_bps", 8)
        }
        for sym in sub["symbols"]["whitelist"]:
            sigs = s.generate_signals(klines=None, tick=None, pos=None, params=params)
            for sig in sigs:
                if sig.get("symbol") and sig["symbol"] != sym:
                    continue
                payload = make_signal(sub_key, sym, sig.get("type","NOOP"), sig.get("strength",0), params, {"strategy": strat_key})
                line = json.dumps(payload, separators=(",",":"))
                print(line)
                outf.write(line + "\n")
                count += 1
    outf.close()
    log.info(f"observe finished, signals={count}")

if __name__ == "__main__":
    doc = load_json(REG)
    validate_schema(doc)
    docm = deep_merge({"subs": {}}, doc)  # no-op but future-proof
    docm = resolve_uids(docm)
    strategies = load_strategies()
    run_observe(docm, strategies)
