#!/usr/bin/env python3
# core/strategy_config.py
from __future__ import annotations
import os, json, math
from pathlib import Path
from typing import Any, Dict
from copy import deepcopy

try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "registry" / "sub_map.json"
STRAT_DIR = ROOT / "config" / "strategies"

class ConfigError(RuntimeError): pass

def _load_json(p: Path) -> dict:
    if not p.exists():
        raise ConfigError(f"Missing config file: {p}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ConfigError(f"Bad JSON in {p}: {e}")

def _load_yaml(p: Path) -> dict:
    if yaml is None:
        raise ConfigError("PyYAML not installed. pip install pyyaml")
    if not p.exists():
        raise ConfigError(f"Missing strategy file: {p.name}")
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise ConfigError(f"Bad YAML in {p.name}: {e}")

def _merge(a: dict, b: dict) -> dict:
    out = deepcopy(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out

def load_sub_record(sub_label_or_uid: str) -> dict:
    data = _load_json(REGISTRY)
    subs = data.get("subs", {})
    # match by label (SUB7) or by exact uid string
    for label, rec in subs.items():
        if label == sub_label_or_uid or str(rec.get("uid","")).strip() == str(sub_label_or_uid).strip():
            rec = deepcopy(rec)
            rec["label"] = label
            return rec
    raise ConfigError(f"Sub not found in registry: {sub_label_or_uid}")

def render_effective_config(sub_label_or_uid: str) -> dict:
    sub = load_sub_record(sub_label_or_uid)
    preset = (sub.get("strategy") or "").strip()
    if not preset:
        raise ConfigError(f"{sub['label']}: strategy not set")
    preset_file = STRAT_DIR / f"{preset}.yaml"
    strat = _load_yaml(preset_file)

    # Base shape
    effective = {
        "meta": {
            "label": sub["label"],
            "uid": str(sub.get("uid","")).strip(),
            "strategy": preset,
        },
        "risk": sub.get("risk", {}),
        "symbols": sub.get("symbols", {}),
        "guardrails": sub.get("guardrails", {}),
    }
    effective = _merge(effective, {"strategy": strat})

    # Normalize
    rp = effective["risk"].get("initial_risk_pct", 0.0)
    effective["risk"]["initial_risk_pct"] = float(rp)

    ladder = effective["strategy"].get("tp", {}).get("ladder_count", 50)
    effective["strategy"]["tp"]["ladder_count"] = int(ladder)

    wl = effective.get("symbols", {}).get("whitelist", []) or []
    effective["symbols"]["whitelist"] = [s.strip().upper() for s in wl]

    max_lev = effective.get("symbols", {}).get("max_leverage", 50)
    effective["symbols"]["max_leverage"] = int(max_lev)

    # Basic validation
    if not effective["symbols"]["whitelist"]:
        raise ConfigError(f"{sub['label']}: symbols.whitelist is empty")
    if effective["risk"]["initial_risk_pct"] <= 0:
        raise ConfigError(f"{sub['label']}: risk.initial_risk_pct must be > 0")

    return effective
