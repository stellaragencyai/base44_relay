# core/base44_registry.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Registry Helper (rooted to /registry folder)
- sub_uids.csv      (input discovered UIDs)
- sub_map.json      (persistent mapping: name/role/strategy/tier/flags)
"""

import os, json, csv
from pathlib import Path
from typing import Dict, List, Optional, Any

# project root = ../ from this file
BASE_DIR = Path(__file__).resolve().parents[1]
REG_DIR  = BASE_DIR / "registry"
CSV_PATH = REG_DIR / "sub_uids.csv"
REG_PATH = REG_DIR / "sub_map.json"

def _empty_entry(uid: str) -> Dict[str, Any]:
    return {
        "uid": uid,
        "name": "",
        "strategy": "",
        "tier": "",
        "role": "",
        "limits": {
            "max_initial_risk_pct": None,
            "max_concurrent_risk_pct": None,
            "symbol_concentration_pct": None
        },
        "flags": {
            "vehicle": False,
            "canary": False,
            "disabled": False
        }
    }

def _read_csv_uids() -> List[str]:
    uids: List[str] = []
    if CSV_PATH.exists():
        with CSV_PATH.open(newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for row in rd:
                val = (row.get("sub_uid") or "").strip()
                if val:
                    uids.append(val)
    return uids

def load_registry() -> Dict[str, Any]:
    if REG_PATH.exists():
        try:
            return json.loads(REG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"subs": {}}  # uid -> entry

def save_registry(reg: Dict[str, Any]) -> None:
    REG_DIR.mkdir(parents=True, exist_ok=True)
    REG_PATH.write_text(json.dumps(reg, indent=2), encoding="utf-8")

def ensure_synced() -> Dict[str, Any]:
    """Merge any new CSV UIDs into the registry; keep existing metadata."""
    REG_DIR.mkdir(parents=True, exist_ok=True)
    reg = load_registry()
    subs = reg.get("subs", {})
    updated = False
    for uid in _read_csv_uids():
        if uid not in subs:
            subs[uid] = _empty_entry(uid)
            updated = True
    reg["subs"] = subs
    if updated:
        save_registry(reg)
    return reg

# ------- Queries -------

def get_all() -> Dict[str, Dict[str, Any]]:
    return ensure_synced().get("subs", {})

def get_by_uid(uid: str) -> Optional[Dict[str, Any]]:
    return get_all().get(uid)

def find_by_role(role: str) -> Optional[Dict[str, Any]]:
    role = (role or "").strip().lower()
    for e in get_all().values():
        if (e.get("role") or "").strip().lower() == role:
            return e
    return None

def find_by_flag(flag: str) -> Optional[Dict[str, Any]]:
    for e in get_all().values():
        if e.get("flags", {}).get(flag) is True:
            return e
    return None

def list_by_strategy(strategy: str):
    s = (strategy or "").strip().lower()
    return [e for e in get_all().values() if (e.get("strategy") or "").strip().lower() == s]

def name_map() -> Dict[str, str]:
    return {uid: (e.get("name") or uid) for uid, e in get_all().items()}

# Env overrides (optional)
def main_uid() -> Optional[str]:
    env_uid = os.getenv("MAIN_SUB_UID", "").strip()
    if env_uid: return env_uid
    e = find_by_role("main")
    return e.get("uid") if e else None

def vehicle_uid() -> Optional[str]:
    env_uid = os.getenv("VEHICLE_SUB_UID", "").strip()
    if env_uid: return env_uid
    e = find_by_role("vehiclefund") or find_by_flag("vehicle")
    return e.get("uid") if e else None

# ------- Mutations -------

def assign(uid: str, name: str=None, strategy: str=None, role: str=None, tier: str=None, flags: Dict[str,bool]=None):
    reg = ensure_synced()
    subs = reg["subs"]
    if uid not in subs:
        subs[uid] = _empty_entry(uid)
    if name is not None:     subs[uid]["name"] = name
    if strategy is not None: subs[uid]["strategy"] = strategy
    if role is not None:     subs[uid]["role"] = role
    if tier is not None:     subs[uid]["tier"] = tier
    if flags:                subs[uid].setdefault("flags", {}).update(flags)
    save_registry(reg)
    return subs[uid]
