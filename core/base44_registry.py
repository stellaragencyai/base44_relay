#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 Registry Helper (rooted to /registry folder)

Files:
  - sub_uids.csv      (input/discovered UIDs; column 'sub_uid' or first column)
  - sub_map.json      (persistent mapping: uid -> entry)

Design goals:
  - Backward compatible public API (get_all, get_by_uid, find_by_role, etc.).
  - Crash-safe atomic writes.
  - Gentle schema validation/auto-healing.
  - Idempotent CSV sync (no dupes, tolerates header variants).
  - Small helper mutations for automation (assign, set_limits, enable/disable).

Env (optional):
  BASE44_REGISTRY_DIR   : override registry directory (default: <repo>/registry)
"""

from __future__ import annotations
import os, json, csv, threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# ---------- paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]
REG_DIR  = Path(os.getenv("BASE44_REGISTRY_DIR", str(BASE_DIR / "registry")))
CSV_PATH = REG_DIR / "sub_uids.csv"
REG_PATH = REG_DIR / "sub_map.json"
_TMP_PATH = REG_DIR / ".sub_map.tmp"

_LOCK = threading.RLock()

# ---------- schema ----------
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

def _validate_entry(uid: str, e: Dict[str, Any]) -> Dict[str, Any]:
    """Return a sanitized entry without throwing."""
    base = _empty_entry(uid)
    out = dict(base)

    if isinstance(e, dict):
        out["uid"] = str(e.get("uid", uid))
        out["name"] = str(e.get("name") or "")
        out["strategy"] = str(e.get("strategy") or "")
        out["tier"] = str(e.get("tier") or "")
        out["role"] = str(e.get("role") or "")

        # limits
        lim = e.get("limits") or {}
        if isinstance(lim, dict):
            out["limits"]["max_initial_risk_pct"]   = _coerce_num(lim.get("max_initial_risk_pct"))
            out["limits"]["max_concurrent_risk_pct"]= _coerce_num(lim.get("max_concurrent_risk_pct"))
            out["limits"]["symbol_concentration_pct"]= _coerce_num(lim.get("symbol_concentration_pct"))

        # flags
        fl = e.get("flags") or {}
        if isinstance(fl, dict):
            out["flags"]["vehicle"]  = bool(fl.get("vehicle", False))
            out["flags"]["canary"]   = bool(fl.get("canary", False))
            out["flags"]["disabled"] = bool(fl.get("disabled", False))
    return out

def _coerce_num(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

# ---------- IO helpers ----------
def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _TMP_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(_TMP_PATH, path)

def _read_csv_uids() -> List[str]:
    uids: List[str] = []
    if not CSV_PATH.exists():
        return uids
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        sniffer = csv.Sniffer()
        sample = f.read(1024)
        f.seek(0)
        has_header = False
        try:
            has_header = sniffer.has_header(sample)
        except Exception:
            pass

        if has_header:
            rd = csv.DictReader(f)
            for row in rd:
                # tolerate various header spellings
                val = (row.get("sub_uid")
                       or row.get("uid")
                       or row.get("id")
                       or "").strip()
                if not val and row:
                    # fallback: first non-empty cell
                    for x in row.values():
                        if str(x).strip():
                            val = str(x).strip()
                            break
                if val:
                    uids.append(val)
        else:
            rd = csv.reader(f)
            for row in rd:
                if not row:
                    continue
                val = str(row[0]).strip()
                if val:
                    uids.append(val)
    # de-dupe in order
    seen = set()
    out: List[str] = []
    for u in uids:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def load_registry() -> Dict[str, Any]:
    """Return dict with key 'subs': {uid -> entry} ensuring schema-correct entries."""
    with _LOCK:
        if REG_PATH.exists():
            try:
                raw = json.loads(REG_PATH.read_text(encoding="utf-8"))
                subs = (raw or {}).get("subs", {})
                if not isinstance(subs, dict):
                    subs = {}
            except Exception:
                subs = {}
        else:
            subs = {}

        # sanitize all entries
        cleaned: Dict[str, Any] = {}
        for uid, e in subs.items():
            cleaned[str(uid)] = _validate_entry(str(uid), e if isinstance(e, dict) else {})

        return {"subs": cleaned}

def save_registry(reg: Dict[str, Any]) -> None:
    with _LOCK:
        subs = (reg or {}).get("subs", {})
        # sanitize before write
        cleaned = {str(uid): _validate_entry(str(uid), e if isinstance(e, dict) else {})
                   for uid, e in (subs.items() if isinstance(subs, dict) else [])}
        _atomic_write_json(REG_PATH, {"subs": cleaned})

def ensure_synced() -> Dict[str, Any]:
    """
    Merge any new CSV UIDs into the registry; keep existing metadata.
    Idempotent and schema-safe.
    """
    with _LOCK:
        reg = load_registry()
        subs = reg.get("subs", {})
        updated = False
        for uid in _read_csv_uids():
            key = str(uid).strip()
            if key and key not in subs:
                subs[key] = _empty_entry(key)
                updated = True
        if updated:
            save_registry({"subs": subs})
        return {"subs": subs}

# ---------- Queries ----------
def get_all() -> Dict[str, Dict[str, Any]]:
    """Return {uid -> entry} after syncing CSV."""
    return ensure_synced().get("subs", {})

def list_uids() -> List[str]:
    return list(get_all().keys())

def get_by_uid(uid: str) -> Optional[Dict[str, Any]]:
    if not uid:
        return None
    return get_all().get(str(uid).strip())

def find_by_role(role: str) -> Optional[Dict[str, Any]]:
    role = (role or "").strip().lower()
    for e in get_all().values():
        if (e.get("role") or "").strip().lower() == role:
            return e
    return None

def find_by_flag(flag: str) -> Optional[Dict[str, Any]]:
    flag = (flag or "").strip()
    for e in get_all().values():
        if bool(((e.get("flags") or {}) or {}).get(flag)) is True:
            return e
    return None

def list_by_strategy(strategy: str) -> List[Dict[str, Any]]:
    s = (strategy or "").strip().lower()
    return [e for e in get_all().values() if (e.get("strategy") or "").strip().lower() == s]

def name_map() -> Dict[str, str]:
    return {uid: (e.get("name") or uid) for uid, e in get_all().items()}

# Env overrides (optional)
def main_uid() -> Optional[str]:
    env_uid = os.getenv("MAIN_SUB_UID", "").strip()
    if env_uid:
        return env_uid
    e = find_by_role("main")
    return e.get("uid") if e else None

def vehicle_uid() -> Optional[str]:
    env_uid = os.getenv("VEHICLE_SUB_UID", "").strip()
    if env_uid:
        return env_uid
    e = find_by_role("vehiclefund") or find_by_flag("vehicle")
    return e.get("uid") if e else None

# ---------- Mutations ----------
def assign(uid: str,
           name: Optional[str] = None,
           strategy: Optional[str] = None,
           role: Optional[str] = None,
           tier: Optional[str] = None,
           flags: Optional[Dict[str, bool]] = None) -> Dict[str, Any]:
    """
    Upsert an entry and return it. Safe for automation.
    """
    with _LOCK:
        reg = ensure_synced()
        subs = reg["subs"]
        key = str(uid).strip()
        if key not in subs:
            subs[key] = _empty_entry(key)
        if name is not None:     subs[key]["name"] = str(name)
        if strategy is not None: subs[key]["strategy"] = str(strategy)
        if role is not None:     subs[key]["role"] = str(role)
        if tier is not None:     subs[key]["tier"] = str(tier)
        if flags:
            subs[key].setdefault("flags", {}).update({k: bool(v) for k, v in flags.items()})
        save_registry({"subs": subs})
        return subs[key]

def set_limits(uid: str,
               max_initial_risk_pct: Optional[float] = None,
               max_concurrent_risk_pct: Optional[float] = None,
               symbol_concentration_pct: Optional[float] = None) -> Dict[str, Any]:
    with _LOCK:
        reg = ensure_synced()
        subs = reg["subs"]
        key = str(uid).strip()
        if key not in subs:
            subs[key] = _empty_entry(key)
        lim = subs[key].setdefault("limits", {})
        if max_initial_risk_pct is not None:
            lim["max_initial_risk_pct"] = _coerce_num(max_initial_risk_pct)
        if max_concurrent_risk_pct is not None:
            lim["max_concurrent_risk_pct"] = _coerce_num(max_concurrent_risk_pct)
        if symbol_concentration_pct is not None:
            lim["symbol_concentration_pct"] = _coerce_num(symbol_concentration_pct)
        save_registry({"subs": subs})
        return subs[key]

def disable_sub(uid: str, reason: str = "") -> Dict[str, Any]:
    with _LOCK:
        reg = ensure_synced()
        subs = reg["subs"]
        key = str(uid).strip()
        if key not in subs:
            subs[key] = _empty_entry(key)
        subs[key].setdefault("flags", {})["disabled"] = True
        if reason:
            subs[key]["flags"]["disabled_reason"] = reason
        save_registry({"subs": subs})
        return subs[key]

def enable_sub(uid: str) -> Dict[str, Any]:
    with _LOCK:
        reg = ensure_synced()
        subs = reg["subs"]
        key = str(uid).strip()
        if key not in subs:
            subs[key] = _empty_entry(key)
        subs[key].setdefault("flags", {})["disabled"] = False
        subs[key]["flags"].pop("disabled_reason", None)
        save_registry({"subs": subs})
        return subs[key]

# Convenience aliases used by some automation scripts
def set_strategy(uid: str, strategy: str) -> Dict[str, Any]:
    return assign(uid, strategy=strategy)

def set_role(uid: str, role: str) -> Dict[str, Any]:
    return assign(uid, role=role)

def set_tier(uid: str, tier: str) -> Dict[str, Any]:
    return assign(uid, tier=tier)
