#!/usr/bin/env python3
# core/strategy_config.py
from __future__ import annotations

import os, json, hashlib
from pathlib import Path
from typing import Any, Dict, List
from copy import deepcopy

try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

ROOT        = Path(__file__).resolve().parents[1]
REGISTRY    = ROOT / "registry" / "sub_map.json"
STRAT_DIR   = ROOT / "config" / "strategies"
STATE_DIR   = ROOT / ".state" / "effective"
STATE_DIR.mkdir(parents=True, exist_ok=True)

class ConfigError(RuntimeError): ...
class MissingDependency(ConfigError): ...

# --------------------------- IO ---------------------------

def _load_json(p: Path) -> dict:
    if not p.exists():
        raise ConfigError(f"Missing config file: {p}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ConfigError(f"Bad JSON in {p}: {e}")

def _load_yaml(p: Path) -> dict:
    if yaml is None:
        raise MissingDependency("PyYAML not installed. pip install pyyaml")
    if not p.exists():
        raise ConfigError(f"Missing strategy file: {p.name}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            raise ConfigError(f"{p.name}: expected mapping at top level")
        return data
    except Exception as e:
        raise ConfigError(f"Bad YAML in {p.name}: {e}")

def _merge(a: dict, b: dict) -> dict:
    """Deep merge b into a without mutating inputs."""
    out = deepcopy(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out

# --------------------------- Helpers ---------------------------

def list_strategies() -> List[str]:
    """Return available *.yaml strategy names without extension."""
    if not STRAT_DIR.exists():
        return []
    return sorted([p.stem for p in STRAT_DIR.glob("*.yaml")])

def _sha1(obj: Any) -> str:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(s).hexdigest()

def _coerce_bool(x: Any, default: bool) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}

def _coerce_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _coerce_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _upper_csv(seq: Any) -> List[str]:
    if seq is None:
        return []
    if isinstance(seq, str):
        seq = [s for s in seq.split(",")]
    return [str(s).strip().upper() for s in seq if str(s).strip()]

# --------------------------- Registry ---------------------------

def load_sub_record(sub_label_or_uid: str) -> dict:
    data = _load_json(REGISTRY)
    subs = data.get("subs", {})
    for label, rec in subs.items():
        uid = str(rec.get("uid", "")).strip()
        if label == sub_label_or_uid or uid == str(sub_label_or_uid).strip():
            out = deepcopy(rec)
            out["label"] = label
            out["uid"] = uid
            # defaults for automation flags at the sub level
            out.setdefault("automation", {})
            out["automation"].setdefault("managed", True)           # if False, system will not place orders
            out["automation"].setdefault("allow_manual", False)     # sub is allowed manual human entries
            return out
    raise ConfigError(f"Sub not found in registry: {sub_label_or_uid}")

# --------------------------- Strategy layering ---------------------------

def _load_strategy_tree(name: str, _seen: set[str] | None = None) -> dict:
    """
    Loads a strategy YAML and resolves 'extends:' chains depth-first.
    Child overrides parent.
    """
    _seen = _seen or set()
    if name in _seen:
        raise ConfigError(f"extends loop detected in strategies: {' -> '.join(list(_seen)+[name])}")
    _seen.add(name)

    path = STRAT_DIR / f"{name}.yaml"
    strat = _load_yaml(path)

    parent_name = (strat.get("extends") or "").strip()
    if parent_name:
        parent = _load_strategy_tree(parent_name, _seen)
        strat = _merge(parent, strat)

    return strat

# --------------------------- Environment overrides ---------------------------

def _apply_env_overrides(eff: dict) -> dict:
    """
    Minimal ENV override surface so you can hot-patch behavior without editing files.
      ENV_RISK_PCT           -> risk.initial_risk_pct
      ENV_TP_RUNGS           -> strategy.tp.ladder_count
      ENV_MAX_LEVERAGE       -> symbols.max_leverage
      ENV_WHITELIST          -> symbols.whitelist (CSV)
    """
    rp  = os.getenv("ENV_RISK_PCT")
    tpr = os.getenv("ENV_TP_RUNGS")
    ml  = os.getenv("ENV_MAX_LEVERAGE")
    wl  = os.getenv("ENV_WHITELIST")

    if rp is not None:
        eff.setdefault("risk", {})["initial_risk_pct"] = _coerce_float(rp, eff.get("risk", {}).get("initial_risk_pct", 0.1))
    if tpr is not None:
        eff.setdefault("strategy", {}).setdefault("tp", {})["ladder_count"] = _coerce_int(tpr, 5)
    if ml is not None:
        eff.setdefault("symbols", {})["max_leverage"] = _coerce_int(ml, 50)
    if wl is not None:
        eff.setdefault("symbols", {})["whitelist"] = _upper_csv(wl)

    return eff

# --------------------------- Normalization & Validation ---------------------------

def _normalize(eff: dict) -> dict:
    # meta
    eff.setdefault("meta", {})
    m = eff["meta"]
    m["label"]   = str(m.get("label", "")).strip()
    m["uid"]     = str(m.get("uid", "")).strip()
    m["strategy"]= str(m.get("strategy", "")).strip()
    m.setdefault("schema_version", 1)

    # risk
    eff.setdefault("risk", {})
    r = eff["risk"]
    r["initial_risk_pct"] = _coerce_float(r.get("initial_risk_pct", 0.10), 0.10)  # percent of equity
    r.setdefault("per_trade_max_leverage", None)  # optional finer cap
    r.setdefault("cooldown_sec", 300)

    # symbols
    eff.setdefault("symbols", {})
    s = eff["symbols"]
    s["whitelist"] = _upper_csv(s.get("whitelist", []))
    s["blacklist"] = _upper_csv(s.get("blacklist", []))
    s["max_leverage"] = _coerce_int(s.get("max_leverage", 50), 50)

    # guardrails
    eff.setdefault("guardrails", {})
    g = eff["guardrails"]
    g.setdefault("global_daily_dd_pct", 3.0)
    g.setdefault("enforce_whitelist", True)
    g.setdefault("news_lockout", False)  # your earlier preference to drop manual lockout

    # strategy
    eff.setdefault("strategy", {})
    st = eff["strategy"]

    # regime gates
    st.setdefault("regime", {})
    reg = st["regime"]
    reg.setdefault("min_bias_adx", 18.0)
    reg.setdefault("min_atr_pct", 0.25)
    reg.setdefault("vol_z_min", 0.8)
    reg.setdefault("bias_tf_min", 60)
    reg.setdefault("scan_tf_min", [5, 15])

    # execution knobs
    st.setdefault("execution", {})
    ex = st["execution"]
    ex.setdefault("post_only_ticks", 2)
    ex.setdefault("maker_only", True)
    ex.setdefault("allow_shorts", True)
    ex.setdefault("window", {"start": "00:00", "end": "23:59"})
    ex.setdefault("equity_coin", "USDT")

    # tp/sl policy
    st.setdefault("tp", {})
    tp = st["tp"]
    tp["ladder_count"] = _coerce_int(tp.get("ladder_count", 5), 5)  # align with your 5 TP preference
    tp.setdefault("alloc_equal", True)
    tp.setdefault("equal_r_start", 0.5)
    tp.setdefault("equal_r_step", 0.5)
    tp.setdefault("tag", "B44")

    st.setdefault("sl", {})
    sl = st["sl"]
    sl.setdefault("use_closer_of_structure_or_atr", True)
    sl.setdefault("atr_mult_fallback", 0.45)
    sl.setdefault("atr_buffer", 0.08)
    sl.setdefault("min_tick_buffer", 2)

    # automation flags
    eff.setdefault("automation", {})
    a = eff["automation"]
    a.setdefault("observe_only", True)  # default safe
    a.setdefault("live", False)
    a.setdefault("adopt_existing_orders", True)
    a.setdefault("cancel_non_b44", False)
    a.setdefault("tp_ws_disable", False)

    return eff

def _validate(eff: dict, sub_label: str):
    # symbols
    wl = eff.get("symbols", {}).get("whitelist", [])
    if not wl:
        raise ConfigError(f"{sub_label}: symbols.whitelist is empty")
    if eff["risk"]["initial_risk_pct"] <= 0:
        raise ConfigError(f"{sub_label}: risk.initial_risk_pct must be > 0")
    lev = eff["symbols"]["max_leverage"]
    if not (1 <= lev <= 200):
        raise ConfigError(f"{sub_label}: symbols.max_leverage must be between 1 and 200")
    tpr = eff["strategy"]["tp"]["ladder_count"]
    if not (1 <= tpr <= 100):
        raise ConfigError(f"{sub_label}: strategy.tp.ladder_count must be 1..100")

# --------------------------- Composition ---------------------------

def render_effective_config(sub_label_or_uid: str, *, persist: bool = True, apply_env: bool = True) -> dict:
    """
    Compose final effective config for a sub:
      base strategy (with extends) + registry fields + per-sub overrides + env overrides
    """
    sub = load_sub_record(sub_label_or_uid)
    preset = (sub.get("strategy") or "").strip()
    if not preset:
        raise ConfigError(f"{sub['label']}: strategy not set")

    # 1) load strategy tree
    strat = _load_strategy_tree(preset)

    # 2) base shape from registry
    effective = {
        "meta": {
            "label": sub["label"],
            "uid": str(sub.get("uid", "")).strip(),
            "strategy": preset,
        },
        "risk": sub.get("risk", {}),
        "symbols": sub.get("symbols", {}),
        "guardrails": sub.get("guardrails", {}),
        "automation": sub.get("automation", {}),
        "strategy": strat,
    }

    # 3) per-sub overrides (arbitrary dict in registry: overrides: {...})
    if isinstance(sub.get("overrides"), dict):
        effective = _merge(effective, sub["overrides"])

    # 4) normalize
    effective = _normalize(effective)

    # 5) env overrides
    if apply_env:
        effective = _apply_env_overrides(effective)

    # 6) validate
    _validate(effective, sub["label"])

    # 7) checksum + persist for other bots
    checksum = _sha1(effective)
    effective["meta"]["checksum"] = checksum

    if persist:
        out_path = STATE_DIR / f"{effective['meta']['label']}.json"
        try:
            out_path.write_text(json.dumps(effective, indent=2), encoding="utf-8")
        except Exception:
            # non-fatal
            pass

    return effective

def effective_for_all_subs(*, persist: bool = True, apply_env: bool = True) -> Dict[str, dict]:
    """
    Render all subs defined in the registry and return mapping {label: effective_config}
    """
    data = _load_json(REGISTRY)
    subs = data.get("subs", {})
    out: Dict[str, dict] = {}
    for label in sorted(subs.keys()):
        try:
            out[label] = render_effective_config(label, persist=persist, apply_env=apply_env)
        except Exception as e:
            # keep going but include error info for diagnostics
            out[label] = {"error": str(e), "meta": {"label": label}}
    return out
