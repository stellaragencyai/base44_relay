# registry/sub_registry_cli.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sub Registry CLI

What this does:
- Lists and assigns metadata for Bybit sub-accounts stored in your Base44 registry.
- Robust import discovery for base44_registry no matter where you run it.
- Pretty table output, optional JSON, CSV export, UID-only view, and simple filters.
- Optional sync on startup to ensure registry files are fresh.

Env (optional):
- BASE44_CORE_DIR: absolute path to your /core directory (import hint)

Examples:
  python registry/sub_registry_cli.py list
  python registry/sub_registry_cli.py list --json
  python registry/sub_registry_cli.py list --filter "tier:Tier2,role:Scanner"
  python registry/sub_registry_cli.py list --uids
  python registry/sub_registry_cli.py list --csv registry/subs_export.csv
  python registry/sub_registry_cli.py assign --uid 123456 --name "Sub7 Canary" --tier Tier1 --role Canary --strategy "Micro-Impulse" --flags tradable:true,canary:true
"""

import argparse
import csv
import json
import os
import sys
import importlib
from pathlib import Path

# --------------------------------------------------------------------------------------
# Import base44_registry with path fallbacks
# --------------------------------------------------------------------------------------
CUR = Path(__file__).resolve()
REG_DIR = CUR.parent
BASE44_CORE_DIR = os.getenv("BASE44_CORE_DIR", "")

def _import_module_with_fallbacks(mod_name: str):
    candidates = []
    if BASE44_CORE_DIR:
        candidates.append(Path(BASE44_CORE_DIR))

    repo_root = CUR.parents[1]              # <repo> that should contain /core and /registry
    core_in_repo = repo_root / "core"
    core_next_to_registry = REG_DIR / "core"

    candidates += [core_in_repo, core_next_to_registry, repo_root, REG_DIR]

    tried = []
    for p in candidates:
        try:
            if p.exists():
                if str(p) not in sys.path:
                    sys.path.insert(0, str(p))
                return importlib.import_module(mod_name)
            tried.append(str(p))
        except Exception:
            tried.append(str(p))
            continue

    raise ImportError(
        f"Unable to import {mod_name}. Tried:\n  - " + "\n  - ".join(tried) +
        "\nSet BASE44_CORE_DIR to your /core directory if needed."
    )

try:
    _registry = _import_module_with_fallbacks("base44_registry")
except Exception as e:
    raise SystemExit(f"[sub_registry_cli] cannot import base44_registry: {e}")

# --------------------------------------------------------------------------------------
# Safe wrappers around base44_registry
# --------------------------------------------------------------------------------------
def _ensure_synced():
    fn = getattr(_registry, "ensure_synced", None)
    if callable(fn):
        try:
            return fn()
        except Exception as e:
            print(f"[sub_registry_cli] ensure_synced failed: {e}")
            return None
    print("[sub_registry_cli] warn: base44_registry.ensure_synced not found; continuing unsynced.")
    return None

def _get_all():
    fn = getattr(_registry, "get_all", None)
    if not callable(fn):
        raise SystemExit("base44_registry.get_all not found")
    try:
        return fn()
    except Exception as e:
        raise SystemExit(f"get_all failed: {e}")

def _assign(**kwargs):
    fn = getattr(_registry, "assign", None)
    if not callable(fn):
        raise SystemExit("base44_registry.assign not found")
    try:
        return fn(**kwargs)
    except TypeError as te:
        # In case signature drifted, try loosening
        # Keep only known keys that won't explode most implementations
        allowed = {k: v for k, v in kwargs.items() if k in ("uid", "name", "strategy", "role", "tier", "flags")}
        try:
            return fn(**allowed)
        except Exception as e2:
            raise SystemExit(f"assign failed: {e2}") from te
    except Exception as e:
        raise SystemExit(f"assign failed: {e}")

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def _parse_kv_csv(s: str):
    """
    Parse "k1:v1,k2:v2" into dict with smart bools.
    """
    out = {}
    if not s:
        return out
    for p in s.split(","):
        if ":" not in p:
            continue
        k, v = p.split(":", 1)
        v = v.strip()
        vl = v.lower()
        if vl in ("1","true","yes","y","on"):  val = True
        elif vl in ("0","false","no","n","off"): val = False
        else: val = v
        out[k.strip()] = val
    return out

def _filter_subs(subs: dict, filt: dict) -> dict:
    if not filt:
        return subs
    def match(entry: dict):
        for k, v in filt.items():
            ev = (entry.get(k) or "")
            # flags can be nested
            if k == "flags":
                flags = entry.get("flags") or {}
                # v can be dict
                if isinstance(v, dict):
                    for fk, fv in v.items():
                        if str(flags.get(fk, "")).lower() != str(fv).lower():
                            return False
                continue
            if str(v).lower() not in str(ev).lower():
                return False
        return True

    return {uid: e for uid, e in subs.items() if match(e)}

def _fmt_flags(flags: dict) -> str:
    if not flags:
        return "-"
    ons = [k for k, v in flags.items() if v]
    return ",".join(ons) if ons else "-"

def _table_print(subs: dict):
    print("UID           | Name           | Role          | Tier  | Strategy        | Flags")
    print("-" * 96)
    for uid, e in subs.items():
        uid_s = str(uid)
        name = (e.get("name") or "-")[:14]
        role = (e.get("role") or "-")[:13]
        tier = (e.get("tier") or "-")[:5]
        strat = (e.get("strategy") or "-")[:15]
        flags = _fmt_flags(e.get("flags") or {})
        print(f"{uid_s:<13} | {name:<14} | {role:<13} | {tier:<5} | {strat:<15} | {flags}")

def _csv_write(subs: dict, path: Path):
    fieldnames = ["uid", "name", "role", "tier", "strategy", "flags"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for uid, e in subs.items():
            flags = e.get("flags") or {}
            w.writerow({
                "uid": uid,
                "name": e.get("name") or "",
                "role": e.get("role") or "",
                "tier": e.get("tier") or "",
                "strategy": e.get("strategy") or "",
                "flags": json.dumps(flags, ensure_ascii=False),
            })

# --------------------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------------------
def cmd_list(args):
    if not args.no_sync:
        _ensure_synced()
    reg = _get_all() or {}
    subs = reg.get("subs", {})

    # filters
    filt = _parse_kv_csv(args.filter or "")
    subs = _filter_subs(subs, filt)

    if args.uids:
        for uid in subs.keys():
            print(uid)
        return

    if args.json:
        print(json.dumps(subs, indent=2, ensure_ascii=False))
        return

    _table_print(subs)

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        _csv_write(subs, out)
        print(f"\n[export] wrote {out}")

def cmd_assign(args):
    flags = _parse_kv_csv(args.flags or "")
    e = _assign(
        uid=args.uid,
        name=args.name,
        strategy=args.strategy,
        role=args.role,
        tier=args.tier,
        flags=flags if flags else None
    )
    print("Updated:")
    print(json.dumps(e, indent=2, ensure_ascii=False))

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(prog="sub_registry_cli", description="Manage Base44 sub registry")
    sub = p.add_subparsers(dest="cmd")

    p_list = sub.add_parser("list", help="List subs")
    p_list.add_argument("--filter", help='Filter like "tier:Tier2,role:Scanner"', default=None)
    p_list.add_argument("--json", action="store_true", help="Output JSON instead of table")
    p_list.add_argument("--uids", action="store_true", help="Print only UIDs, one per line")
    p_list.add_argument("--csv", help="Export to CSV at the given path", default=None)
    p_list.add_argument("--no-sync", action="store_true", help="Do not call ensure_synced() first")
    p_list.set_defaults(func=cmd_list)

    p_assign = sub.add_parser("assign", help="Assign/update a sub entry")
    p_assign.add_argument("--uid", required=True)
    p_assign.add_argument("--name")
    p_assign.add_argument("--strategy")
    p_assign.add_argument("--role")
    p_assign.add_argument("--tier")
    p_assign.add_argument("--flags", help="k1:true,k2:false")
    p_assign.set_defaults(func=cmd_assign)

    args = p.parse_args()
    if not hasattr(args, "func"):
        p.print_help()
        sys.exit(1)
    args.func(args)

if __name__ == "__main__":
    main()
