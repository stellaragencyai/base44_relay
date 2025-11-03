#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sub Registry CLI
- Simple commands to view and assign names/strategies/roles/flags to sub-UIDs.
Usage examples:
  python sub_registry_cli.py list
  python sub_registry_cli.py assign --uid 302355261 --name Main --role Main --tier Tier1
  python sub_registry_cli.py assign --uid 260417078 --name VehicleFund --role VehicleFund --flags vehicle:true
  python sub_registry_cli.py assign --uid 253800120 --strategy A7_Canary --tier Tier1
"""

import argparse, sys
from base44_registry import ensure_synced, get_all, assign, name_map

def cmd_list(args):
    reg = ensure_synced()
    subs = reg.get("subs", {})
    print("UID           | Name           | Role          | Tier  | Strategy       | Flags")
    print("-"*86)
    for uid, e in subs.items():
        flags = ",".join([k for k,v in (e.get("flags") or {}).items() if v]) or "-"
        print(f"{uid:<13}| { (e.get('name') or '-')[:14]:<14}| { (e.get('role') or '-')[:13]:<13}| "
              f"{ (e.get('tier') or '-')[:5]:<5}| { (e.get('strategy') or '-')[:14]:<14}| {flags}")

def _parse_flags(s: str):
    # "vehicle:true,disabled:false"
    out = {}
    if not s:
        return out
    parts = s.split(",")
    for p in parts:
        if ":" in p:
            k,v = p.split(":",1)
            out[k.strip()] = (v.strip().lower() in ("1","true","yes","y","on"))
    return out

def cmd_assign(args):
    flags = _parse_flags(args.flags or "")
    e = assign(
        uid=args.uid,
        name=args.name,
        strategy=args.strategy,
        role=args.role,
        tier=args.tier,
        flags=flags if flags else None
    )
    print("Updated:")
    print(e)

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")

    p_list = sub.add_parser("list", help="List registry")
    p_list.set_defaults(func=cmd_list)

    p_assign = sub.add_parser("assign", help="Assign fields to a uid")
    p_assign.add_argument("--uid", required=True)
    p_assign.add_argument("--name")
    p_assign.add_argument("--strategy")
    p_assign.add_argument("--role")
    p_assign.add_argument("--tier")
    p_assign.add_argument("--flags", help="k1:true,k2:false")
    p_assign.set_defaults(func=cmd_assign)

    args = p.parse_args()
    if not hasattr(args, "func"):
        p.print_help(); sys.exit(1)
    args.func(args)

if __name__ == "__main__":
    main()
