#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/gate_probe.py — quick CLI to see gate decisions
Usage:
  python tools/gate_probe.py BTCUSDT ETHUSDT
"""
from __future__ import annotations
import sys, json
from core.gates import check_all

def main():
    syms = sys.argv[1:] or ["BTCUSDT","ETHUSDT"]
    out = {}
    for s in syms:
        gr = check_all(s)
        out[s.upper()] = {"allow": gr.allow, "reasons": gr.reasons, "detail": gr.detail}
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
