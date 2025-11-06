#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Base44 .env validator & normalizer

Usage:
  python tools/validate_env.py                  # validate ./.env
  python tools/validate_env.py path/to/.env     # validate specific file
  python tools/validate_env.py --fix            # validate + write .env.fixed
  python tools/validate_env.py path --fix       # validate + write path.fixed
"""

import os, re, sys
from pathlib import Path
from typing import Dict, Tuple, List

RE_BOOL  = re.compile(r"^(true|false|0|1)$", re.I)
RE_INT   = re.compile(r"^-?\d+$")
RE_FLOAT = re.compile(r"^-?\d+(\.\d+)?$")
RE_URL   = re.compile(r"^https?://[^\s]+$", re.I)
RE_TIME  = re.compile(r"^\d{2}:\d{2}$")
RE_TZ    = re.compile(r"^[A-Za-z]+/[A-Za-z_]+$")
RE_SYM   = re.compile(r"^[A-Z0-9]+USDT$")

CSV_KEYS_SYMBOLS = {"SYMBOL_WHITELIST","SYMBOL_SCAN_LIST","EXEC_SYMBOLS","SIG_SYMBOLS"}
CSV_KEYS_UIDS    = {"PNL_SUB_UIDS","SUB_UIDS"}

BOOL_KEYS = {
    "TP_ADOPT_EXISTING","TP_CANCEL_NON_B44","TP_DRY_RUN","TP_WS_DISABLE",
    "PNL_SEND_HOURLY","PNL_SEND_DAILY","OBSERVE_ONLY","OBSERVE_TEST_SIGNAL",
    "OBSERVE_APPEND","SIG_ENABLED","SIG_SEND_CHART_LINKS","SIG_DRY_RUN",
    "LIVE","EXEC_ENABLED","EXEC_DRY_RUN","EXEC_ALLOW_SHORTS","POST_ONLY",
    "SAFE_MODE","DRY_RUN","PROXY_ENABLE"
}

def load_env(path: Path) -> Dict[str, str]:
    env = {}
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            raw = line.rstrip("\n")
            s = raw.strip()
            if not s or s.startswith("#"):
                # preserve comments/blank lines as markers with unique keys
                env[f"#LINE:{ln}"] = raw
                continue
            if "=" not in raw:
                env[f"#LINE:{ln}"] = f"# [IGNORED MALFORMED] {raw}"
                continue
            k, v = raw.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def dump_env(env: Dict[str,str]) -> str:
    # Reconstruct with original comment/blank line markers in order
    lines: List[Tuple[int,str]] = []
    for k, v in env.items():
        if k.startswith("#LINE:"):
            idx = int(k.split(":",1)[1])
            lines.append((idx, v))
        else:
            # Put non-marker lines at large index to append in natural order
            lines.append((10_000_000 + len(lines), f"{k}={v}"))
    lines.sort(key=lambda x: x[0])
    return "\n".join(x[1] for x in lines) + "\n"

def real_items(env: Dict[str,str]) -> Dict[str,str]:
    return {k:v for k,v in env.items() if not k.startswith("#LINE:")}

def parse_csv(v: str) -> List[str]:
    return [x.strip() for x in v.split(",") if x.strip()]

def normalize_bool(v: str) -> str:
    if v.lower() in ("1","true"): return "true"
    if v.lower() in ("0","false"): return "false"
    return v

def normalize_csv_symbols(v: str) -> str:
    items = parse_csv(v)
    items = [x.upper() for x in items]
    items = [x for x in items if x]  # no empties
    # warn later if they don’t match pattern; we still uppercase/dedupe/sort
    items = sorted(set(items))
    return ",".join(items)

def normalize_csv_uids(v: str) -> str:
    items = parse_csv(v)
    items = [re.sub(r"\D", "", x) for x in items]  # keep digits only
    items = [x for x in items if x]
    items = sorted(set(items))
    return ",".join(items)

def write_fixed(path: Path, env: Dict[str,str]):
    out = dump_env(env)
    fixed = Path(str(path) + ".fixed")
    fixed.write_text(out, encoding="utf-8")
    return fixed

def validate(env: Dict[str,str], errs: List[str], warns: List[str]):
    # Required core
    for k in ("TZ","RELAY_URL","RELAY_TOKEN"):
        if k not in env or env[k]=="":
            errs.append(f"{k} missing or empty")

    # Basic shapes
    if "TZ" in env and env["TZ"] and not RE_TZ.match(env["TZ"]):
        warns.append(f"TZ looks odd: '{env['TZ']}'")

    def check_url(name):
        if name in env and env[name]:
            if not RE_URL.match(env[name]):
                errs.append(f"{name} must be http(s) URL, got '{env[name]}'")

    for k in ("RELAY_URL","RELAY_BASE","DASHBOARD_RELAY_BASE"):
        check_url(k)

    if "RELAY_CORS_ALLOW" in env and env["RELAY_CORS_ALLOW"]:
        for u in parse_csv(env["RELAY_CORS_ALLOW"]):
            if not RE_URL.match(u):
                errs.append(f"RELAY_CORS_ALLOW item must be URL, got '{u}'")

    # Telegram combo
    if env.get("TELEGRAM_BOT_TOKEN","") and not env.get("TELEGRAM_CHAT_ID",""):
        errs.append("TELEGRAM_CHAT_ID required when TELEGRAM_BOT_TOKEN is set")

    # Bybit combo
    if env.get("BYBIT_API_KEY","") or env.get("BYBIT_API_SECRET",""):
        if not env.get("BYBIT_API_KEY","") or not env.get("BYBIT_API_SECRET",""):
            errs.append("Both BYBIT_API_KEY and BYBIT_API_SECRET must be set together")

    # Gate thresholds
    for k in ("REGIME_MIN_ADX","REGIME_MIN_ATR_PCT","REGIME_MAX_ATR_PCT"):
        if k in env and env[k] and not RE_FLOAT.match(env[k]):
            errs.append(f"{k} must be float")

    # TRADING_WINDOWS format
    if env.get("TRADING_WINDOWS",""):
        for rng in parse_csv(env["TRADING_WINDOWS"]):
            if "-" not in rng:
                errs.append(f"TRADING_WINDOWS bad chunk '{rng}'"); continue
            a,b = rng.split("-",1)
            if not RE_TIME.match(a) or not RE_TIME.match(b):
                errs.append(f"TRADING_WINDOWS times must be HH:MM, got '{rng}'")

    # Symbols / UIDs sanity
    for k in CSV_KEYS_SYMBOLS:
        if env.get(k,""):
            bad = [x for x in parse_csv(env[k]) if not RE_SYM.match(x)]
            if bad:
                warns.append(f"{k} unusual symbols: {bad}")
    for k in CSV_KEYS_UIDS:
        if env.get(k,""):
            bad = [x for x in parse_csv(env[k]) if not re.match(r"^\d+$", x)]
            if bad:
                errs.append(f"{k} has non-numeric entries: {bad}")

    # Risk guard ranges
    if env.get("RISK_PCT",""):
        if not RE_FLOAT.match(env["RISK_PCT"]):
            errs.append(f"RISK_PCT must be float percent, got '{env['RISK_PCT']}'")
        else:
            val = float(env["RISK_PCT"])
            if not (0.0 <= val <= 100.0):
                errs.append(f"RISK_PCT out of range [0,100], got {val}")
            elif val > 2.0:
                warns.append("RISK_PCT > 2.0% per trade. Confirm you mean that.")
    if env.get("DAILY_LOSS_CAP_PCT",""):
        if not RE_FLOAT.match(env["DAILY_LOSS_CAP_PCT"]):
            errs.append("DAILY_LOSS_CAP_PCT must be float percent")
        else:
            val = float(env["DAILY_LOSS_CAP_PCT"])
            if not (0.1 <= val <= 50.0):
                errs.append(f"DAILY_LOSS_CAP_PCT out of range [0.1,50], got {val}")

    for k in ("MAX_CONCURRENT","MAX_SYMBOL_TRADES","LEVERAGE","EXEC_COOLDOWN_SEC","PULLBACK_BPS","PNL_POLL_SEC","COOLDOWN_MIN"):
        if env.get(k,"") and not RE_INT.match(env[k]):
            errs.append(f"{k} must be integer")

    # Legacy vs new
    if env.get("DRY_RUN","") == "0" and env.get("EXEC_DRY_RUN","") == "true":
        warns.append("Both DRY_RUN=0 and legacy EXEC_DRY_RUN=true set. Disable legacy when using new executor.")

def fixup(env: Dict[str,str]) -> Tuple[Dict[str,str], List[str]]:
    changes = []
    fixed = dict(env)

    # Normalize booleans
    for k in BOOL_KEYS:
        if k in fixed and fixed[k] != "":
            canon = normalize_bool(fixed[k])
            if canon != fixed[k]:
                changes.append(f"{k}: '{fixed[k]}' → '{canon}'")
                fixed[k] = canon

    # Trim whitespace around comma lists and dedupe/sort
    for k in CSV_KEYS_SYMBOLS:
        if k in fixed:
            newv = normalize_csv_symbols(fixed[k])
            if newv != fixed[k]:
                changes.append(f"{k}: normalized CSV symbols")
                fixed[k] = newv
    for k in CSV_KEYS_UIDS:
        if k in fixed:
            newv = normalize_csv_uids(fixed[k])
            if newv != fixed[k]:
                changes.append(f"{k}: normalized CSV UIDs")
                fixed[k] = newv

    # Collapse multiple commas and stray spaces in RELAY_CORS_ALLOW
    if "RELAY_CORS_ALLOW" in fixed:
        items = [x.strip() for x in fixed["RELAY_CORS_ALLOW"].split(",") if x.strip()]
        newv = ",".join(dict.fromkeys(items))  # dedupe preserve order
        if newv != fixed["RELAY_CORS_ALLOW"]:
            changes.append("RELAY_CORS_ALLOW: normalized CSV")
            fixed["RELAY_CORS_ALLOW"] = newv

    # Uppercase symbols where we know they should be uppercase
    # Already done by normalize_csv_symbols, but also catch single-value keys if any appear later.

    # Strip trailing spaces for all non-marker lines
    for k in list(fixed.keys()):
        if not k.startswith("#LINE:"):
            v = fixed[k].strip()
            if v != fixed[k]:
                changes.append(f"{k}: trimmed whitespace")
                fixed[k] = v

    return fixed, changes

def main():
    args = [a for a in sys.argv[1:] if a != "--fix"]
    do_fix = ("--fix" in sys.argv[1:])
    env_path = Path(args[0]) if args else Path(".env")
    if not env_path.exists():
        print(f"[FAIL] {env_path} not found.")
        sys.exit(2)

    raw_env = load_env(env_path)
    env = real_items(raw_env)

    errs, warns = [], []
    validate(env, errs, warns)

    if errs:
        print("\n[FAIL] .env has problems:")
        for e in errs: print("  -", e)
    else:
        print("[OK] .env basic validation passed.")

    if warns:
        print("\n[WARNINGS]")
        for w in warns: print("  •", w)

    if do_fix:
        fixed_map, changes = fixup(raw_env)
        out = write_fixed(env_path, fixed_map)
        print(f"\n[FIX] wrote normalized file → {out}")
        if changes:
            print("\n[CHANGES]")
            for c in changes: print("  •", c)

    sys.exit(1 if errs else 0)

if __name__ == "__main__":
    main()
