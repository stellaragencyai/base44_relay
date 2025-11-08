#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resolve missing sub UIDs in registry/sub_map.json using uid_book + uid_label.
- Creates a backup registry/sub_map.json.bak
- Validates that resolved UIDs look numeric
- Optional --check-only to preview changes without writing
- Optional --verify to confirm Bybit sees those UIDs in positions snapshot

Exit codes:
  0 = OK, wrote (or nothing to do)
  2 = Unresolved entries (labels not in uid_book or missing uid_label)
  3 = Verify failed: some enabled subs not visible from Bybit client
"""

from __future__ import annotations
import json, sys, argparse, re
from pathlib import Path

REG_PATH = Path("registry") / "sub_map.json"
UID_RE = re.compile(r"^\d{5,}$")  # Bybit subUid are numeric strings (5+ digits)

def load_submap(p: Path):
    data = json.loads(p.read_text(encoding="utf-8"))
    uid_book = data.get("uid_book") or {}
    subs = data.get("subs") or {}
    return data, uid_book, subs

def resolve_uids(data, uid_book, subs):
    changed = 0
    unresolved = []
    for code, cfg in subs.items():
        # leave MAIN alone
        if code.upper() == "MAIN":
            continue
        uid = (cfg.get("uid") or "").strip()
        label = (cfg.get("uid_label") or "").strip()

        if uid:
            # sanity check: numeric
            if not UID_RE.match(uid):
                unresolved.append((code, f"existing uid '{uid}' not numeric?"))
            continue

        if not label:
            unresolved.append((code, "no uid and no uid_label"))
            continue

        mapped = uid_book.get(label)
        if not mapped:
            unresolved.append((code, f"uid_label '{label}' not found in uid_book"))
            continue

        mapped = str(mapped).strip()
        if not UID_RE.match(mapped):
            unresolved.append((code, f"mapped uid '{mapped}' not numeric?"))
            continue

        cfg["uid"] = mapped
        changed += 1

    return changed, unresolved

def write_with_backup(p: Path, data) -> Path:
    bak = p.with_suffix(".json.bak")
    bak.write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return bak

def verify_visibility(subs) -> list[str]:
    """
    Ask Bybit which subs are visible via positions snapshot.
    Returns list of enabled subs (by uid) that are NOT visible.
    """
    try:
        from core.bybit_client import Bybit
    except Exception as e:
        print(f"[WARN] verify skipped: can't import core.bybit_client: {e}")
        return []

    by = Bybit()
    try:
        by.sync_time()
    except Exception:
        pass

    ok, data, err = by.get_positions(category="linear")
    if not ok:
        print(f"[WARN] verify skipped: positions fetch failed: {err}")
        return []

    visible = set()
    for it in (data.get("result", {}) or {}).get("list", []) or []:
        sub = str(it.get("accountId") or it.get("subUid") or "MAIN")
        visible.add(sub)

    missing = []
    for code, cfg in subs.items():
        if code.upper() == "MAIN":
            continue
        if not cfg.get("enabled", True):
            continue
        uid = str(cfg.get("uid") or "").strip()
        if uid and uid not in visible:
            missing.append(uid)
    return missing

def main():
    ap = argparse.ArgumentParser(description="Resolve & verify sub UIDs from sub_map.json")
    ap.add_argument("--check-only", action="store_true", help="Preview changes; don't write")
    ap.add_argument("--verify", action="store_true", help="Verify visibility via Bybit positions")
    args = ap.parse_args()

    p = Path(REG_PATH)
    if not p.exists():
        print(f"[ERR] Missing {REG_PATH}")
        sys.exit(1)

    data, uid_book, subs = load_submap(p)
    changed, unresolved = resolve_uids(data, uid_book, subs)

    if changed:
        print(f"[OK] Will fill {changed} uid(s) from uid_book via uid_label.")
    else:
        print("[OK] No uid fields to fill.")

    if unresolved:
        print("[WARN] Unresolved entries:")
        for code, msg in unresolved:
            print(f"  - {code}: {msg}")

    if args.check_only:
        print("[INFO] --check-only used; not writing changes.")
        if unresolved:
            sys.exit(2)
        sys.exit(0)

    if changed:
        bak = write_with_backup(p, data)
        print(f"[OK] Updated {REG_PATH}. Backup saved as {bak.name}")

    # Optional verification against live snapshot
    if args.verify:
        missing = verify_visibility(subs)
        if missing:
            print("[WARN] Enabled subs not visible in Bybit positions snapshot:", ", ".join(missing))
            sys.exit(3)

    if unresolved:
        sys.exit(2)

    print("[DONE] sub_map.json looks good.")
    sys.exit(0)

if __name__ == "__main__":
    main()
