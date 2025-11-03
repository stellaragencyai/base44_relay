# registry/bybit_subuid_harvester.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bybit Sub-UID Harvester

- Queries all sub-members from Bybit (via your relay client) and writes them to CSV.
- Robust import discovery for base44_client/base44_registry no matter where you run it from.
- Handles pagination (cursor-based) until all results are fetched.
- Creates /registry directory if missing and writes: sub_uids.csv
- Optionally updates/merges registry via base44_registry.ensure_synced() if available.

Environment (optional):
- BASE44_CORE_DIR: absolute path to your /core directory (import hint)
- BYBIT_SUBMEMBERS_PAGE_SIZE: page size for the endpoint (default 50)
"""

import os
import sys
import csv
import json
import importlib
from pathlib import Path

CUR = Path(__file__).resolve()
REG_DIR = CUR.parent
CSV_PATH = REG_DIR / "sub_uids.csv"

PAGE_SIZE = int(os.getenv("BYBIT_SUBMEMBERS_PAGE_SIZE", "50"))
BASE44_CORE_DIR = os.getenv("BASE44_CORE_DIR", "")

# --------------------------------------------------------------------------------------
# Import helpers
# --------------------------------------------------------------------------------------
def _import_module_with_fallbacks(mod_name: str):
    """
    Try to import a module after augmenting sys.path with reasonable candidates.
    """
    candidates = []
    if BASE44_CORE_DIR:
        candidates.append(Path(BASE44_CORE_DIR))

    repo_root = CUR.parents[1]          # <repo> that should contain /core and /registry
    core_in_repo = repo_root / "core"
    core_next_to_registry = REG_DIR / "core"

    # Search order: env hint, repo/core, sibling/core, repo root, registry
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

# Load base44_client and registry, with graceful fallbacks
try:
    _client = _import_module_with_fallbacks("base44_client")
except Exception as e:
    raise SystemExit(f"[harvester] cannot import base44_client: {e}")

try:
    _registry = _import_module_with_fallbacks("base44_registry")
except Exception:
    _registry = None  # not fatal; ensure_synced is optional

# --------------------------------------------------------------------------------------
# Safe wrappers
# --------------------------------------------------------------------------------------
def ensure_dir(path: Path) -> None:
    """
    Create directory if missing. Prefer client's ensure_dir if available.
    """
    fn = getattr(_client, "ensure_dir", None)
    if callable(fn):
        try:
            fn(str(path))
            return
        except Exception:
            pass
    path.mkdir(parents=True, exist_ok=True)

def ensure_synced() -> None:
    """
    Optionally merge/refresh registry artifacts after harvesting.
    """
    if _registry is None:
        return
    fn = getattr(_registry, "ensure_synced", None)
    if callable(fn):
        try:
            fn()
        except Exception as e:
            print(f"[harvester] ensure_synced failed: {e}")

def _safe_proxy(method: str, path: str, *, params=None, data=None, json_body=None):
    """
    Call base44_client.proxy with flexible signatures:
      proxy(method, path, params=?)
      proxy(method, path, params=?, data=?)
      proxy(method, path, params=?, json=?)
    """
    fn = getattr(_client, "proxy", None)
    if fn is None:
        raise RuntimeError("base44_client.proxy not found")

    attempts = [
        lambda: fn(method, path, params=params or {}),
        lambda: fn(method, path, params=params or {}, data=data or {}),
        lambda: fn(method, path, params=params or {}, json=json_body or {}),
        lambda: fn(method, path),  # bare minimum
    ]
    last_err = None
    for attempt in attempts:
        try:
            return attempt()
        except TypeError as te:
            last_err = te
            continue
    # If we reached here, signature didn't match
    raise RuntimeError(f"proxy signature mismatch: {last_err}")

# --------------------------------------------------------------------------------------
# Bybit pagination
# --------------------------------------------------------------------------------------
def fetch_all_sub_members():
    """
    Fetch every page from /v5/user/query-sub-members using cursor-based pagination.
    Returns a list of raw dicts from Bybit.
    """
    results = []
    cursor = ""
    while True:
        params = {"limit": PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor

        body = _safe_proxy("GET", "/v5/user/query-sub-members", params=params)
        if not isinstance(body, dict) or (body.get("retCode") not in (0, "0")):
            rc = None if not isinstance(body, dict) else body.get("retCode")
            msg = None if not isinstance(body, dict) else body.get("retMsg")
            raise SystemExit(f"Bybit error retCode={rc} retMsg={msg}")

        result = body.get("result") or {}
        lst = result.get("list") or []
        results.extend(lst)

        cursor = result.get("nextPageCursor") or result.get("cursor") or ""
        if not cursor or len(lst) == 0:
            break
    return results

# --------------------------------------------------------------------------------------
# Extraction + write
# --------------------------------------------------------------------------------------
def extract_uids(records):
    """
    Extract UID and a couple of optional fields if present.
    We only guarantee sub_uid in the CSV to be safe.
    """
    extracted = []
    for item in records:
        # Common keys differ per account type/version; try several
        uid = item.get("uid") or item.get("memberId") or item.get("subMemberId") or item.get("userId")
        if not uid:
            continue
        rec = {"sub_uid": str(uid)}
        # Optional extras (write only if available; we don't rely on them)
        for k in ("userName", "nickName", "status", "createdTime"):
            if k in item and item.get(k) is not None:
                rec[k] = str(item.get(k))
        extracted.append(rec)
    return extracted

def write_csv(rows):
    """
    Write rows to sub_uids.csv.
    Always includes 'sub_uid'. Adds optional columns present in the data set.
    """
    ensure_dir(REG_DIR)

    # Determine all optional keys that appear
    fieldnames = ["sub_uid"]
    optional_keys = set()
    for r in rows:
        for k in r.keys():
            if k != "sub_uid":
                optional_keys.add(k)
    fieldnames += sorted(optional_keys)

    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    print(f"[harvester] starting → writing to {CSV_PATH}")
    records = fetch_all_sub_members()
    uids = extract_uids(records)
    write_csv(uids)
    ensure_synced()
    print(f"[harvester] done. {len(uids)} sub_uids written to {CSV_PATH}")

if __name__ == "__main__":
    main()
