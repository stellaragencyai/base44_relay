from __future__ import annotations
import os, json, datetime, glob
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv
getcontext().prec = 28

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

PNL_DIR = Path(os.getenv("PNL_LOG_DIR", str(ROOT / "logs" / "pnl")))
PNL_DIR.mkdir(parents=True, exist_ok=True)

def _today_key(dt: Optional[datetime.datetime]=None) -> str:
    d = dt or datetime.datetime.now()
    return d.strftime("%Y-%m-%d")

def _read_latest_row() -> Optional[dict]:
    # Prefer today’s file if rotated; else fallback to daily_pnl_log.jsonl
    today_glob = list(PNL_DIR.glob(f"{_today_key()}*.jsonl"))
    candidates = [PNL_DIR / "daily_pnl_log.jsonl"] + today_glob
    newest = None
    newest_mtime = -1
    for p in candidates:
        if p.exists() and p.stat().st_mtime > newest_mtime:
            newest, newest_mtime = p, p.stat().st_mtime
    if not newest or not newest.exists():
        return None
    # read last non-empty line
    last = None
    with newest.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if s:
                last = s
    if not last:
        return None
    try:
        return json.loads(last)
    except Exception:
        return None

def _baseline_for_today() -> Optional[Dict[str, str]]:
    # Walk the file and pick the first row of today
    today_files = sorted(glob.glob(str(PNL_DIR / f"{_today_key()}*.jsonl")))
    candidates = [PNL_DIR / "daily_pnl_log.jsonl"] + [Path(p) for p in today_files]
    for p in candidates:
        if not Path(p).exists():
            continue
        try:
            with open(p, "r", encoding="utf-8") as fh:
                for line in fh:
                    js = json.loads(line.strip())
                    if "data" in js:
                        return js["data"]
        except Exception:
            continue
    return None

def get_summary_payload() -> Dict[str, Any]:
    last = _read_latest_row()
    base = _baseline_for_today()
    cur_total = Decimal("0")
    base_total = Decimal("0")
    if last and "data" in last and "total" in last["data"]:
        try:
            cur_total = Decimal(str(last["data"]["total"]))
        except Exception:
            cur_total = Decimal("0")
    if base and "total" in base:
        try:
            base_total = Decimal(str(base["total"]))
        except Exception:
            base_total = Decimal("0")
    dd_pct = float(((base_total - cur_total) / base_total * Decimal("100")) if base_total > 0 else Decimal("0"))
    return {
        "ts": last.get("ts") if last else None,
        "total": float(cur_total),
        "baseline_total": float(base_total),
        "dd_pct": max(0.0, dd_pct),
        "by_account": (last or {}).get("data", {}),
    }
