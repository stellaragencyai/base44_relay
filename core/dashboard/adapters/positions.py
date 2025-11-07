from __future__ import annotations
import os, requests
from typing import Dict, Any, List
from dotenv import load_dotenv
from pathlib import Path

from core.bybit_client import Bybit

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

RELAY_URL = (os.getenv("RELAY_URL","") or "").rstrip("/")
RELAY_TOKEN = os.getenv("RELAY_TOKEN","") or os.getenv("RELAY_SECRET","")
SUB_UIDS = [s.strip() for s in (os.getenv("SUB_UIDS","") or "").split(",") if s.strip()]

def _hdrs() -> Dict[str,str]:
    h = {"Content-Type":"application/json"}
    if RELAY_TOKEN:
        h["Authorization"] = f"Bearer {RELAY_TOKEN}"
        h["x-relay-token"] = RELAY_TOKEN
    return h

def _via_relay_positions(member_id: str | None) -> List[dict]:
    if not RELAY_URL:
        return []
    url = f"{RELAY_URL}/bybit/positions"
    params = {}
    if member_id:
        params["memberId"] = member_id
    try:
        r = requests.get(url, headers=_hdrs(), params=params, timeout=8)
        r.raise_for_status()
        js = r.json() or []
        return js if isinstance(js, list) else []
    except Exception:
        return []

def _via_client_main() -> List[dict]:
    by = Bybit()
    ok, data, err = by.get_positions(category="linear")
    if not ok:
        return []
    return (data.get("result") or {}).get("list") or []

def _map_row(p: dict, account: str) -> dict:
    # normalize a few standard fields
    return {
        "account": account,
        "symbol": p.get("symbol") or "",
        "side": p.get("side") or "",
        "size": p.get("size") or p.get("qty") or "0",
        "avgPrice": p.get("avgPrice") or p.get("entryPrice") or "",
        "unrealizedPnl": p.get("unrealisedPnl") or p.get("unrealizedPnl") or "",
        "positionIdx": p.get("positionIdx") or 0,
        "leverage": p.get("leverage") or "",
    }

def get_positions_payload() -> Dict[str, Any]:
    rows: List[dict] = []

    # MAIN
    for p in _via_client_main():
        rows.append(_map_row(p, "main"))

    # SUBS via Relay if available
    for uid in SUB_UIDS:
        plist = _via_relay_positions(uid) if RELAY_URL else []
        for p in plist:
            rows.append(_map_row(p, f"sub:{uid}"))

    return {"rows": rows}
