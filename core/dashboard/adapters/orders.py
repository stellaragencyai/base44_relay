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

def _main_open_orders() -> List[dict]:
    by = Bybit()
    ok, data, err = by.get_open_orders(category="linear", openOnly=True)
    if not ok:
        return []
    return (data.get("result") or {}).get("list") or []

def _relay_open_orders(member_id: str) -> List[dict]:
    if not RELAY_URL:
        return []
    try:
        r = requests.get(f"{RELAY_URL}/bybit/orders/open", headers=_hdrs(), params={"memberId": member_id}, timeout=8)
        r.raise_for_status()
        js = r.json() or []
        return js if isinstance(js, list) else []
    except Exception:
        return []

def _is_reduce_only(o: dict) -> bool:
    v = str(o.get("reduceOnly", "")).lower()
    return v in ("true","1")

def _is_limit(o: dict) -> bool:
    return (o.get("orderType") or "").lower() == "limit"

def get_orders_payload() -> Dict[str, Any]:
    rows: List[dict] = []

    def collect(account: str, lst: List[dict]):
        for o in lst:
            try:
                if not _is_limit(o) or not _is_reduce_only(o):
                    continue
                rows.append({
                    "account": account,
                    "symbol": o.get("symbol"),
                    "side": o.get("side"),
                    "qty": o.get("qty"),
                    "price": o.get("price"),
                    "orderId": o.get("orderId"),
                    "orderLinkId": o.get("orderLinkId"),
                    "timeInForce": o.get("timeInForce"),
                })
            except Exception:
                continue

    collect("main", _main_open_orders())
    for uid in SUB_UIDS:
        collect(f"sub:{uid}", _relay_open_orders(uid))

    return {"rows": rows}
