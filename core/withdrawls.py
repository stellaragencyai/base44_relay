#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/withdraw.py — Allowlisted, approval-gated withdrawals via relay proxy.

Public:
  request_withdraw(coin, amount, address_label, account_uid="MAIN", policy=None, reason="...") -> req_id

Policy shape (subset used here):
{
  "enabled": true,
  "approval_required": true,
  "daily_cap_usd": 2000,
  "time_window_utc": {"start":"08:00","end":"20:00"},
  "allowlist": {
    "USDT": {
      "binance_main": {"address":"...", "chain":"TRX"},
      "cold_vault":   "0xABCDEF..."
    }
  }
}
"""

from __future__ import annotations
import os, json, time, datetime as dt
from typing import Optional, Dict, Any, Tuple

from core.decision_log import log_event
from core.config import settings
from tools.notifier_telegram import tg
from core import relay_client as rc
from core.approval_client import require_approval

STATE_PATH = settings.DIR_STATE / "withdraw_state.json"

class AllowlistError(Exception): pass
class WindowError(Exception): pass
class CapError(Exception): pass

def _read_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_state(obj: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass

def _utc_now() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def _in_window(win: dict) -> bool:
    if not win:
        return True
    start = str(win.get("start","00:00"))
    end   = str(win.get("end","23:59"))
    try:
        sh, sm = [int(x) for x in start.split(":")]
        eh, em = [int(x) for x in end.split(":")]
    except Exception:
        return True
    now = _utc_now().time()
    s = dt.time(sh, sm)
    e = dt.time(eh, em)
    if e >= s:
        return s <= now <= e
    # overnight window
    return now >= s or now <= e

def _resolve_allowlist(policy: dict, coin: str, label: str) -> Tuple[str, Optional[str]]:
    al = ((policy.get("allowlist") or {}).get(coin) or {})
    if label not in al:
        raise AllowlistError(f"label '{label}' not in allowlist for {coin}")
    target = al[label]
    if isinstance(target, dict):
        return str(target.get("address") or ""), (target.get("chain") or None)
    return str(target), None

def _usd_from_coin(coin: str, amount: float) -> float:
    # Rough: use ticker lastPrice if available or assume 1 for USDT
    if coin.upper() == "USDT":
        return float(amount)
    try:
        tk = rc.ticker(f"{coin.upper()}USDT")
        last = float(tk.get("lastPrice") or 0)
        return float(amount) * (last if last > 0 else 0.0)
    except Exception:
        return 0.0

def _check_caps(policy: dict, coin: str, amount: float) -> None:
    usd = _usd_from_coin(coin, amount)
    cap = float(policy.get("daily_cap_usd", 0) or 0.0)
    if cap <= 0:
        return
    st = _read_state()
    key = dt.datetime.utcnow().strftime("%Y-%m-%d")
    used = float(((st.get("days") or {}).get(key) or {}).get("usd", 0.0))
    if used + usd > cap + 1e-6:
        raise CapError(f"daily cap exceeded: used={used:.2f} + {usd:.2f} > {cap:.2f} USD")

def _note_caps(policy: dict, coin: str, amount: float) -> None:
    usd = _usd_from_coin(coin, amount)
    st = _read_state()
    key = dt.datetime.utcnow().strftime("%Y-%m-%d")
    days = st.get("days") or {}
    row = days.get(key) or {"usd": 0.0}
    row["usd"] = float(row.get("usd", 0.0)) + float(usd)
    days[key] = row
    st["days"] = days
    _write_state(st)

def request_withdraw(coin: str,
                     amount: float,
                     address_label: str,
                     *,
                     account_uid: str = "MAIN",
                     policy: Optional[dict] = None,
                     reason: str = "manual") -> str:
    pol = dict(policy or {})
    if not bool(pol.get("enabled", True)):
        raise AllowlistError("withdrawals disabled by policy")

    if not _in_window(pol.get("time_window_utc") or {}):
        raise WindowError("outside allowed withdrawal window")

    addr, chain = _resolve_allowlist(pol, coin.upper(), address_label)

    # caps
    _check_caps(pol, coin, float(amount))

    if bool(pol.get("approval_required", True)):
        rid = require_approval(
            action="withdraw",
            account_key=str(account_uid),
            reason=f"{reason}:{coin}:{amount}",
            ttl_sec=600,
            timeout_sec=int(os.getenv("APPROVAL_TIMEOUT_SEC", "180") or "180"),
            poll_sec=2.5
        )
        tg.safe_text(f"🔐 Approval OK • withdraw • req={rid}", quiet=True)

    # Relay proxy to Bybit /v5/asset/withdraw/create
    body: Dict[str, Any] = {
        "coin": coin.upper(),
        "chain": chain or "",               # empty lets exchange default
        "address": addr,
        "amount": f"{float(amount):.8f}",
        "timestamp": int(time.time() * 1000)
    }
    extra = {"subUid": account_uid} if account_uid and account_uid != "MAIN" else None
    res = rc.proxy("POST", "/v5/asset/withdraw/create", body=body, extra=extra)

    # Typical success contains result.withdrawId
    wid = ""
    try:
        wid = str(((res.get("result") or {}).get("withdrawId") or "")).strip()
    except Exception:
        wid = ""

    log_event("withdraw", "submit", "", account_uid, {"coin": coin, "amount": float(amount), "label": address_label, "chain": chain, "resp": res})
    _note_caps(pol, coin, float(amount))
    return wid or "requested"
