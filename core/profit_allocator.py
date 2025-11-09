#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/profit_allocator.py — Profit router with approvals and relay integration.

What it does
- Reads per-account profit allocation policy (JSON).
- Tracks last processed time per account; sums realized PnL since last checkpoint.
- Splits profit by envelopes (treasury/ops/tax/rd/risk_buffer) with min-transfer floor.
- Stages actions:
    • withdraw (approval-gated, allowlisted) via Bybit /v5/asset/withdraw/create
    • internal notes (placeholders for inter-account moves; logged for now)
- Dry-run by default. Emits Telegram previews and decision logs.

Env
  PROFIT_POLICY_PATH=./registry/profit_policy.json
  PROFIT_ALLOC_DRY_RUN=true
  PROFIT_ALLOC_INTERVAL_SEC=300
  HTTP_TIMEOUT_S=15
"""

from __future__ import annotations
import os, json, time, math
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from core.logger import get_logger
from core.decision_log import log_event
from tools.notifier_telegram import tg
from core.config import settings
from core import relay_client as rc
from core.withdraw import request_withdraw, AllowlistError, WindowError, CapError

log = get_logger("core.profit_allocator")

STATE_FILE = (settings.DIR_STATE / "profit_alloc_state.json")
POLICY_PATH = Path(os.getenv("PROFIT_POLICY_PATH", "./registry/profit_policy.json"))
DRY_RUN = (os.getenv("PROFIT_ALLOC_DRY_RUN", "true").strip().lower() in {"1","true","yes","on"})

def _now_ms() -> int:
    return int(time.time() * 1000)

def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_json(path: Path, obj: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass

def _load_policy() -> dict:
    pol = _read_json(POLICY_PATH)
    if not pol:
        raise RuntimeError(f"profit policy not found or empty: {POLICY_PATH}")
    return pol

def _state() -> dict:
    s = _read_json(STATE_FILE)
    s.setdefault("accounts", {})  # {uid: {"last_ms": int}}
    return s

def _save_state(s: dict) -> None:
    _write_json(STATE_FILE, s)

def _merge_account(defaults: dict, acc_node: Optional[dict]) -> dict:
    out = dict(defaults or {})
    out.update(acc_node or {})
    return out

def _is_numeric(uid: str) -> bool:
    try:
        int(str(uid))
        return True
    except Exception:
        return False

def _fetch_closed_pnl_usd(uid: str, since_ms: int) -> Tuple[float, int]:
    """
    Sum realized closed PnL (USD) for linear per account since since_ms.
    Returns (pnl_usd, last_seen_ms). Uses relay proxy; subUid routing if uid is numeric.
    """
    total = 0.0
    last_seen = since_ms
    params = {"category": "linear", "limit": 200, "startTime": since_ms}
    extra = {"subUid": uid} if _is_numeric(uid) else None

    # Single page is usually enough; loop cautiously if needed.
    for _ in range(3):
        body = rc.proxy("GET", "/v5/position/closed-pnl", params=params, extra=extra)
        items = ((body.get("result") or {}).get("list") or [])
        if not items:
            break
        for it in items:
            try:
                pnl = float(it.get("closedPnl", 0) or 0)
                ts = int(it.get("createdTime", it.get("updatedTime", 0)) or 0)
                total += pnl
                last_seen = max(last_seen, ts)
            except Exception:
                continue
        # stop if fewer than limit came back
        if len(items) < int(params["limit"]):
            break
        params["startTime"] = last_seen + 1

    return float(total), int(last_seen)

def _allocations_from_policy(net_profit_usd: float, policy: dict) -> List[Tuple[str, float]]:
    pct_map = dict(policy.get("allocation_pct") or {})
    out: List[Tuple[str, float]] = []
    for bucket, pct in pct_map.items():
        try:
            val = max(0.0, net_profit_usd * float(pct) / 100.0)
        except Exception:
            val = 0.0
        if val > 0:
            out.append((bucket, val))
    return out

def _format_preview(uid: str, coin: str, profit: float, actions: List[dict]) -> str:
    lines = [f"💸 Profit Allocation • acct={uid} • coin={coin} • profit={profit:.2f} {coin}"]
    for a in actions:
        if a["type"] == "withdraw":
            lines.append(f"  ↳ withdraw {a['amount']:.2f} {coin} → {a['address_label']} ({a.get('chain','?')})")
        else:
            lines.append(f"  ↳ note {a['bucket']}: {a['amount_usd']:.2f} USD")
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    lines.append(f"mode={mode}")
    return "\n".join(lines)

def plan_actions(uid: str, profit_usd: float, pol: dict) -> List[dict]:
    """
    Build actions for this account based on policy. For simplicity:
    - All buckets generate a 'note' action (internal accounting).
    - If withdrawals.enabled and bucket == 'treasury' (typical), stage a withdraw using policy.withdrawals.allowlist.
    """
    coin = str(pol.get("coin") or "USDT").upper()
    min_usd = float(pol.get("min_transfer_usd", 25.0) or 25.0)
    if profit_usd < max(0.0, min_usd):
        return []

    actions: List[dict] = []
    allocs = _allocations_from_policy(profit_usd, pol)
    wd = pol.get("withdrawals") or {}
    wd_enabled = bool(wd.get("enabled", True))
    allowlist = ((wd.get("allowlist") or {}).get(coin) or {})
    # pick the first allowlist label as default sink
    default_label = next(iter(allowlist.keys()), None)

    for bucket, usd in allocs:
        actions.append({"type": "note", "bucket": bucket, "amount_usd": float(usd)})
        # convention: withdraw bucket is 'treasury' unless policy says otherwise
        if wd_enabled and bucket == "treasury" and default_label:
            target = allowlist.get(default_label)
            address = target.get("address") if isinstance(target, dict) else str(target)
            chain = target.get("chain") if isinstance(target, dict) else None
            actions.append({
                "type": "withdraw",
                "coin": coin,
                "amount": float(usd),
                "address_label": default_label,
                "address": address,
                "chain": chain,
                "account_uid": uid,
                "policy_withdraw": wd
            })
    return actions

def execute_actions(uid: str, actions: List[dict]) -> None:
    for a in actions:
        if a["type"] == "note":
            log_event("allocator", "bucket_note", "", uid, {"bucket": a["bucket"], "amount_usd": a["amount_usd"]})
            continue
        if a["type"] == "withdraw":
            coin = a["coin"]; amt = float(a["amount"])
            label = a["address_label"]; wdpol = a.get("policy_withdraw") or {}
            try:
                if DRY_RUN:
                    tg.safe_text(f"🧪 DRY • would withdraw {amt:.2f} {coin} to {label} (uid={uid})", quiet=True)
                    log_event("allocator","withdraw_dry","",uid,{"coin":coin,"amount":amt,"label":label})
                    continue
                reqid = request_withdraw(coin, amt, label, account_uid=uid, policy=wdpol, reason="profit_allocation")
                tg.safe_text(f"✅ Withdraw requested • {amt:.2f} {coin} • label={label} • uid={uid} • req={reqid}", quiet=True)
                log_event("allocator","withdraw_ok","",uid,{"coin":coin,"amount":amt,"label":label,"req":reqid})
            except (AllowlistError, WindowError, CapError) as e:
                tg.safe_text(f"❌ Withdraw blocked • uid={uid} • {e}", quiet=True)
                log_event("allocator","withdraw_block","",uid,{"err":str(e)}, level="warning")
            except Exception as e:
                tg.safe_text(f"❌ Withdraw error • uid={uid} • {e}", quiet=True)
                log_event("allocator","withdraw_error","",uid,{"err":str(e)}, level="error")

def run_once() -> None:
    pol_root = _load_policy()
    defaults = pol_root.get("defaults") or {}
    accounts = pol_root.get("accounts") or {}

    st = _state()
    st_accts = st["accounts"]

    # derive known UIDs: explicit keys + MAIN if not present
    known = set(accounts.keys()) | {"MAIN"}

    for uid in sorted(known):
        merged = _merge_account(defaults, accounts.get(uid))
        last_ms = int((st_accts.get(uid) or {}).get("last_ms", 0))
        if last_ms == 0:
            # first boot: set checkpoint and skip historical allocation
            st_accts[uid] = {"last_ms": _now_ms()}
            _save_state(st)
            log_event("allocator","bootstrap_checkpoint","",uid,{"ms":st_accts[uid]["last_ms"]})
            continue

        pnl_usd, new_last = _fetch_closed_pnl_usd(uid, last_ms)
        if pnl_usd <= 0:
            # still bump last to avoid reprocessing same rows forever
            st_accts[uid]["last_ms"] = max(last_ms, new_last)
            _save_state(st)
            continue

        actions = plan_actions(uid, pnl_usd, merged)
        if not actions:
            # nothing above min threshold
            st_accts[uid]["last_ms"] = max(last_ms, new_last)
            _save_state(st)
            continue

        preview = _format_preview(uid, merged.get("coin","USDT"), pnl_usd, actions)
        tg.safe_text(preview, quiet=True)
        log_event("allocator","plan", "", uid, {"profit_usd": pnl_usd, "actions": actions})

        execute_actions(uid, actions)

        # store checkpoint
        st_accts[uid]["last_ms"] = max(last_ms, new_last)
        _save_state(st)

if __name__ == "__main__":
    run_once()
