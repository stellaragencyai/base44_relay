#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# core/approval_client.py
"""
Approval Client — require friend approval before sensitive ops.

Usage
  from core.approval_client import require_approval, ApprovalError
  require_approval(action="disable_breaker", account_key="sub:260417078", reason="maintenance", ttl_sec=900, timeout_sec=120)

Env (.env)
  APPROVAL_SERVICE_URL=http://127.0.0.1:5055
  APPROVAL_SHARED_SECRET=the_same_secret_as_service
"""

from __future__ import annotations
import os, json, time, hmac, hashlib
from typing import Optional, Dict, Any, Tuple
import requests

SERVICE = os.getenv("APPROVAL_SERVICE_URL", "http://127.0.0.1:5055").rstrip("/")
SECRET  = os.getenv("APPROVAL_SHARED_SECRET", "")

class ApprovalError(Exception): pass
class ApprovalDenied(ApprovalError): pass
class ApprovalExpired(ApprovalError): pass
class ApprovalTimeout(ApprovalError): pass

def _hmac_hex(key: str, raw: bytes) -> str:
    import hashlib, hmac
    return hmac.new(key.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def _post_json(path: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {SECRET}",
        "Content-Type": "application/json",
        "X-Sign": _hmac_hex(SECRET, raw),
    }
    r = requests.post(f"{SERVICE}{path}", data=raw, headers=headers, timeout=12)
    if r.status_code >= 400:
        raise ApprovalError(f"service error {r.status_code}: {r.text[:200]}")
    return r.json()

def _get(path: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {SECRET}"}
    r = requests.get(f"{SERVICE}{path}", headers=headers, timeout=10)
    if r.status_code >= 400:
        raise ApprovalError(f"service error {r.status_code}: {r.text[:200]}")
    return r.json()

def require_approval(*, action: str, account_key: str, reason: str = "", ttl_sec: int = 900, timeout_sec: int = 180, poll_sec: float = 2.5) -> str:
    """
    Create an approval request and block until approved/denied/expired/timeout.
    Returns the request id if approved. Raises otherwise.
    """
    if not SECRET:
        raise ApprovalError("APPROVAL_SHARED_SECRET missing; cannot request approval")

    payload = {"action": action, "account": account_key, "reason": reason, "ttl_sec": int(ttl_sec)}
    js = _post_json("/request", payload)
    rid = js.get("id")
    if not rid:
        raise ApprovalError("service returned no id")

    deadline = time.time() + max(1, timeout_sec)
    last_status = None

    while time.time() < deadline:
        st = _get(f"/status/{rid}")
        rec = (st.get("record") or {})
        status = rec.get("status")
        if status != last_status:
            print(f"[approval] {rid} → {status}")
            last_status = status

        if status == "approved":
            return rid
        if status == "denied":
            raise ApprovalDenied(f"approval denied for {action} {account_key}")
        if status == "expired":
            raise ApprovalExpired(f"approval expired for {action} {account_key}")

        time.sleep(max(0.5, float(poll_sec)))

    raise ApprovalTimeout(f"approval timeout after {timeout_sec}s")
