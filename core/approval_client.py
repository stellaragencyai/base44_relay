#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# core/approval_client.py
"""
Approval Client — require human approval before sensitive ops.

Usage
  from core.approval_client import require_approval, ApprovalError
  require_approval(
      action="disable_breaker",
      account_key="sub:260417078",
      reason="maintenance",
      ttl_sec=900,
      timeout_sec=120
  )

Env (.env)
  APPROVAL_SERVICE_URL=http://127.0.0.1:5055
  APPROVAL_SHARED_SECRET=the_same_secret_as_service
  APPROVAL_TIMEOUT_SEC=180         # default overall wait
  APPROVAL_POLL_SEC=2.5            # poll cadence
  APPROVAL_DEV_AUTO=1              # if set and service/secret missing → auto-approve in dev
"""

from __future__ import annotations
import os, json, time, hmac, hashlib
from typing import Optional, Dict, Any, Tuple

# requests is nice, but don't make it a hard dependency in case someone forgot to install it
try:
    import requests  # type: ignore
    _HAS_REQUESTS = True
except Exception:
    _HAS_REQUESTS = False
    import urllib.request, urllib.error  # fallback

SERVICE = (os.getenv("APPROVAL_SERVICE_URL", "").strip() or "http://127.0.0.1:5055").rstrip("/")
SECRET  = os.getenv("APPROVAL_SHARED_SECRET", "") or ""
DEF_TIMEOUT = int(os.getenv("APPROVAL_TIMEOUT_SEC", "180") or 180)
DEF_POLL    = float(os.getenv("APPROVAL_POLL_SEC", "2.5") or 2.5)
DEV_AUTO    = (os.getenv("APPROVAL_DEV_AUTO", "1").strip().lower() in {"1","true","yes","on"})

# ---------- errors ----------
class ApprovalError(Exception): ...
class ApprovalDenied(ApprovalError): ...
class ApprovalExpired(ApprovalError): ...
class ApprovalTimeout(ApprovalError): ...

# ---------- http helpers ----------
def _hmac_hex(key: str, raw: bytes) -> str:
    return hmac.new(key.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def _headers(raw_body: Optional[bytes] = None) -> Dict[str, str]:
    h = {"Content-Type": "application/json"}
    if SECRET:
        h["Authorization"] = f"Bearer {SECRET}"
        if raw_body is not None:
            h["X-Sign"] = _hmac_hex(SECRET, raw_body)
    return h

def _post_json(path: str, obj: Dict[str, Any]) -> Dict[str, Any]:
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    url = f"{SERVICE}{path}"
    if _HAS_REQUESTS:
        r = requests.post(url, data=raw, headers=_headers(raw), timeout=12)
        if r.status_code >= 400:
            raise ApprovalError(f"service error {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}
    # urllib fallback
    req = urllib.request.Request(url=url, method="POST", data=raw, headers=_headers(raw))
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            txt = resp.read().decode("utf-8", "replace")
            return json.loads(txt) if txt else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace") if hasattr(e, "read") else str(e)
        raise ApprovalError(f"service error {e.code}: {body[:200]}")

def _get(path: str) -> Dict[str, Any]:
    url = f"{SERVICE}{path}"
    if _HAS_REQUESTS:
        r = requests.get(url, headers=_headers(None), timeout=10)
        if r.status_code >= 400:
            raise ApprovalError(f"service error {r.status_code}: {r.text[:200]}")
        return r.json() if r.text else {}
    # urllib fallback
    req = urllib.request.Request(url=url, method="GET", headers=_headers(None))
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            txt = resp.read().decode("utf-8", "replace")
            return json.loads(txt) if txt else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace") if hasattr(e, "read") else str(e)
        raise ApprovalError(f"service error {e.code}: {body[:200]}")

# ---------- API shims (supports two server styles) ----------
def _create_request_legacy(action: str, account_key: str, reason: str, ttl_sec: int) -> str:
    # Your existing endpoints: POST /request -> {"id": "..."}
    js = _post_json("/request", {"action": action, "account": account_key, "reason": reason, "ttl_sec": int(ttl_sec)})
    rid = (js.get("id") or js.get("request_id") or "").strip()
    if not rid:
        raise ApprovalError("service returned no id (legacy)")
    return rid

def _status_request_legacy(req_id: str) -> str:
    # GET /status/{id} -> {"record":{"status":"pending|approved|denied|expired"}}
    st = _get(f"/status/{req_id}")
    rec = (st.get("record") or {})
    status = str(rec.get("status", "")).lower()
    return status

def _create_request_v1(action: str, account_key: str, reason: str, ttl_sec: int) -> str:
    # Alternative endpoints: POST /v1/approvals -> {"request_id":"..."}
    js = _post_json("/v1/approvals", {"action": action, "account_key": account_key, "reason": reason, "ttl_sec": int(ttl_sec)})
    rid = (js.get("request_id") or js.get("id") or "").strip()
    if not rid:
        raise ApprovalError("service returned no request_id (v1)")
    return rid

def _status_request_v1(req_id: str) -> str:
    # GET /v1/approvals/{id} -> {"status":"pending|approved|rejected|expired"}
    st = _get(f"/v1/approvals/{req_id}")
    status = str(st.get("status", "")).lower()
    if status == "rejected":
        status = "denied"
    return status

def _detect_mode() -> str:
    # If the server speaks /v1, prefer it; otherwise assume legacy.
    try:
        _get("/v1/ping")
        return "v1"
    except Exception:
        return "legacy"

# ---------- dev stub ----------
def _dev_auto_require(action: str, account_key: str, reason: str, ttl_sec: int, timeout_sec: int, poll_sec: float) -> str:
    # If we’re missing SERVICE or SECRET and DEV_AUTO is true, auto-approve.
    rid = f"dev-{int(time.time()*1000)}"
    t0 = time.time()
    # pretend we’re polling and a human nodded
    while time.time() - t0 < min(1.2, timeout_sec):
        time.sleep(min(0.25, poll_sec))
    return rid

# ---------- public API ----------
def require_approval(
    *,
    action: str,
    account_key: str,
    reason: str = "",
    ttl_sec: int = 900,
    timeout_sec: int = None,       # type: ignore
    poll_sec: float = None         # type: ignore
) -> str:
    """
    Create an approval request and block until approved/denied/expired/timeout.
    Returns request_id if approved. Raises otherwise.

    Raises:
      ApprovalDenied, ApprovalExpired, ApprovalTimeout, ApprovalError
    """
    timeout_s = int(DEF_TIMEOUT if timeout_sec is None else timeout_sec)
    poll_s = float(DEF_POLL if poll_sec is None else poll_sec)

    # Dev convenience: if SERVICE or SECRET is missing and DEV_AUTO is enabled, auto-approve.
    if (not SERVICE or not SECRET) and DEV_AUTO:
        return _dev_auto_require(action, account_key, reason, int(ttl_sec), timeout_s, poll_s)

    if not SECRET:
        # In prod, we require a secret to prevent randoms from spamming approvals.
        raise ApprovalError("APPROVAL_SHARED_SECRET missing; cannot request approval")

    mode = _detect_mode()
    if mode == "v1":
        create_fn, status_fn = _create_request_v1, _status_request_v1
    else:
        create_fn, status_fn = _create_request_legacy, _status_request_legacy

    rid = create_fn(action, account_key, reason, int(ttl_sec))

    deadline = time.time() + max(1, timeout_s)
    last_status = None

    while time.time() < deadline:
        status = status_fn(rid)  # pending|approved|denied|expired
        if status != last_status:
            # keep stdout noise minimal but useful
            print(f"[approval] {rid} → {status}")
            last_status = status

        if status == "approved":
            return rid
        if status in {"denied", "rejected"}:
            raise ApprovalDenied(f"approval denied for {action} {account_key}")
        if status == "expired":
            raise ApprovalExpired(f"approval expired for {action} {account_key}")

        time.sleep(max(0.3, poll_s))

    raise ApprovalTimeout(f"approval timeout after {timeout_s}s")
