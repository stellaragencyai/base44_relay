#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
services/approval_service.py — minimal human-in-the-loop approvals

Supports two API styles:

Legacy (matches your existing client):
  POST /request                    -> {"id": "<req_id>"}
     body: {"action","account","reason","ttl_sec"}
  GET  /status/<id>                -> {"record":{"id","status","reason","account","action","ts","ttl_sec","actor"}}

Modern:
  POST /v1/approvals               -> {"request_id": "<req_id>"}
     body: {"action","account_key","reason","ttl_sec"}
  GET  /v1/approvals/<id>          -> {"id","status","reason","account_key","action","ts","ttl_sec","approved_by"}

Utility:
  GET  /v1/ping                    -> {"ok": true}
  GET  /ui/<id>                    -> minimal HTML approve/deny page
  POST /v1/approvals/<id>/approve  -> {"ok": true}
  POST /v1/approvals/<id>/deny     -> {"ok": true}

Auth:
  - Bearer token in Authorization: Bearer <secret>
  - Optional X-Sign HMAC SHA256 of the raw JSON body using the same secret (client sets it)

Env:
  APPROVAL_SERVICE_HOST=0.0.0.0
  APPROVAL_SERVICE_PORT=5055
  APPROVAL_SHARED_SECRET=change_me
  APPROVAL_STATE_PATH=.state/approvals.json
  APPROVAL_DEFAULT_TTL_SEC=900

Run:
  python services/approval_service.py
"""

from __future__ import annotations
import os, json, time, hmac, hashlib, threading
from pathlib import Path
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify, abort, redirect, Response

HOST = os.getenv("APPROVAL_SERVICE_HOST", "0.0.0.0")
PORT = int(os.getenv("APPROVAL_SERVICE_PORT", "5055") or 5055)
SECRET = os.getenv("APPROVAL_SHARED_SECRET", "")  # if empty, auth is disabled (dev only)
STATE_PATH = Path(os.getenv("APPROVAL_STATE_PATH", ".state/approvals.json"))
DEFAULT_TTL = int(os.getenv("APPROVAL_DEFAULT_TTL_SEC", "900") or 900)

app = Flask(__name__)
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

# In-memory store: id -> record
REC: Dict[str, Dict[str, Any]] = {}
_LOCK = threading.Lock()

def _now() -> int:
    return int(time.time())

def _load_state():
    if STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                REC.update(data)
        except Exception:
            pass

def _save_state():
    try:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(REC, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(STATE_PATH)
    except Exception:
        pass

def _gen_id(prefix: str = "appr") -> str:
    raw = f"{prefix}-{_now()}-{os.getpid()}-{os.urandom(4).hex()}".encode("utf-8")
    h = hashlib.blake2s(raw, digest_size=8).hexdigest()
    return f"{prefix}-{h}"

def _expired(rec: Dict[str, Any]) -> bool:
    ttl = int(rec.get("ttl_sec") or 0)
    if ttl <= 0:
        return False
    return (_now() - int(rec.get("ts") or 0)) >= ttl

def _status(rec: Dict[str, Any]) -> str:
    s = rec.get("status", "pending")
    if s == "pending" and _expired(rec):
        return "expired"
    return s

def _auth_ok(req) -> bool:
    if not SECRET:
        return True  # dev mode
    # bearer
    auth = (req.headers.get("Authorization") or "").split()
    if len(auth) == 2 and auth[0].lower() == "bearer" and auth[1] == SECRET:
        # optional HMAC for POST/PUT with bodies
        if req.method in ("POST", "PUT", "PATCH"):
            xs = req.headers.get("X-Sign", "")
            raw = req.get_data(cache=False, as_text=False) or b""
            expect = hmac.new(SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()
            # If header present but doesn't match, block; if header absent, allow (clients may not send it)
            if xs and xs != expect:
                return False
        return True
    return False

@app.before_request
def _auth_guard():
    # Allow UI and ping without auth so humans can click
    if request.path.startswith("/ui/") or request.path in ("/v1/ping", "/"):
        return
    if not _auth_ok(request):
        abort(401)

@app.get("/")
def root():
    return jsonify({"ok": True, "endpoints": ["/request","/status/<id>","/v1/approvals","/v1/approvals/<id>","/ui/<id>"]})

@app.get("/v1/ping")
def ping():
    return jsonify({"ok": True, "ts": _now()})

# ---------- Legacy API ----------
@app.post("/request")
def request_legacy():
    try:
        js = request.get_json(force=True, silent=False) or {}
    except Exception:
        abort(400, "bad json")

    action = (js.get("action") or "").strip()
    account = (js.get("account") or "").strip()
    reason = (js.get("reason") or "").strip()
    ttl = int(js.get("ttl_sec") or DEFAULT_TTL)

    if not action or not account:
        abort(400, "action and account required")

    rid = _gen_id("req")
    rec = {
        "id": rid,
        "action": action,
        "account": account,
        "reason": reason,
        "ttl_sec": int(ttl),
        "ts": _now(),
        "status": "pending",
        "actor": ""
    }
    with _LOCK:
        REC[rid] = rec
        _save_state()
    return jsonify({"id": rid})

@app.get("/status/<rid>")
def status_legacy(rid: str):
    with _LOCK:
        rec = REC.get(rid)
    if not rec:
        abort(404, "not found")
    out = dict(rec)
    out["status"] = _status(rec)
    return jsonify({"record": out})

# ---------- Modern API ----------
@app.post("/v1/approvals")
def create_v1():
    try:
        js = request.get_json(force=True, silent=False) or {}
    except Exception:
        abort(400, "bad json")

    action = (js.get("action") or "").strip()
    account = (js.get("account_key") or "").strip()
    reason = (js.get("reason") or "").strip()
    ttl = int(js.get("ttl_sec") or DEFAULT_TTL)

    if not action or not account:
        abort(400, "action and account_key required")

    rid = _gen_id("apr")
    rec = {
        "id": rid,
        "action": action,
        "account_key": account,
        "reason": reason,
        "ttl_sec": int(ttl),
        "ts": _now(),
        "status": "pending",
        "approved_by": ""
    }
    with _LOCK:
        REC[rid] = rec
        _save_state()
    return jsonify({"request_id": rid})

@app.get("/v1/approvals/<rid>")
def status_v1(rid: str):
    with _LOCK:
        rec = REC.get(rid)
    if not rec:
        abort(404, "not found")
    status = _status(rec)
    return jsonify({
        "id": rec["id"],
        "action": rec.get("action",""),
        "account_key": rec.get("account_key") or rec.get("account") or "",
        "reason": rec.get("reason",""),
        "ttl_sec": rec.get("ttl_sec", 0),
        "ts": rec.get("ts", 0),
        "status": status,
        "approved_by": rec.get("approved_by") or rec.get("actor") or ""
    })

@app.post("/v1/approvals/<rid>/approve")
def approve_v1(rid: str):
    who = request.headers.get("X-Actor") or request.args.get("actor") or "human"
    with _LOCK:
        rec = REC.get(rid)
        if not rec:
            abort(404, "not found")
        if _status(rec) == "expired":
            rec["status"] = "expired"
            _save_state()
            abort(410, "expired")
        rec["status"] = "approved"
        rec["approved_by"] = who
        rec["actor"] = who
        _save_state()
    return jsonify({"ok": True})

@app.post("/v1/approvals/<rid>/deny")
def deny_v1(rid: str):
    who = request.headers.get("X-Actor") or request.args.get("actor") or "human"
    with _LOCK:
        rec = REC.get(rid)
        if not rec:
            abort(404, "not found")
        if _status(rec) == "expired":
            rec["status"] = "expired"
            _save_state()
            abort(410, "expired")
        rec["status"] = "denied"
        rec["approved_by"] = ""
        rec["actor"] = who
        _save_state()
    return jsonify({"ok": True})

# ---------- Barebones UI ----------
_HTML = """
<!doctype html><html><head>
<meta charset="utf-8"><title>Approval {rid}</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:2rem;color:#111}
.card{max-width:720px;padding:1.25rem;border:1px solid #ddd;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.05)}
h1{font-size:1.25rem;margin:0 0 .5rem}
.meta{color:#444;font-size:.95rem;margin:.25rem 0}
.actions{margin-top:1rem;display:flex;gap:.75rem}
.btn{padding:.6rem 1rem;border-radius:10px;border:0;cursor:pointer;font-weight:600}
.approve{background:#16a34a;color:#fff}
.deny{background:#dc2626;color:#fff}
.pending{background:#eab308;color:#111;padding:.25rem .5rem;border-radius:8px;display:inline-block;margin-top:.5rem}
small{color:#666}
</style>
</head><body><div class="card">
<h1>Approval <code>{rid}</code></h1>
<div class="meta"><b>Action:</b> {action}</div>
<div class="meta"><b>Account:</b> {account}</div>
<div class="meta"><b>Reason:</b> {reason}</div>
<div class="meta"><b>TTL:</b> {ttl} s</div>
<div class="meta"><b>Status:</b> <span class="pending">{status}</span></div>
<form class="actions" method="post">
  <input type="hidden" name="actor" value="web-ui">
  <button formaction="/v1/approvals/{rid}/approve" class="btn approve">Approve</button>
  <button formaction="/v1/approvals/{rid}/deny" class="btn deny">Deny</button>
</form>
<small>Tip: set APPROVAL_SHARED_SECRET to require API auth; UI is intentionally open for humans.</small>
</div></body></html>
"""

@app.get("/ui/<rid>")
def ui(rid: str):
    with _LOCK:
        rec = REC.get(rid)
    if not rec:
        return Response("Not found", status=404)
    status = _status(rec)
    html = _HTML.format(
        rid=rec["id"],
        action=rec.get("action",""),
        account=rec.get("account_key") or rec.get("account") or "",
        reason=(rec.get("reason") or "(none)"),
        ttl=int(rec.get("ttl_sec") or 0),
        status=status
    )
    return Response(html, mimetype="text/html")

# ---------- bootstrap ----------
if __name__ == "__main__":
    _load_state()
    try:
        app.run(host=HOST, port=PORT)
    finally:
        _save_state()
