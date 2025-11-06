#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# core/approval_service.py
"""
Approval Service — Friend-in-the-loop overrides (Flask, file-backed, HMAC-signed)

What it does
- Accepts signed approval requests from your bots (client).
- Generates a one-time code + link, emails it to FRIEND_EMAIL (and/or calls SMS hook).
- Tracks status: pending -> approved/denied/expired.
- Provides a minimal web UI for the friend to click Approve or Deny.
- Persists to .state/approvals.json (append-safe).

Security
- All bot→service calls require Authorization: Bearer <APPROVAL_SHARED_SECRET>.
- Each request body is also HMAC-SHA256 signed with X-Sign: <hex>, computed over the raw JSON bytes.
- One-time code is 6 digits; link includes a signed request id + code.
- CORS off by default; this isn’t for browsers, it’s a tiny internal service.

Env (.env)
  APPROVAL_HOST=0.0.0.0
  APPROVAL_PORT=5055
  APPROVAL_SHARED_SECRET=use_a_long_random_string
  APPROVAL_DATA_DIR=.state
  FRIEND_EMAIL=friend@example.com
  SMTP_HOST=smtp.example.com
  SMTP_PORT=587
  SMTP_USER=username
  SMTP_PASS=password
  SMTP_FROM=Base44 <no-reply@yourdomain.tld>
  # optional
  APPROVAL_SERVICE_BASE=https://your-approval-host  # for links in email; defaults to http://host:port

Endpoints
  POST /request        (auth+signed) -> create approval; {action, account, reason, ttl_sec?}
  GET  /status/<rid>   (auth) -> {status, created_at, ...}
  POST /approve        -> form/json {rid, code} OR click link /ui/approve?rid=..&code=..
  GET  /ui/approve     -> simple html approve/deny page for the friend
  POST /deny           -> form/json {rid}

Run
  python -m core.approval_service
"""

from __future__ import annotations
import os, json, hmac, hashlib, random, string, smtplib, time
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urlencode

from flask import Flask, request, jsonify, abort, Response, redirect

APP = Flask(__name__)

HOST = os.getenv("APPROVAL_HOST", "0.0.0.0")
PORT = int(os.getenv("APPROVAL_PORT", "5055"))
SECRET = os.getenv("APPROVAL_SHARED_SECRET", "")
DATA_DIR = Path(os.getenv("APPROVAL_DATA_DIR", ".state"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
STORE = DATA_DIR / "approvals.json"

FRIEND_EMAIL = os.getenv("FRIEND_EMAIL", "")
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
SMTP_FROM = os.getenv("SMTP_FROM", "Base44 <no-reply@base44.local>")

BASE_URL = os.getenv("APPROVAL_SERVICE_BASE")  # if unset, infer at runtime per request

def _now() -> int: return int(time.time())

def _load_db() -> Dict[str, Any]:
    try:
        return json.loads(STORE.read_text(encoding="utf-8"))
    except Exception:
        return {"by_id": {}}

def _save_db(db: Dict[str, Any]) -> None:
    tmp = STORE.with_suffix(".tmp")
    tmp.write_text(json.dumps(db, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(STORE)

def _rand_id(n: int = 22) -> str:
    alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(alpha) for _ in range(n))

def _code6() -> str:
    return "".join(random.choice("0123456789") for _ in range(6))

def _hmac_hex(key: str, raw: bytes) -> str:
    return hmac.new(key.encode("utf-8"), raw, hashlib.sha256).hexdigest()

def _require_bearer():
    if not SECRET:
        abort(500, "Service misconfigured: APPROVAL_SHARED_SECRET missing.")
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != SECRET:
        abort(401, "Unauthorized")

def _require_signed_json() -> Dict[str, Any]:
    _require_bearer()
    raw = request.get_data() or b"{}"
    want = request.headers.get("X-Sign", "")
    have = _hmac_hex(SECRET, raw)
    if not hmac.compare_digest(want, have):
        abort(401, "Invalid signature")
    try:
        obj = json.loads(raw.decode("utf-8"))
        return obj
    except Exception:
        abort(400, "Bad JSON")

def _send_email(to_addr: str, subject: str, body: str) -> None:
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, to_addr]):
        # If email isn’t configured, we still behave but log to console.
        print(f"[approval/email/dry] to={to_addr} subj={subject}\n{body}")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to_addr
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_FROM, [to_addr], msg.as_string())

def _public_base(req) -> str:
    if BASE_URL:
        return BASE_URL.rstrip("/")
    scheme = "https" if req.headers.get("X-Forwarded-Proto","").lower() == "https" else req.scheme
    host = req.headers.get("Host") or f"127.0.0.1:{PORT}"
    return f"{scheme}://{host}"

def _status_view(x: Dict[str, Any]) -> Dict[str, Any]:
    redacted = dict(x)
    redacted.pop("code", None)
    return redacted

@APP.route("/request", methods=["POST"])
def create_request():
    obj = _require_signed_json()
    action = str(obj.get("action") or "").strip()
    account = str(obj.get("account") or "").strip()
    reason = str(obj.get("reason") or "").strip()
    ttl = int(obj.get("ttl_sec") or 900)  # 15 min default

    if not action or not account:
        abort(400, "action and account are required")

    db = _load_db()
    rid = _rand_id()
    code = _code6()
    now = _now()
    rec = {
        "id": rid,
        "action": action,
        "account": account,
        "reason": reason,
        "status": "pending",
        "created_at": now,
        "ttl_sec": ttl,
        "expires_at": now + ttl if ttl > 0 else 0,
        "code": code,
        "audit": [],
    }
    db["by_id"][rid] = rec
    _save_db(db)

    link = f"{_public_base(request)}/ui/approve?{urlencode({'rid':rid,'code':code})}"

    email_body = (
        f"Approval needed for Base44 action:\n\n"
        f"Action: {action}\n"
        f"Account: {account}\n"
        f"Reason: {reason or '(no reason)'}\n"
        f"Expires: {'never' if not rec['expires_at'] else time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(rec['expires_at']))} UTC\n\n"
        f"One-time code: {code}\n"
        f"Approve link: {link}\n"
        f"If you do NOT recognize this, ignore the email.\n"
    )
    try:
        _send_email(FRIEND_EMAIL, f"[Base44] Approval needed: {action} ({account})", email_body)
    except Exception as e:
        print(f"[approval/email/error] {e}")

    return jsonify({"ok": True, "id": rid, "status": "pending", "expires_at": rec["expires_at"]})

@APP.get("/status/<rid>")
def status(rid: str):
    _require_bearer()
    db = _load_db()
    rec = db["by_id"].get(rid)
    if not rec:
        abort(404, "not found")
    # auto-expire
    if rec["status"] == "pending" and rec["expires_at"] and _now() > rec["expires_at"]:
        rec["status"] = "expired"
        rec["audit"].append({"ts": _now(), "event": "auto_expire"})
        _save_db(db)
    return jsonify({"ok": True, "record": _status_view(rec)})

@APP.post("/approve")
def approve():
    rid = request.values.get("rid") or (request.json or {}).get("rid")
    code = request.values.get("code") or (request.json or {}).get("code")
    if not rid or not code:
        abort(400, "rid and code required")

    db = _load_db()
    rec = db["by_id"].get(rid)
    if not rec:
        abort(404, "not found")

    if rec["status"] != "pending":
        return jsonify({"ok": True, "status": rec["status"]})

    if rec["expires_at"] and _now() > rec["expires_at"]:
        rec["status"] = "expired"
        rec["audit"].append({"ts": _now(), "event": "approve_after_expiry"})
        _save_db(db)
        return jsonify({"ok": True, "status": "expired"})

    if str(code).strip() != str(rec["code"]).strip():
        rec["audit"].append({"ts": _now(), "event": "bad_code"})
        _save_db(db)
        abort(401, "bad code")

    rec["status"] = "approved"
    rec["audit"].append({"ts": _now(), "event": "approved"})
    _save_db(db)
    return jsonify({"ok": True, "status": "approved"})

@APP.post("/deny")
def deny():
    rid = request.values.get("rid") or (request.json or {}).get("rid")
    if not rid:
        abort(400, "rid required")
    db = _load_db()
    rec = db["by_id"].get(rid)
    if not rec:
        abort(404, "not found")
    if rec["status"] == "pending":
        rec["status"] = "denied"
        rec["audit"].append({"ts": _now(), "event": "denied"})
        _save_db(db)
    return jsonify({"ok": True, "status": rec["status"]})

# Minimal UI
@APP.get("/ui/approve")
def ui_page():
    rid = request.args.get("rid", "")
    code = request.args.get("code", "")
    html = f"""
<!doctype html><meta charset="utf-8">
<title>Base44 Approval</title>
<style>
body{{font-family:system-ui,Arial;margin:2rem;}}
.card{{max-width:520px;border:1px solid #ddd;border-radius:12px;padding:20px;box-shadow:0 2px 10px rgba(0,0,0,.04);}}
button{{padding:.6rem 1rem;border-radius:10px;border:0;cursor:pointer;margin-right:.5rem}}
.approve{{background:#10b981;color:#fff}}
.deny{{background:#ef4444;color:#fff}}
input{{padding:.5rem;border:1px solid #ccc;border-radius:8px;width:100%;}}
small{{color:#666}}
</style>
<div class="card">
  <h2>Approve action</h2>
  <form method="post" action="/approve">
    <input type="hidden" name="rid" value="{rid}">
    <label>One-time code</label>
    <input name="code" value="{code}" placeholder="6-digit code">
    <div style="margin-top:12px">
      <button class="approve" type="submit">Approve</button>
      <button class="deny" formaction="/deny" formmethod="post">Deny</button>
    </div>
  </form>
  <p><small>Request: {rid or '(none)'} — paste the code from the email if not prefilled.</small></p>
</div>
"""
    return Response(html, mimetype="text/html")

def main():
    APP.run(host=HOST, port=PORT)

if __name__ == "__main__":
    main()
