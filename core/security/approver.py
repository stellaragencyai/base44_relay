# core/security/approver.py

import smtplib, ssl, json, time, requests, secrets
from email.message import EmailMessage
from datetime import datetime, timezone
from .config import load
from core.logger import get_logger

cfg = load()
log = get_logger("approver")

class ApprovalResult:
    def __init__(self, approved: bool, reason: str = "", token: str = ""):
        self.approved = approved
        self.reason = reason
        self.token = token

def _make_payload(action: str, amount: float, sub_uid: str, meta: dict):
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "amount": amount,
        "sub_uid": sub_uid,
        "meta": meta or {}
    }

def _one_time_token():
    return secrets.token_urlsafe(24)

def approve_via_telegram(payload: dict, timeout_sec=120) -> ApprovalResult:
    if not cfg.tg_bot or not cfg.tg_chat:
        return ApprovalResult(False, "Telegram not configured")
    token = _one_time_token()
    text = (
        "⚠️ *Approval Requested*\n"
        f"Action: `{payload['action']}`\n"
        f"Amount: `{payload['amount']}`\n"
        f"Sub UID: `{payload['sub_uid']}`\n"
        f"Meta: `{json.dumps(payload['meta'])}`\n\n"
        f"Reply with: `{token}` to approve within {timeout_sec}s."
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{cfg.tg_bot}/sendMessage",
            json={"chat_id": cfg.tg_chat, "text": text, "parse_mode":"Markdown"},
            timeout=10
        )
    except Exception as e:
        return ApprovalResult(False, f"Telegram send failed: {e}")

    # Poll for replies (simple approach; production could use webhook)
    deadline = time.time() + timeout_sec
    last_update_id = None
    while time.time() < deadline:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{cfg.tg_bot}/getUpdates",
                params={"offset": last_update_id + 1 if last_update_id else None},
                timeout=10
            ).json()
            for upd in resp.get("result", []):
                last_update_id = upd["update_id"]
                msg = upd.get("message") or upd.get("channel_post") or {}
                if str(msg.get("chat", {}).get("id")) != str(cfg.tg_chat):
                    continue
                text = (msg.get("text") or "").strip()
                if text == token:
                    return ApprovalResult(True, token=token)
        except Exception:
            pass
        time.sleep(2)

    return ApprovalResult(False, "Timeout waiting for approver")

def approve_via_email(payload: dict, timeout_sec=180) -> ApprovalResult:
    if not cfg.smtp_host or not cfg.approver_email_to:
        return ApprovalResult(False, "Email not configured")
    token = _one_time_token()
    msg = EmailMessage()
    msg["From"] = cfg.smtp_user
    msg["To"] = cfg.approver_email_to
    msg["Subject"] = "Base44 Approval Request"
    msg.set_content(
        f"Approval requested:\n{json.dumps(payload, indent=2)}\n\n"
        f"Reply to this email with ONLY this token on the first line within {timeout_sec}s:\n{token}\n"
    )
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as s:
            s.starttls(context=ctx)
            s.login(cfg.smtp_user, cfg.smtp_pass)
            s.send_message(msg)
    except Exception as e:
        return ApprovalResult(False, f"Email send failed: {e}")

    # Dumb placeholder: no IMAP read here. Keep email as secondary path.
    return ApprovalResult(False, "Email path requires IMAP listener in prod")

def request_approval(action: str, amount: float, sub_uid: str, meta: dict) -> ApprovalResult:
    payload = _make_payload(action, amount, sub_uid, meta)
    if cfg.approver_provider == "telegram":
        return approve_via_telegram(payload)
    if cfg.approver_provider == "email":
        return approve_via_email(payload)
    return ApprovalResult(False, f"Unknown APPROVER_PROVIDER={cfg.approver_provider}")
