# core/security/config.py
import os
from dataclasses import dataclass

def as_bool(v, default=False):
    if v is None: return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}

@dataclass
class SecConfig:
    enabled: bool
    secret: str
    always_access_sub_uid: str

    yubikey_method: str
    fido2_rpid: str
    fido2_user: str
    yubi_oath_account: str

    approver_provider: str
    tg_bot: str
    tg_chat: str

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    approver_email_to: str

    timelock_enabled: bool
    timelock_allow_after: str
    timelock_min_equity: float
    timelock_min_green_days: int

    bind: str
    port: int

def load():
    return SecConfig(
        enabled=as_bool(os.getenv("SECURITY_ENABLED","true"), True),
        secret=os.getenv("SECURITY_SECRET",""),
        always_access_sub_uid=os.getenv("ALWAYS_ACCESS_SUB_UID","").strip(),

        yubikey_method=os.getenv("YUBI_METHOD","fido2").strip(),
        fido2_rpid=os.getenv("FIDO2_RPID","base44.local").strip(),
        fido2_user=os.getenv("FIDO2_USER_HANDLE","operator").strip(),
        yubi_oath_account=os.getenv("YUBI_OATH_ACCOUNT","Base44").strip(),

        approver_provider=os.getenv("APPROVER_PROVIDER","telegram").strip(),
        tg_bot=os.getenv("TELEGRAM_BOT_TOKEN","").strip(),
        tg_chat=os.getenv("TELEGRAM_CHAT_ID","").strip(),

        smtp_host=os.getenv("SMTP_HOST","").strip(),
        smtp_port=int(os.getenv("SMTP_PORT","587")),
        smtp_user=os.getenv("SMTP_USER","").strip(),
        smtp_pass=os.getenv("SMTP_PASS","").strip(),
        approver_email_to=os.getenv("APPROVER_EMAIL_TO","").strip(),

        timelock_enabled=as_bool(os.getenv("TIMELOCK_ENABLED","true"), True),
        timelock_allow_after=os.getenv("TIMELOCK_ALLOW_AFTER","1970-01-01T00:00:00Z").strip(),
        timelock_min_equity=float(os.getenv("TIMELOCK_MIN_EQUITY_USD","0.0")),
        timelock_min_green_days=int(os.getenv("TIMELOCK_MIN_GREEN_DAYS","0")),

        bind=os.getenv("SECURITY_BIND","127.0.0.1").strip(),
        port=int(os.getenv("SECURITY_PORT","5002")),
    )
