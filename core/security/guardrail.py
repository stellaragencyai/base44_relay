# core/security/guardrail.py
import hmac, hashlib, time
from typing import Dict
from core.logger import get_logger
from .config import load
from .yubikey_guard import YubiGuard
from .approver import request_approval
from .timelock import check as timelock_check

log = get_logger("guardrail")
cfg = load()
yubi = YubiGuard()

def _hmac_token(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

def is_exempt(sub_uid: str) -> bool:
    return str(sub_uid) == str(cfg.always_access_sub_uid).strip() if cfg.always_access_sub_uid else False

def verify_all(action: str, amount: float, sub_uid: str, meta: Dict) -> (bool, str):
    if not cfg.enabled:
        return True, "security disabled"

    if is_exempt(sub_uid):
        return True, f"exempt sub_uid={sub_uid}"

    ok, why = timelock_check()
    if not ok:
        return False, f"timelock: {why}"

    if not yubi.require():
        return False, "yubikey failure or not present"

    appr = request_approval(action, amount, sub_uid, meta)
    if not appr.approved:
        return False, f"approver: {appr.reason}"

    # Final signed token to attach to the request, if caller wants
    nonce = str(int(time.time()))
    payload = f"{action}|{amount}|{sub_uid}|{nonce}"
    sig = _hmac_token(cfg.secret, payload)
    return True, sig
