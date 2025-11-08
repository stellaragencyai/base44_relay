# core/security/yubikey_guard.py
import os, subprocess, time, secrets, hmac, hashlib
from .config import load
from core.logger import get_logger
log = get_logger("yubikey")

cfg = load()

class YubiGuard:
    def __init__(self):
        self.method = cfg.yubikey_method

    def _fido2_challenge(self) -> bool:
        try:
            # Lazy import to avoid breaking environments without libfido2
            from fido2.hid import CtapHidDevice
            from fido2.client import Fido2Client
            from fido2 import cbor
        except Exception as e:
            log.warning("fido2 not available: %s", e)
            return False

        devices = list(CtapHidDevice.list_devices())
        if not devices:
            log.error("No FIDO2 devices detected.")
            return False

        device = devices[0]
        client = Fido2Client(device, f"https://{cfg.fido2_rpid}")
        challenge = secrets.token_bytes(32)
        log.info("Touch the YubiKey to approve (FIDO2) ...")
        try:
            assertion = client.get_assertion(
                rp_id=cfg.fido2_rpid,
                client_data_hash=hashlib.sha256(challenge).digest(),
                allow_credentials=None,
                options={"uv": False, "up": True}
            )
            # If we got an assertion without errors, we’re good.
            return True
        except Exception as e:
            log.error("FIDO2 assertion failed: %s", e)
            return False

    def _ykman_oath(self) -> bool:
        account = cfg.yubi_oath_account or "Base44"
        try:
            # Requires: ykman installed, OATH credential set up on the key
            # This will prompt for touch on the key.
            out = subprocess.check_output(
                ["ykman", "oath", "accounts", "code", account],
                stderr=subprocess.STDOUT,
                text=True,
                timeout=20
            )
            log.info("YubiKey OATH code ok: %s", out.strip())
            return True
        except subprocess.CalledProcessError as e:
            log.error("ykman oath error: %s", e.output.strip())
            return False
        except FileNotFoundError:
            log.error("ykman not installed.")
            return False
        except Exception as e:
            log.error("ykman oath unexpected: %s", e)
            return False

    def require(self) -> bool:
        method = (self.method or "fido2").lower()
        if method == "none":
            log.warning("YubiKey requirement disabled by config.")
            return True
        if method == "fido2":
            ok = self._fido2_challenge()
            if ok: return True
            log.warning("FIDO2 path failed. Trying ykman...")
            return self._ykman_oath()
        if method == "ykman":
            return self._ykman_oath()
        log.error("Unknown YUBI_METHOD=%s", method)
        return False
