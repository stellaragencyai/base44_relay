#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
core/crypto_cmd.py — Encrypted command path (AES-GCM + HKDF) bound to time-lock tokens

What this is
- A tiny library + CLI to SEAL JSON commands such that they can only be opened if:
  1) The holder presents a VALID time-lock unlock token (from core/time_lock_manager.py),
  2) The token’s account_key matches the ciphertext audience,
  3) The token is NOT expired, and
  4) (Optional) The account is not currently locked anymore (after consuming token on your side).

Crypto
- AES-256-GCM with random 96-bit nonce
- KEK derivation via HKDF-SHA256 from TIMELOCK_HMAC_SECRET and a per-message SALT
- AAD binds: version, audience (account_key), issued_ts, and a stable HMAC of the unlock token
- Output is a compact JSON envelope safe to store/send
- No secrets ever stored inside the ciphertext envelope

Env (.env)
  TIMELOCK_HMAC_SECRET=super_long_random_hex_or_base64
  TZ=UTC

Runtime deps
  pip install cryptography python-dotenv

Typical flow
  # 1) Admin generates an unlock token (bound to sub account):
  python -m core.time_lock_manager --generate-token sub:260417078 --ttl-seconds 600

  # 2) You encrypt a command bound to that same account_key + token:
  python -m core.crypto_cmd --encrypt \
     --account sub:260417078 \
     --token '<PASTE_TOKEN>' \
     --in command.json \
     --out sealed.json

  # 3) The consumer verifies/decrypts with the token (same token string):
  python -m core.crypto_cmd --decrypt --token '<PASTE_TOKEN>' --in sealed.json --out opened.json

Integration (Python)
  from core.crypto_cmd import seal_command, open_command, CryptoError
  sealed = seal_command({"op":"withdraw","amount":10}, account_key="sub:260417078", unlock_token=token_str)
  opened = open_command(sealed, unlock_token=token_str)

Security notes
- Token hash is part of AAD to bind ciphertext to a specific token instance and audience.
- The key is derived from TIMELOCK_HMAC_SECRET using HKDF salt-per-message, so compromise of one
  ciphertext does not help with others.
- This library PURPOSELY does not reach out to the time_lock_manager API. It only validates tokens
  cryptographically (signature/expiry). Enforcement such as "consume token" or "ensure account unlocked"
  should be done by your orchestrator service right before executing the sensitive action.

"""

from __future__ import annotations
import os
import json
import time
import base64
import hmac
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

# cryptography primitives
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# local token validator (no network; purely cryptographic)
try:
    from core.time_lock_manager import validate_unlock_token
except Exception as _e:
    raise SystemExit(
        "core.crypto_cmd: missing core/time_lock_manager.py or import failed. "
        "Add that file first (you already have it) and ensure PYTHONPATH includes repo root."
    ) from _e


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")

def _b64u_dec(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _now_ts() -> int:
    return int(time.time())

def _json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def _json_loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def _hmac_sha256(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()

def _mask(s: str, keep: int = 6) -> str:
    if not s: return "<empty>"
    if len(s) <= keep * 2: return s[0:2] + "…" + s[-2:]
    return s[:keep] + "…" + s[-keep:]


# --------------------------------------------------------------------------------------
# Config / Key Derivation
# --------------------------------------------------------------------------------------

HMAC_SECRET_RAW = (os.getenv("TIMELOCK_HMAC_SECRET") or "").encode("utf-8")
if not HMAC_SECRET_RAW or len(HMAC_SECRET_RAW) < 16:
    raise SystemExit("TIMELOCK_HMAC_SECRET missing or too short. Put a long random secret in your .env.")

HKDF_INFO = b"base44/crypto_cmd/v1"  # context label for HKDF

def _derive_aes_key(salt: bytes, audience: str, token_hash: bytes) -> bytes:
    """
    Derive a per-message AES-256-GCM key from TIMELOCK_HMAC_SECRET + salt + audience + token_hash.
    The token_hash binds ciphertext to a specific token instance (and audience).
    """
    # Mix secret + audience + token hash into a single ikm via HMAC to avoid leaking secret via HKDF salt
    ikm = _hmac_sha256(HMAC_SECRET_RAW, audience.encode("utf-8") + b"|" + token_hash)
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,  # AES-256
        salt=salt,
        info=HKDF_INFO,
    )
    return hkdf.derive(ikm)


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------

@dataclass
class CryptoEnvelope:
    v: int
    alg: str
    aud: str           # account_key
    ts: int            # issued ts
    salt: str          # b64url
    nonce: str         # b64url
    aad: str           # b64url JSON of AAD fields
    ct: str            # b64url ciphertext (includes GCM tag)
    tokh: str          # b64url of token HMAC (stable binding; not reversible)

class CryptoError(Exception):
    pass

def _token_binding(unlock_token: str) -> Tuple[bytes, Dict[str, Any]]:
    """
    Validate the unlock token cryptographically and return a stable HMAC binding + parsed payload.
    """
    v = validate_unlock_token(unlock_token)
    if not v.get("ok"):
        raise CryptoError(f"unlock token invalid: {v.get('error')}")
    payload = v["payload"]  # {'a': account_key, 'exp': ts, 'n': base64nonce}
    # Stable binding: HMAC(secret, token_raw_bytes) — use raw token string bytes
    tok_hash = _hmac_sha256(HMAC_SECRET_RAW, unlock_token.encode("utf-8"))
    return tok_hash, payload

def seal_command(command_obj: Dict[str, Any], *, account_key: str, unlock_token: str) -> Dict[str, Any]:
    """
    Encrypt a JSON-serializable dict with AES-256-GCM; bind to account_key and unlock_token.
    Returns an envelope dict suitable for storage/transmission.
    """
    if not isinstance(command_obj, dict):
        raise CryptoError("command_obj must be a dict")

    tok_hash, tok_payload = _token_binding(unlock_token)
    tok_account = tok_payload.get("a")
    if str(tok_account) != str(account_key):
        raise CryptoError(f"token account_key mismatch: token={tok_account} cmd={account_key}")

    ts = _now_ts()
    salt = os.urandom(16)
    nonce = os.urandom(12)

    aad_obj = {
        "v": 1,
        "alg": "AESGCM",
        "aud": account_key,
        "ts": ts,
        "tokh": _b64u(tok_hash),
    }
    aad_bytes = _json_dumps(aad_obj)

    key = _derive_aes_key(salt, account_key, tok_hash)
    aes = AESGCM(key)

    pt = _json_dumps(command_obj)
    ct = aes.encrypt(nonce, pt, aad_bytes)

    env = CryptoEnvelope(
        v=1,
        alg="AESGCM",
        aud=account_key,
        ts=ts,
        salt=_b64u(salt),
        nonce=_b64u(nonce),
        aad=_b64u(aad_bytes),
        ct=_b64u(ct),
        tokh=_b64u(tok_hash),
    )
    return env.__dict__

def open_command(envelope: Dict[str, Any], *, unlock_token: str) -> Dict[str, Any]:
    """
    Decrypt an envelope with the provided unlock token. Raises CryptoError on failure.
    """
    try:
        v = int(envelope.get("v"))
        alg = str(envelope.get("alg"))
        aud = str(envelope.get("aud"))
        ts = int(envelope.get("ts"))
        salt = _b64u_dec(str(envelope.get("salt")))
        nonce = _b64u_dec(str(envelope.get("nonce")))
        aad_bytes = _b64u_dec(str(envelope.get("aad")))
        ct = _b64u_dec(str(envelope.get("ct")))
        tokh = _b64u_dec(str(envelope.get("tokh")))
    except Exception as e:
        raise CryptoError(f"bad envelope fields: {e}")

    if v != 1 or alg.upper() != "AESGCM":
        raise CryptoError("unsupported envelope version/alg")

    # Validate token and confirm binding + audience
    tok_hash, tok_payload = _token_binding(unlock_token)
    if not hmac.compare_digest(tok_hash, tokh):
        raise CryptoError("token binding mismatch (wrong token for this ciphertext)")

    tok_account = str(tok_payload.get("a"))
    if tok_account != aud:
        raise CryptoError(f"token account_key mismatch: token={tok_account} env.aud={aud}")

    # Derive key and decrypt
    key = _derive_aes_key(salt, aud, tok_hash)
    aes = AESGCM(key)

    try:
        pt = aes.decrypt(nonce, ct, aad_bytes)
    except Exception as e:
        raise CryptoError(f"decrypt failed: {e}")

    try:
        obj = _json_loads(pt)
    except Exception as e:
        raise CryptoError(f"plaintext not JSON: {e}")

    return obj


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------

def _cli():
    import argparse, sys

    p = argparse.ArgumentParser(description="Base44 Crypto Cmd (seal/open)")
    sub = p.add_subparsers(dest="cmd")

    pe = sub.add_parser("encrypt", help="Encrypt a JSON command file")
    pe.add_argument("--account", required=True, help="account_key (e.g., sub:260417078 or MAIN)")
    pe.add_argument("--token", required=True, help="unlock token string")
    pe.add_argument("--in", dest="infile", required=True, help="input JSON file")
    pe.add_argument("--out", dest="outfile", required=True, help="output sealed JSON file")

    pd = sub.add_parser("decrypt", help="Decrypt a sealed JSON envelope")
    pd.add_argument("--token", required=True, help="unlock token string")
    pd.add_argument("--in", dest="infile", required=True, help="sealed JSON file")
    pd.add_argument("--out", dest="outfile", required=True, help="output plaintext JSON file")

    # shortcuts
    p.add_argument("--encrypt", action="store_true", help="shortcut: same as subcommand encrypt")
    p.add_argument("--decrypt", action="store_true", help="shortcut: same as subcommand decrypt")

    args, extra = p.parse_known_args()

    # map shortcuts
    if args.encrypt and not args.cmd:
        args.cmd = "encrypt"
    if args.decrypt and not args.cmd:
        args.cmd = "decrypt"

    if args.cmd == "encrypt":
        try:
            data = json.loads(Path(args.infile).read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[crypto_cmd] failed reading JSON: {e}", file=sys.stderr); sys.exit(2)
        env = seal_command(data, account_key=args.account, unlock_token=args.token)
        Path(args.outfile).write_text(json.dumps(env, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"[crypto_cmd] sealed → {args.outfile}  aud={args.account} token={_mask(args.token)}")
        return

    if args.cmd == "decrypt":
        try:
            env = json.loads(Path(args.infile).read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[crypto_cmd] failed reading envelope: {e}", file=sys.stderr); sys.exit(2)
        try:
            obj = open_command(env, unlock_token=args.token)
        except CryptoError as e:
            print(f"[crypto_cmd] decrypt error: {e}", file=sys.stderr); sys.exit(3)
        Path(args.outfile).write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[crypto_cmd] opened → {args.outfile}  aud={env.get('aud')} token={_mask(args.token)}")
        return

    p.print_help()


if __name__ == "__main__":
    _cli()
