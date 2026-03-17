from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from .config import settings


def _key_material(raw_key: str) -> bytes:
    digest = hashlib.sha256(raw_key.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _fernet() -> Fernet:
    key = settings.input_encryption_key
    if not key:
        raise RuntimeError("INPUT_ENCRYPTION_KEY is required")
    return Fernet(_key_material(key))


def encrypt_input_payload(payload: dict[str, Any]) -> str:
    token = _fernet().encrypt(json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
    return token.decode("utf-8")


def decrypt_input_payload(token: str) -> dict[str, Any]:
    try:
        plain = _fernet().decrypt(token.encode("utf-8"))
    except InvalidToken as exc:
        raise ValueError("invalid encrypted input payload") from exc
    decoded = json.loads(plain.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("decrypted payload must be object")
    return decoded

