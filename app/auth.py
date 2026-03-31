from __future__ import annotations

import hashlib
import secrets


def hash_password(password: str, salt: bytes | None = None) -> tuple[bytes, bytes]:
    salt_bytes = salt if salt is not None else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_bytes, 250_000)
    return salt_bytes, digest


def verify_password(password: str, salt: bytes, expected_hash: bytes) -> bool:
    _, digest = hash_password(password, salt=salt)
    return secrets.compare_digest(digest, expected_hash)
