from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass
from typing import Any


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    rounds = 260_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return f"pbkdf2_sha256${rounds}${_b64url_encode(salt)}${_b64url_encode(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        algo, rounds_s, salt_s, digest_s = encoded.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        salt = _b64url_decode(salt_s)
        expected = _b64url_decode(digest_s)
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt, int(rounds_s)
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


@dataclass(frozen=True)
class TokenClaims:
    sub: str
    exp: int


class TokenError(ValueError):
    pass


def create_access_token(
    *,
    user_id: str,
    secret: str,
    expires_seconds: int = 60 * 60 * 12,
) -> str:
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"sub": user_id, "iat": now, "exp": now + int(expires_seconds)}
    signing_input = (
        _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        + "."
        + _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    )
    signature = hmac.new(
        secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256
    ).digest()
    return signing_input + "." + _b64url_encode(signature)


def decode_access_token(token: str, *, secret: str) -> TokenClaims:
    try:
        header_s, payload_s, signature_s = token.split(".", 2)
        signing_input = f"{header_s}.{payload_s}"
        expected = hmac.new(
            secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(_b64url_decode(signature_s), expected):
            raise TokenError("invalid token signature")
        payload: dict[str, Any] = json.loads(_b64url_decode(payload_s))
        sub = str(payload.get("sub") or "")
        exp = int(payload.get("exp") or 0)
        if not sub:
            raise TokenError("missing subject")
        if exp < int(time.time()):
            raise TokenError("token expired")
        return TokenClaims(sub=sub, exp=exp)
    except TokenError:
        raise
    except Exception as exc:
        raise TokenError("invalid token") from exc

