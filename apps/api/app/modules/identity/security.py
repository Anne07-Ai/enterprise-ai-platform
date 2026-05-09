"""Password hashing (argon2id), JWT mint/verify, API key generation.

Keys are loaded from disk if EAIP_AUTH_JWT_*_KEY_PATH points to a file;
otherwise we generate ephemeral RSA keys at startup (with a loud warning) so
local development works out of the box. Production deployments must mount real
key files.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import jwt
from argon2 import PasswordHasher, Type
from argon2.exceptions import VerifyMismatchError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.core.config import AuthSettings, get_settings
from app.core.errors import AuthenticationError
from app.core.logging import get_logger

logger = get_logger(__name__)


# --- password hashing ----------------------------------------------------


@lru_cache(maxsize=1)
def _password_hasher() -> PasswordHasher:
    s = get_settings().auth
    return PasswordHasher(
        type=Type.ID,
        time_cost=s.argon2_time_cost,
        memory_cost=s.argon2_memory_cost_kib,
        parallelism=s.argon2_parallelism,
        hash_len=s.argon2_hash_length,
    )


def hash_password(plaintext: str) -> str:
    return _password_hasher().hash(plaintext)


def verify_password(hashed: str, plaintext: str) -> bool:
    try:
        return _password_hasher().verify(hashed, plaintext)
    except VerifyMismatchError:
        return False
    except Exception:  # pragma: no cover — malformed hash, treat as failure
        return False


def needs_rehash(hashed: str) -> bool:
    try:
        return _password_hasher().check_needs_rehash(hashed)
    except Exception:
        return False


# --- JWT keys ------------------------------------------------------------


_PRIVATE_KEY: bytes | None = None
_PUBLIC_KEY: bytes | None = None


def _generate_ephemeral_keys() -> tuple[bytes, bytes]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    pub = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return priv, pub


def _load_keys(settings: AuthSettings) -> tuple[bytes, bytes]:
    priv_path: Path | None = settings.jwt_private_key_path
    pub_path: Path | None = settings.jwt_public_key_path
    if priv_path and priv_path.exists() and pub_path and pub_path.exists():
        return priv_path.read_bytes(), pub_path.read_bytes()
    logger.warning(
        "auth.jwt.ephemeral_keys",
        detail=(
            "JWT key files not found — generating ephemeral RSA keypair. "
            "Tokens issued by this process will be invalid after restart. "
            "Mount real key files for production."
        ),
    )
    return _generate_ephemeral_keys()


def get_jwt_keys() -> tuple[bytes, bytes]:
    global _PRIVATE_KEY, _PUBLIC_KEY
    if _PRIVATE_KEY is None or _PUBLIC_KEY is None:
        _PRIVATE_KEY, _PUBLIC_KEY = _load_keys(get_settings().auth)
    return _PRIVATE_KEY, _PUBLIC_KEY


def reset_jwt_keys_for_tests() -> None:
    """Test helper — force re-generation on the next call."""
    global _PRIVATE_KEY, _PUBLIC_KEY
    _PRIVATE_KEY = None
    _PUBLIC_KEY = None


# --- token mint / verify -------------------------------------------------


def mint_access_token(
    *,
    user_id: UUID,
    org_id: UUID,
    role: str,
    scopes: list[str] | None = None,
) -> tuple[str, str, datetime]:
    """Returns (token, jti, expires_at)."""
    s = get_settings().auth
    now = datetime.now(UTC)
    exp = now + timedelta(seconds=s.access_token_ttl_seconds)
    jti = uuid4().hex
    payload: dict[str, Any] = {
        "iss": s.jwt_issuer,
        "aud": s.jwt_audience,
        "sub": str(user_id),
        "org": str(org_id),
        "role": role,
        "scopes": scopes or [],
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
        "kind": "access",
    }
    private_key, _ = get_jwt_keys()
    token = jwt.encode(payload, private_key, algorithm=s.jwt_algorithm)
    return token, jti, exp


def mint_refresh_token(*, user_id: UUID, org_id: UUID) -> tuple[str, str, datetime]:
    """Returns (opaque_token, hash, expires_at).

    The plaintext token is what the client stores; the hash is what we persist.
    """
    s = get_settings().auth
    now = datetime.now(UTC)
    exp = now + timedelta(seconds=s.refresh_token_ttl_seconds)
    raw = secrets.token_urlsafe(48)
    # Bind the refresh token to the user/org so a stolen token can't be used
    # against another account on a different host.
    token = f"{user_id}.{org_id}.{raw}"
    return token, hash_opaque_token(token), exp


def hash_opaque_token(token: str) -> str:
    """Stable, fast hash for opaque tokens (refresh, API key body).

    SHA-256 is sufficient — the token has 384+ bits of entropy and the storage
    cost of argon2 per refresh is unjustified. argon2id IS used for API keys
    where the key body is shorter and brute-forcing matters more.
    """
    return hashlib.sha256(token.encode()).hexdigest()


def verify_access_token(token: str) -> dict[str, Any]:
    s = get_settings().auth
    _, public_key = get_jwt_keys()
    try:
        payload = jwt.decode(
            token,
            public_key,
            algorithms=[s.jwt_algorithm],
            audience=s.jwt_audience,
            issuer=s.jwt_issuer,
        )
    except jwt.ExpiredSignatureError as exc:
        raise AuthenticationError("Token expired.") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthenticationError("Invalid token.") from exc
    if payload.get("kind") != "access":
        raise AuthenticationError("Wrong token kind.")
    return payload


# --- API keys ------------------------------------------------------------


def generate_api_key(*, is_test: bool = False) -> tuple[str, str, str]:
    """Generate a new API key.

    Returns (plaintext, prefix, body_hash). The plaintext is shown to the
    user once at creation time; the prefix is what we store and display
    on subsequent reads; the body_hash is what's used for verification.
    """
    s = get_settings().auth
    prefix_label = s.api_key_prefix_test if is_test else s.api_key_prefix_live
    body = secrets.token_urlsafe(32)
    plaintext = f"{prefix_label}{body}"
    # Public prefix is the static portion plus the first 8 chars of the body.
    prefix = f"{prefix_label}{body[:8]}"
    return plaintext, prefix, hash_password(plaintext)


def verify_api_key(stored_hash: str, plaintext: str) -> bool:
    return verify_password(stored_hash, plaintext)
