"""Pure-Python tests for password hashing, JWT mint/verify, API key generation."""

from __future__ import annotations

from uuid import uuid4

import pytest

from app.core.errors import AuthenticationError
from app.modules.identity.security import (
    generate_api_key,
    hash_password,
    mint_access_token,
    needs_rehash,
    verify_access_token,
    verify_password,
)


@pytest.mark.unit
class TestPasswordHashing:
    def test_hash_then_verify_succeeds(self) -> None:
        h = hash_password("hunter2_with_some_length")
        assert verify_password(h, "hunter2_with_some_length")

    def test_wrong_password_rejected(self) -> None:
        h = hash_password("alpha-bravo-charlie")
        assert not verify_password(h, "alpha-bravo-delta")

    def test_hash_is_argon2id(self) -> None:
        h = hash_password("any-password-that-is-long-enough")
        assert h.startswith("$argon2id$")

    def test_needs_rehash_on_unrelated_string(self) -> None:
        assert needs_rehash("not-a-real-hash") is False


@pytest.mark.unit
class TestJWT:
    def test_mint_then_verify_roundtrip(self) -> None:
        token, jti, _ = mint_access_token(
            user_id=uuid4(), org_id=uuid4(), role="member", scopes=["org:read"]
        )
        payload = verify_access_token(token)
        assert payload["jti"] == jti
        assert payload["role"] == "member"
        assert payload["scopes"] == ["org:read"]

    def test_tampered_token_rejected(self) -> None:
        token, _, _ = mint_access_token(user_id=uuid4(), org_id=uuid4(), role="member")
        broken = token[:-2] + ("AB" if token[-2:] != "AB" else "CD")
        with pytest.raises(AuthenticationError):
            verify_access_token(broken)


@pytest.mark.unit
class TestApiKey:
    def test_live_prefix_visible(self) -> None:
        plaintext, prefix, h = generate_api_key(is_test=False)
        assert plaintext.startswith("eaip_live_")
        assert prefix.startswith("eaip_live_")
        assert verify_password(h, plaintext)

    def test_test_prefix(self) -> None:
        plaintext, prefix, _ = generate_api_key(is_test=True)
        assert plaintext.startswith("eaip_test_")
        assert prefix.startswith("eaip_test_")

    def test_keys_are_unique(self) -> None:
        a, _, _ = generate_api_key()
        b, _, _ = generate_api_key()
        assert a != b
