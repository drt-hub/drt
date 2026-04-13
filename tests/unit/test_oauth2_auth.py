"""Tests for OAuth2 Client Credentials auth."""

from __future__ import annotations

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import OAuth2ClientCredentialsAuth
from drt.destinations.auth import AuthHandler, _oauth2_cache


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    """Clear OAuth2 token cache between tests."""
    _oauth2_cache.clear()


def test_oauth2_token_exchange(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_CLIENT_ID", "test-id")
    monkeypatch.setenv("MY_CLIENT_SECRET", "test-secret")

    httpserver.expect_request("/oauth/token", method="POST").respond_with_json(
        {"access_token": "tok_abc123", "token_type": "Bearer", "expires_in": 3600}
    )

    auth = OAuth2ClientCredentialsAuth(
        type="oauth2_client_credentials",
        token_url=httpserver.url_for("/oauth/token"),
        client_id_env="MY_CLIENT_ID",
        client_secret_env="MY_CLIENT_SECRET",
    )
    headers = AuthHandler(auth).get_headers()
    assert headers == {"Authorization": "Bearer tok_abc123"}


def test_oauth2_with_scope(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CID", "id")
    monkeypatch.setenv("CSEC", "secret")

    httpserver.expect_request("/token", method="POST").respond_with_json(
        {"access_token": "scoped_tok", "expires_in": 3600}
    )

    auth = OAuth2ClientCredentialsAuth(
        type="oauth2_client_credentials",
        token_url=httpserver.url_for("/token"),
        client_id_env="CID",
        client_secret_env="CSEC",
        scope="contacts.write",
    )
    headers = AuthHandler(auth).get_headers()
    assert headers["Authorization"] == "Bearer scoped_tok"

    # Verify scope was sent
    req = httpserver.log[0][0]
    assert b"scope=contacts.write" in req.data


def test_oauth2_caches_token(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CID", "id")
    monkeypatch.setenv("CSEC", "secret")

    httpserver.expect_request("/token").respond_with_json(
        {"access_token": "cached_tok", "expires_in": 3600}
    )

    auth = OAuth2ClientCredentialsAuth(
        type="oauth2_client_credentials",
        token_url=httpserver.url_for("/token"),
        client_id_env="CID",
        client_secret_env="CSEC",
    )

    # First call — hits server
    h1 = AuthHandler(auth).get_headers()
    # Second call — should use cache (server only expects 1 request)
    h2 = AuthHandler(auth).get_headers()

    assert h1 == h2 == {"Authorization": "Bearer cached_tok"}
    assert len(httpserver.log) == 1


def test_oauth2_missing_client_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_ID", raising=False)
    monkeypatch.setenv("CSEC", "secret")

    auth = OAuth2ClientCredentialsAuth(
        type="oauth2_client_credentials",
        token_url="http://unused",
        client_id_env="MISSING_ID",
        client_secret_env="CSEC",
    )
    with pytest.raises(ValueError, match="MISSING_ID"):
        AuthHandler(auth).get_headers()


def test_oauth2_missing_client_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CID", "id")
    monkeypatch.delenv("MISSING_SEC", raising=False)

    auth = OAuth2ClientCredentialsAuth(
        type="oauth2_client_credentials",
        token_url="http://unused",
        client_id_env="CID",
        client_secret_env="MISSING_SEC",
    )
    with pytest.raises(ValueError, match="MISSING_SEC"):
        AuthHandler(auth).get_headers()
