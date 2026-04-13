"""AuthHandler — resolves AuthConfig to concrete HTTP headers.

Separates auth logic from the destination implementation for testability
and future Rust portability.
"""

from __future__ import annotations

import base64
import os

from drt.config.credentials import resolve_env

# Re-export AuthConfig type for convenience
from drt.config.models import (
    ApiKeyAuth,
    AuthConfig,  # noqa: F401
    BasicAuth,
    BearerAuth,
    OAuth2ClientCredentialsAuth,
)


class AuthHandler:
    """Resolve an AuthConfig to ready-to-use HTTP headers."""

    def __init__(self, auth: AuthConfig | None) -> None:
        self._auth = auth

    def get_headers(self) -> dict[str, str]:
        """Return resolved Authorization headers dict."""
        if self._auth is None:
            return {}

        auth = self._auth

        if isinstance(auth, BearerAuth):
            token = resolve_env(auth.token, auth.token_env)
            if not token:
                raise ValueError(
                    "BearerAuth: provide 'token' or set the env var named in 'token_env'."
                )
            return {"Authorization": f"Bearer {token}"}

        if isinstance(auth, ApiKeyAuth):
            value = resolve_env(auth.value, auth.value_env)
            if not value:
                raise ValueError(
                    "ApiKeyAuth: provide 'value' or set the env var named in 'value_env'."
                )
            return {auth.header: value}

        if isinstance(auth, BasicAuth):
            username = os.environ.get(auth.username_env, "")
            password = os.environ.get(auth.password_env, "")
            if not username:
                raise ValueError(
                    f"BasicAuth: env var '{auth.username_env}' is not set."
                )
            if not password:
                raise ValueError(
                    f"BasicAuth: env var '{auth.password_env}' is not set."
                )
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        if isinstance(auth, OAuth2ClientCredentialsAuth):
            return _get_oauth2_token(auth)

        return {}


# Cache: token_url → (access_token, expires_at)
_oauth2_cache: dict[str, tuple[str, float]] = {}


def _get_oauth2_token(auth: OAuth2ClientCredentialsAuth) -> dict[str, str]:
    """Exchange client credentials for an access token (with caching)."""
    import time

    import httpx

    from drt.config.credentials import resolve_env

    # Check cache
    cached = _oauth2_cache.get(auth.token_url)
    if cached:
        token, expires_at = cached
        if time.monotonic() < expires_at:
            return {"Authorization": f"Bearer {token}"}

    client_id = resolve_env(None, auth.client_id_env)
    client_secret = resolve_env(None, auth.client_secret_env)
    if not client_id:
        raise ValueError(
            f"OAuth2: env var '{auth.client_id_env}' is not set."
        )
    if not client_secret:
        raise ValueError(
            f"OAuth2: env var '{auth.client_secret_env}' is not set."
        )

    data: dict[str, str] = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if auth.scope:
        data["scope"] = auth.scope

    with httpx.Client(timeout=30.0) as client:
        response = client.post(auth.token_url, data=data)
        response.raise_for_status()

    token_data = response.json()
    access_token: str = token_data["access_token"]
    expires_in: int = token_data.get("expires_in", 3600)

    # Cache with 60s safety margin
    _oauth2_cache[auth.token_url] = (
        access_token,
        time.monotonic() + expires_in - 60,
    )

    return {"Authorization": f"Bearer {access_token}"}
