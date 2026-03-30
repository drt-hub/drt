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

        return {}
