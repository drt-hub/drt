"""URI-scheme secret provider resolution (#782).

    password_env: "aws-sm://prod/drt/snowflake#password"

Extends ``resolve_env``'s existing chain (explicit value -> OS env ->
secrets.toml) with one more fallback: if none of those resolved the value,
and the value looks like a ``scheme://`` URI for a registered provider,
fetch it from there. See ``drt.config.credentials.resolve_env``.

Each provider ships as an extra with a lazy SDK import (``drt-core[aws-secrets]``
etc.) — importing this package never imports a provider's SDK, only the
provider actually referenced by a profile does, at first use.
"""

from __future__ import annotations

from drt.config.secret_providers.aws import AwsSecretsManagerProvider
from drt.config.secret_providers.base import (
    SecretProvider,
    SecretRef,
    clear_cache,
    parse_secret_uri,
    register,
    resolve_provider_uri,
)

register("aws-sm", AwsSecretsManagerProvider())

__all__ = [
    "SecretProvider",
    "SecretRef",
    "clear_cache",
    "parse_secret_uri",
    "register",
    "resolve_provider_uri",
]
