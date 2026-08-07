"""HashiCorp Vault provider (#782) — resolves ``vault://`` URIs.

    password_env: "vault://secret/data/drt/snowflake#password"

``ref.path`` mirrors Vault's own raw KV v2 HTTP path: mount point, the
literal ``data`` segment KV v2's API inserts to distinguish a data read
from a metadata/delete operation on the same secret, then the logical
secret path — the same shape ``vault kv get`` / curl examples against
Vault use, so it's what an operator configuring this already recognizes.
hvac's own ``read_secret_version()`` wants the mount and the logical path
passed separately, without that ``data`` segment, so it's split out here
rather than exposed as a parsing detail on every caller.

Connects via ``VAULT_ADDR`` / ``VAULT_TOKEN`` (``hvac.Client()``'s own
defaults) — no drt-specific config, same as the AWS/GCP legs relying on
their SDKs' ambient credential resolution. Unlike those two SDKs, the
client is *not* cached across calls — see :meth:`VaultProvider._get_client`.

A ``#key`` is required, unlike AWS/GCP: a KV v2 secret's payload is always
a field map (there's no "the secret is just one string" case to fall back
to), so there's no sensible value to return without naming which field.

Requires: pip install drt-core[vault]
"""

from __future__ import annotations

from typing import Any

from drt.config.secret_providers.base import SecretRef, select_field


class VaultProvider:
    def fetch(self, ref: SecretRef) -> str:
        if ref.key is None:
            raise LookupError(
                f"vault: '{ref.path}' has no #key — a Vault KV v2 secret is a "
                "field map, so a field must be named (e.g. '#password')"
            )

        mount_point, secret_path = _split_kv2_path(ref.path)
        client = self._get_client()
        # Lazy: only reachable once _get_client() has confirmed the extra is
        # installed.
        from hvac.exceptions import InvalidPath

        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=secret_path, mount_point=mount_point
            )
        except InvalidPath as e:
            raise LookupError(f"vault: no secret found at '{ref.path}'") from e

        payload = response["data"]["data"]
        return select_field(payload, ref, scheme="vault")

    def _get_client(self) -> Any:
        # Deliberately not cached on self, unlike AwsSecretsManagerProvider
        # and GcpSecretManagerProvider. boto3 and google-auth both refresh
        # their own credentials internally, so a process-lifetime client is
        # harmless there. hvac.Client() captures VAULT_TOKEN once at
        # construction and never refreshes it, and Vault service tokens are
        # conventionally short-TTL — a cached client under a long-lived
        # process (`drt serve`) would go from "may resolve a rotated
        # secret's stale value for up to DRT_SECRET_CACHE_TTL_SECONDS"
        # (base._value_cache's TTL, #929) to "every fetch fails outright"
        # once the token expires, which is worse and specific to this leg.
        # Construction itself is cheap (no network call — it just wraps
        # connection params), so paying it every call is simpler than adding
        # token-renewal handling here.
        try:
            import hvac  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "vault:// secret references require: pip install drt-core[vault]"
            ) from e
        return hvac.Client()


def _split_kv2_path(path: str) -> tuple[str, str]:
    """Split ``mount/data/secret/path`` into ``(mount, "secret/path")``.

    The literal ``data`` segment is Vault's own KV v2 API convention, not
    part of the logical secret path — present so the raw path an operator
    already knows from ``vault kv get`` / curl works unmodified as the URI,
    while hvac's higher-level client wants the two pieces apart.
    """
    parts = path.split("/")
    if len(parts) < 3 or parts[1] != "data":
        raise LookupError(
            f"vault: expected 'mount/data/path', got '{path}' — the literal "
            "'data' segment (Vault's own KV v2 API convention) is required"
        )
    return parts[0], "/".join(parts[2:])
