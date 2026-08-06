"""GCP Secret Manager provider (#782) — resolves ``gcp-sm://`` URIs.

    password_env: "gcp-sm://projects/p/secrets/drt-sf/versions/latest#password"

Unlike AWS (where a bare secret id implicitly means its latest version),
GCP's ``AccessSecretVersion`` resource name always names a version
explicitly — ``ref.path`` is the whole
``projects/*/secrets/*/versions/*`` string, with ``latest`` as a real,
resolvable alias rather than an implicit default. An optional ``#key``
selects a field inside a JSON-valued secret, same as the AWS leg.

Requires: pip install drt-core[gcp-secrets]
"""

from __future__ import annotations

from typing import Any

from drt.config.secret_providers.base import SecretRef, extract_key


class GcpSecretManagerProvider:
    def __init__(self) -> None:
        self._client: Any | None = None

    def fetch(self, ref: SecretRef) -> str:
        client = self._get_client()
        # Lazy: only reachable once _get_client() has confirmed the extra is
        # installed, so google.api_core (a transitive dependency of
        # google-cloud-secret-manager) is guaranteed importable here.
        from google.api_core.exceptions import NotFound

        try:
            response = client.access_secret_version(request={"name": ref.path})
        except NotFound as e:
            raise LookupError(f"gcp-sm: no secret version found for '{ref.path}'") from e

        value = response.payload.data.decode("utf-8")
        return extract_key(value, ref, scheme="gcp-sm")

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from google.cloud import secretmanager  # type: ignore[import-untyped]
            except ImportError as e:
                raise ImportError(
                    "gcp-sm:// secret references require: pip install drt-core[gcp-secrets]"
                ) from e
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client
