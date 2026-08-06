"""AWS Secrets Manager provider (#782) — resolves ``aws-sm://`` URIs.

    password_env: "aws-sm://prod/drt/snowflake#password"

``ref.path`` is the secret id (name or ARN) as accepted by
``GetSecretValue``; an optional ``#key`` selects a field inside a
JSON-valued secret (a secret holding several related credentials under one
id, rather than one id per field). No ``#key`` means the secret string
itself is the value.

Requires: pip install drt-core[aws-secrets]
"""

from __future__ import annotations

from typing import Any

from drt.config.secret_providers.base import SecretRef, extract_key


class AwsSecretsManagerProvider:
    def __init__(self) -> None:
        self._client: Any | None = None

    def fetch(self, ref: SecretRef) -> str:
        client = self._get_client()
        try:
            response = client.get_secret_value(SecretId=ref.path)
        except client.exceptions.ResourceNotFoundException as e:
            raise LookupError(f"aws-sm: no secret found for '{ref.path}'") from e

        value = response.get("SecretString")
        if value is None:
            # Binary secrets have no field structure to select a #key from.
            raise LookupError(f"aws-sm: secret '{ref.path}' has no SecretString value")

        return extract_key(str(value), ref, scheme="aws-sm")

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                import boto3  # type: ignore[import-untyped]
            except ImportError as e:
                raise ImportError(
                    "aws-sm:// secret references require: pip install drt-core[aws-secrets]"
                ) from e
            self._client = boto3.client("secretsmanager")
        return self._client
