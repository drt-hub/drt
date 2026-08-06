"""Tests for URI-scheme secret provider resolution (#782)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import resolve_env
from drt.config.secret_providers import base as sp_base
from drt.config.secret_providers.aws import AwsSecretsManagerProvider
from drt.config.secret_providers.base import (
    SecretRef,
    clear_cache,
    parse_secret_uri,
    register,
    resolve_provider_uri,
)


@pytest.fixture(autouse=True)
def _clear_provider_cache() -> Iterator[None]:
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def _fake_provider() -> Iterator[tuple[str, MagicMock]]:
    """Register a throwaway provider under a scheme unused by production code."""
    scheme = "test-scheme"
    provider = MagicMock()
    register(scheme, provider)
    yield scheme, provider
    del sp_base._registry[scheme]


# ---------------------------------------------------------------------------
# parse_secret_uri
# ---------------------------------------------------------------------------


class TestParseSecretUri:
    def test_aws_style_path_and_fragment(self) -> None:
        ref = parse_secret_uri("aws-sm://prod/drt/snowflake#password")
        assert ref == SecretRef(path="prod/drt/snowflake", key="password")

    def test_gcp_style_no_fragment(self) -> None:
        ref = parse_secret_uri("gcp-sm://projects/p/secrets/drt-sf/versions/latest")
        assert ref == SecretRef(path="projects/p/secrets/drt-sf/versions/latest", key=None)

    def test_vault_style_path_and_fragment(self) -> None:
        ref = parse_secret_uri("vault://secret/data/drt/snowflake#password")
        assert ref == SecretRef(path="secret/data/drt/snowflake", key="password")


# ---------------------------------------------------------------------------
# registry + dispatch
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_duplicate_scheme_registration_raises(self) -> None:
        register("test-dup-scheme", MagicMock())
        try:
            with pytest.raises(ValueError, match="already registered"):
                register("test-dup-scheme", MagicMock())
        finally:
            del sp_base._registry["test-dup-scheme"]

    def test_unregistered_scheme_returns_none(self) -> None:
        assert resolve_provider_uri("nope-sm://whatever") is None

    def test_registered_scheme_dispatches_with_parsed_ref(
        self, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.return_value = "the-secret"

        assert resolve_provider_uri(f"{scheme}://a/b#c") == "the-secret"
        provider.fetch.assert_called_once_with(SecretRef(path="a/b", key="c"))

    def test_result_is_cached_across_calls(self, _fake_provider: tuple[str, MagicMock]) -> None:
        scheme, provider = _fake_provider
        provider.fetch.return_value = "cached-value"

        assert resolve_provider_uri(f"{scheme}://x") == "cached-value"
        assert resolve_provider_uri(f"{scheme}://x") == "cached-value"
        provider.fetch.assert_called_once()

    def test_different_uris_are_cached_independently(
        self, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.side_effect = ["value-x", "value-y"]

        assert resolve_provider_uri(f"{scheme}://x") == "value-x"
        assert resolve_provider_uri(f"{scheme}://y") == "value-y"
        assert provider.fetch.call_count == 2


# ---------------------------------------------------------------------------
# AwsSecretsManagerProvider
# ---------------------------------------------------------------------------


def _fake_client(secret_string: str | None = "unset", *, not_found: bool = False) -> MagicMock:
    """A stand-in Secrets Manager client. ``secret_string=None`` models a
    binary secret (no ``SecretString`` key in the response at all)."""
    client = MagicMock()

    class _ResourceNotFoundException(Exception):
        pass

    client.exceptions.ResourceNotFoundException = _ResourceNotFoundException
    if not_found:
        client.get_secret_value.side_effect = _ResourceNotFoundException("not found")
    elif secret_string is None:
        client.get_secret_value.return_value = {}
    else:
        client.get_secret_value.return_value = {"SecretString": secret_string}
    return client


def _mock_boto3(client: MagicMock) -> dict[str, MagicMock]:
    boto3_mod = MagicMock()
    boto3_mod.client.return_value = client
    return {"boto3": boto3_mod}


class TestAwsSecretsManagerProvider:
    def test_plain_string_secret_no_key(self) -> None:
        client = _fake_client(secret_string="hunter2")
        with patch.dict("sys.modules", _mock_boto3(client)):
            value = AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key=None))
        assert value == "hunter2"
        client.get_secret_value.assert_called_once_with(SecretId="prod/drt/x")

    def test_json_secret_with_key_extracts_field(self) -> None:
        client = _fake_client(secret_string=json.dumps({"password": "hunter2", "user": "svc"}))
        with patch.dict("sys.modules", _mock_boto3(client)):
            value = AwsSecretsManagerProvider().fetch(
                SecretRef(path="prod/drt/x", key="password")
            )
        assert value == "hunter2"

    def test_json_secret_missing_key_raises(self) -> None:
        client = _fake_client(secret_string=json.dumps({"user": "svc"}))
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="key 'password' not found"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key="password"))

    def test_non_json_secret_with_key_requested_raises(self) -> None:
        client = _fake_client(secret_string="hunter2")
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="isn't JSON"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key="password"))

    def test_binary_secret_raises(self) -> None:
        client = _fake_client(secret_string=None)
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="no SecretString"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key=None))

    def test_missing_secret_raises_lookup_error(self) -> None:
        client = _fake_client(not_found=True)
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="no secret found"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/nope", key=None))

    def test_missing_boto3_raises_helpful_import_error(self) -> None:
        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[aws-secrets\]"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="x", key=None))

    def test_client_is_constructed_once_and_reused(self) -> None:
        client = _fake_client(secret_string="a")
        modules = _mock_boto3(client)
        provider = AwsSecretsManagerProvider()
        with patch.dict("sys.modules", modules):
            provider.fetch(SecretRef(path="x", key=None))
            provider.fetch(SecretRef(path="y", key=None))
        modules["boto3"].client.assert_called_once_with("secretsmanager")


# ---------------------------------------------------------------------------
# resolve_env integration
# ---------------------------------------------------------------------------


class TestResolveEnvIntegration:
    def test_plain_env_var_name_unaffected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_VAR", "value-from-env")
        assert resolve_env(None, "MY_VAR") == "value-from-env"

    def test_unregistered_scheme_falls_through_to_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("nope-sm://whatever", raising=False)
        assert resolve_env(None, "nope-sm://whatever") is None

    def test_registered_scheme_resolves_through_full_chain(
        self, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.return_value = "resolved-secret"
        assert resolve_env(None, f"{scheme}://a/b#c") == "resolved-secret"

    def test_explicit_value_beats_provider_uri(
        self, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        assert resolve_env("explicit", f"{scheme}://a/b") == "explicit"
        provider.fetch.assert_not_called()

    def test_os_env_beats_provider_uri(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        # A real env var happens to be named after a scheme URI — env still wins,
        # matching the existing env > secrets.toml precedence this step extends.
        scheme, provider = _fake_provider
        env_var_name = f"{scheme}://a/b"
        monkeypatch.setenv(env_var_name, "from-os-env")
        assert resolve_env(None, env_var_name) == "from-os-env"
        provider.fetch.assert_not_called()
