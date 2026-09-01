"""Tests for URI-scheme secret provider resolution (#782)."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import resolve_env
from drt.config.secret_providers import base as sp_base
from drt.config.secret_providers.aws import AwsSecretsManagerProvider
from drt.config.secret_providers.base import (
    SecretRef,
    clear_cache,
    extract_key,
    parse_secret_uri,
    register,
    resolve_provider_uri,
    select_field,
)
from drt.config.secret_providers.gcp import GcpSecretManagerProvider
from drt.config.secret_providers.vault import VaultProvider, _split_kv2_path


@pytest.fixture(autouse=True)
def _clear_provider_cache(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("DRT_SECRET_CACHE_TTL_SECONDS", raising=False)
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
# extract_key — shared JSON-field selection, used by every provider
# ---------------------------------------------------------------------------


class TestExtractKey:
    def test_no_key_returns_raw_unchanged(self) -> None:
        ref = SecretRef(path="x", key=None)
        assert extract_key("not json at all", ref, scheme="test-sm") == "not json at all"

    def test_key_extracts_field(self) -> None:
        ref = SecretRef(path="x", key="password")
        raw = json.dumps({"password": "hunter2", "user": "svc"})
        assert extract_key(raw, ref, scheme="test-sm") == "hunter2"

    def test_key_requested_but_value_not_json_raises(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="isn't JSON"):
            extract_key("plain-string", ref, scheme="test-sm")

    def test_key_missing_from_json_object_raises(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="key 'password' not found"):
            extract_key(json.dumps({"user": "svc"}), ref, scheme="test-sm")

    def test_key_requested_but_json_is_not_an_object_raises(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="key 'password' not found"):
            extract_key(json.dumps(["password", "hunter2"]), ref, scheme="test-sm")

    def test_null_value_at_key_raises_rather_than_stringifying(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="isn't a plain value"):
            extract_key(json.dumps({"password": None}), ref, scheme="test-sm")

    def test_nested_object_at_key_raises_rather_than_stringifying(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="isn't a plain value"):
            extract_key(json.dumps({"password": {"nested": "oops"}}), ref, scheme="test-sm")

    def test_error_message_names_the_scheme_actually_used(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match=r"^vault:"):
            extract_key("plain-string", ref, scheme="vault")


# ---------------------------------------------------------------------------
# select_field — the dict-native half extract_key delegates to; also used
# directly by providers (Vault) whose payload never needs a JSON-parse step
# ---------------------------------------------------------------------------


class TestSelectField:
    def test_extracts_field(self) -> None:
        ref = SecretRef(path="x", key="password")
        assert select_field({"password": "hunter2", "user": "svc"}, ref, scheme="vault") == (
            "hunter2"
        )

    def test_missing_key_raises(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="key 'password' not found"):
            select_field({"user": "svc"}, ref, scheme="vault")

    def test_null_value_raises_rather_than_stringifying(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="isn't a plain value"):
            select_field({"password": None}, ref, scheme="vault")

    def test_nested_object_value_raises_rather_than_stringifying(self) -> None:
        ref = SecretRef(path="x", key="password")
        with pytest.raises(LookupError, match="isn't a plain value"):
            select_field({"password": {"nested": "oops"}}, ref, scheme="vault")

    def test_non_string_scalar_is_stringified(self) -> None:
        ref = SecretRef(path="x", key="port")
        assert select_field({"port": 5432}, ref, scheme="vault") == "5432"


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

    def test_resolve_provider_uri_fires_secret_accessed_audit_event(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        """#299 / ADR 0008: fires on an actual provider fetch, never with
        the resolved value in the event details."""
        from drt.observability.audit import AuditEvent, register_audit_logger

        scheme, provider = _fake_provider
        provider.fetch.return_value = "the-secret"
        monkeypatch.setenv("DRT_SECRET_CACHE_TTL_SECONDS", "0")
        captured: list[AuditEvent] = []

        class _Capturing:
            def log_event(self, event: AuditEvent) -> None:
                captured.append(event)

        register_audit_logger(_Capturing())
        try:
            resolve_provider_uri(f"{scheme}://a/b#c")
        finally:
            from drt.observability.audit import _reset_audit_logger

            _reset_audit_logger()

        assert len(captured) == 1
        assert captured[0].event_type == "secret_accessed"
        assert captured[0].details == {"scheme": scheme, "path": "a/b"}
        assert "the-secret" not in captured[0].details.values()

    def test_audit_logger_resolving_its_own_secret_does_not_deadlock(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        """Regression for the P1 Codex review finding on this PR: an
        Enterprise AuditLogger.log_event() that itself calls
        resolve_provider_uri() (e.g. to fetch its sink's credentials) must
        not deadlock on _value_cache_lock — the audit call now fires after
        that lock is released. Run on a background thread with a bounded
        join so a regression fails the test instead of hanging the suite.
        """
        import threading

        from drt.observability.audit import (
            AuditEvent,
            _reset_audit_logger,
            register_audit_logger,
        )

        scheme, provider = _fake_provider
        provider.fetch.return_value = "the-secret"
        # Cache enabled with a real TTL — this is the branch that held
        # _value_cache_lock around the fetch before the fix.
        monkeypatch.delenv("DRT_SECRET_CACHE_TTL_SECONDS", raising=False)

        class _NestedFetchAuditLogger:
            def log_event(self, event: AuditEvent) -> None:
                # Deliberately re-enters resolve_provider_uri from inside
                # the audit callback, on a different (uncached) URI so it
                # forces a second, nested lock acquisition attempt.
                resolve_provider_uri(f"{scheme}://sink-credential")

        register_audit_logger(_NestedFetchAuditLogger())
        result: list[str | None] = [None]
        exc: list[BaseException] = []

        def run() -> None:
            try:
                result[0] = resolve_provider_uri(f"{scheme}://a/b#c")
            except BaseException as e:  # noqa: BLE001 — captured for the assertion below
                exc.append(e)

        try:
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            thread.join(timeout=5)
            assert not thread.is_alive(), (
                "resolve_provider_uri deadlocked — the audit callback's nested "
                "call is still holding/waiting on _value_cache_lock"
            )
        finally:
            _reset_audit_logger()

        assert exc == []
        assert result[0] == "the-secret"

    def test_secret_resolution_survives_audit_logger_failure(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        """Regression for the other P1 Codex review finding: a raising
        AuditLogger must not fail the secret fetch it's auditing —
        AuditLogger.log_event's best-effort contract is enforced at the
        call site, not left to every future implementation."""
        from drt.observability.audit import AuditEvent, register_audit_logger

        scheme, provider = _fake_provider
        provider.fetch.return_value = "the-secret"
        monkeypatch.setenv("DRT_SECRET_CACHE_TTL_SECONDS", "0")

        class _BrokenSink:
            def log_event(self, event: AuditEvent) -> None:
                raise RuntimeError("sink unreachable")

        register_audit_logger(_BrokenSink())
        try:
            result = resolve_provider_uri(f"{scheme}://a/b#c")
        finally:
            from drt.observability.audit import _reset_audit_logger

            _reset_audit_logger()

        assert result == "the-secret"

    def test_result_is_cached_before_ttl_expiry(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.return_value = "cached-value"
        monotonic = MagicMock(return_value=100.0)
        monkeypatch.setattr(sp_base.time, "monotonic", monotonic)
        monkeypatch.setenv("DRT_SECRET_CACHE_TTL_SECONDS", "300")

        assert resolve_provider_uri(f"{scheme}://x") == "cached-value"
        monotonic.return_value = 399.0
        assert resolve_provider_uri(f"{scheme}://x") == "cached-value"
        provider.fetch.assert_called_once()

    def test_result_is_refetched_after_ttl_expiry(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.side_effect = ["initial-value", "rotated-value"]
        monotonic = MagicMock(return_value=100.0)
        monkeypatch.setattr(sp_base.time, "monotonic", monotonic)
        monkeypatch.setenv("DRT_SECRET_CACHE_TTL_SECONDS", "300")

        assert resolve_provider_uri(f"{scheme}://x") == "initial-value"
        monotonic.return_value = 400.1
        assert resolve_provider_uri(f"{scheme}://x") == "rotated-value"
        assert provider.fetch.call_count == 2

    def test_concurrent_cache_misses_fetch_once(
        self, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        worker_count = 8
        barrier = threading.Barrier(worker_count)
        results: list[str | None] = [None] * worker_count

        def slow_fetch(ref: SecretRef) -> str:
            time.sleep(0.05)
            return "shared-value"

        def resolve(index: int) -> None:
            barrier.wait()
            results[index] = resolve_provider_uri(f"{scheme}://x")

        provider.fetch.side_effect = slow_fetch
        threads = [threading.Thread(target=resolve, args=(i,)) for i in range(worker_count)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert results == ["shared-value"] * worker_count
        provider.fetch.assert_called_once_with(SecretRef(path="x", key=None))

    def test_non_positive_ttl_disables_caching(
        self, monkeypatch: pytest.MonkeyPatch, _fake_provider: tuple[str, MagicMock]
    ) -> None:
        scheme, provider = _fake_provider
        provider.fetch.side_effect = ["initial-value", "rotated-value"]
        monkeypatch.setenv("DRT_SECRET_CACHE_TTL_SECONDS", "0")

        assert resolve_provider_uri(f"{scheme}://x") == "initial-value"
        assert resolve_provider_uri(f"{scheme}://x") == "rotated-value"
        assert provider.fetch.call_count == 2

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
            value = AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key="password"))
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

    def test_json_secret_null_value_at_key_raises_rather_than_stringifying(self) -> None:
        client = _fake_client(secret_string=json.dumps({"password": None}))
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="isn't a plain value"):
                AwsSecretsManagerProvider().fetch(SecretRef(path="prod/drt/x", key="password"))

    def test_json_secret_nested_object_at_key_raises_rather_than_stringifying(self) -> None:
        client = _fake_client(secret_string=json.dumps({"password": {"nested": "oops"}}))
        with patch.dict("sys.modules", _mock_boto3(client)):
            with pytest.raises(LookupError, match="isn't a plain value"):
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
# GcpSecretManagerProvider
# ---------------------------------------------------------------------------


class _NotFound(Exception):
    """Stand-in for google.api_core.exceptions.NotFound."""


def _fake_gcp_client(secret_value: str | None = "unset", *, not_found: bool = False) -> MagicMock:
    client = MagicMock()
    if not_found:
        client.access_secret_version.side_effect = _NotFound("not found")
    else:
        response = MagicMock()
        response.payload.data = (secret_value or "").encode("utf-8")
        client.access_secret_version.return_value = response
    return client


def _mock_gcp_modules(client: MagicMock) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy ``from google.cloud import
    secretmanager`` and ``from google.api_core.exceptions import NotFound``."""
    secretmanager_mod = MagicMock()
    secretmanager_mod.SecretManagerServiceClient.return_value = client

    google_cloud_mod = MagicMock()
    google_cloud_mod.secretmanager = secretmanager_mod

    exceptions_mod = MagicMock()
    exceptions_mod.NotFound = _NotFound

    api_core_mod = MagicMock()
    api_core_mod.exceptions = exceptions_mod

    google_mod = MagicMock()
    google_mod.cloud = google_cloud_mod
    google_mod.api_core = api_core_mod

    return {
        "google": google_mod,
        "google.cloud": google_cloud_mod,
        "google.cloud.secretmanager": secretmanager_mod,
        "google.api_core": api_core_mod,
        "google.api_core.exceptions": exceptions_mod,
    }


class TestGcpSecretManagerProvider:
    def test_plain_string_secret_no_key(self) -> None:
        client = _fake_gcp_client(secret_value="hunter2")
        with patch.dict("sys.modules", _mock_gcp_modules(client)):
            value = GcpSecretManagerProvider().fetch(
                SecretRef(path="projects/p/secrets/x/versions/latest", key=None)
            )
        assert value == "hunter2"
        client.access_secret_version.assert_called_once_with(
            request={"name": "projects/p/secrets/x/versions/latest"}
        )

    def test_json_secret_with_key_extracts_field(self) -> None:
        client = _fake_gcp_client(secret_value=json.dumps({"password": "hunter2"}))
        with patch.dict("sys.modules", _mock_gcp_modules(client)):
            value = GcpSecretManagerProvider().fetch(
                SecretRef(path="projects/p/secrets/x/versions/latest", key="password")
            )
        assert value == "hunter2"

    def test_missing_secret_version_raises_lookup_error(self) -> None:
        client = _fake_gcp_client(not_found=True)
        with patch.dict("sys.modules", _mock_gcp_modules(client)):
            with pytest.raises(LookupError, match="no secret version found"):
                GcpSecretManagerProvider().fetch(
                    SecretRef(path="projects/p/secrets/nope/versions/latest", key=None)
                )

    def test_missing_extra_raises_helpful_import_error(self) -> None:
        with patch.dict("sys.modules", {"google.cloud.secretmanager": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[gcp-secrets\]"):
                GcpSecretManagerProvider().fetch(
                    SecretRef(path="projects/p/secrets/x/versions/latest", key=None)
                )

    def test_client_is_constructed_once_and_reused(self) -> None:
        client = _fake_gcp_client(secret_value="a")
        modules = _mock_gcp_modules(client)
        provider = GcpSecretManagerProvider()
        with patch.dict("sys.modules", modules):
            provider.fetch(SecretRef(path="projects/p/secrets/x/versions/latest", key=None))
            provider.fetch(SecretRef(path="projects/p/secrets/y/versions/latest", key=None))
        modules["google.cloud.secretmanager"].SecretManagerServiceClient.assert_called_once()


# ---------------------------------------------------------------------------
# _split_kv2_path
# ---------------------------------------------------------------------------


class TestSplitKv2Path:
    def test_default_mount(self) -> None:
        assert _split_kv2_path("secret/data/drt/snowflake") == ("secret", "drt/snowflake")

    def test_custom_mount(self) -> None:
        assert _split_kv2_path("kv2-custom/data/drt/snowflake") == (
            "kv2-custom",
            "drt/snowflake",
        )

    def test_deeply_nested_path(self) -> None:
        assert _split_kv2_path("secret/data/team/service/creds") == (
            "secret",
            "team/service/creds",
        )

    def test_missing_data_segment_raises(self) -> None:
        with pytest.raises(LookupError, match="'data' segment"):
            _split_kv2_path("secret/drt/snowflake")

    def test_too_short_raises(self) -> None:
        with pytest.raises(LookupError, match="'data' segment"):
            _split_kv2_path("secret/data")


# ---------------------------------------------------------------------------
# VaultProvider
# ---------------------------------------------------------------------------


class _InvalidPath(Exception):
    """Stand-in for hvac.exceptions.InvalidPath."""


def _fake_hvac_client(data: dict[str, Any] | None = None, *, not_found: bool = False) -> MagicMock:
    client = MagicMock()
    if not_found:
        client.secrets.kv.v2.read_secret_version.side_effect = _InvalidPath("no handler")
    else:
        client.secrets.kv.v2.read_secret_version.return_value = {"data": {"data": data or {}}}
    return client


def _mock_hvac_modules(client: MagicMock) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy ``import hvac`` and
    ``from hvac.exceptions import InvalidPath``."""
    exceptions_mod = MagicMock()
    exceptions_mod.InvalidPath = _InvalidPath

    hvac_mod = MagicMock()
    hvac_mod.Client.return_value = client
    hvac_mod.exceptions = exceptions_mod

    return {"hvac": hvac_mod, "hvac.exceptions": exceptions_mod}


class TestVaultProvider:
    def test_successful_fetch_extracts_field(self) -> None:
        client = _fake_hvac_client(data={"password": "hunter2", "user": "svc"})
        with patch.dict("sys.modules", _mock_hvac_modules(client)):
            value = VaultProvider().fetch(
                SecretRef(path="secret/data/drt/snowflake", key="password")
            )
        assert value == "hunter2"
        client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="drt/snowflake", mount_point="secret"
        )

    def test_custom_mount_point_is_passed_through(self) -> None:
        client = _fake_hvac_client(data={"password": "hunter2"})
        with patch.dict("sys.modules", _mock_hvac_modules(client)):
            VaultProvider().fetch(SecretRef(path="kv2-custom/data/drt/snowflake", key="password"))
        client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="drt/snowflake", mount_point="kv2-custom"
        )

    def test_no_key_raises(self) -> None:
        with pytest.raises(LookupError, match="has no #key"):
            VaultProvider().fetch(SecretRef(path="secret/data/drt/snowflake", key=None))

    def test_invalid_path_shape_raises_before_any_client_call(self) -> None:
        with pytest.raises(LookupError, match="'data' segment"):
            VaultProvider().fetch(SecretRef(path="secret/drt/snowflake", key="password"))

    def test_not_found_raises_lookup_error(self) -> None:
        client = _fake_hvac_client(not_found=True)
        with patch.dict("sys.modules", _mock_hvac_modules(client)):
            with pytest.raises(LookupError, match="no secret found"):
                VaultProvider().fetch(SecretRef(path="secret/data/nope", key="password"))

    def test_missing_extra_raises_helpful_import_error(self) -> None:
        with patch.dict("sys.modules", {"hvac": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[vault\]"):
                VaultProvider().fetch(SecretRef(path="secret/data/x", key="password"))

    def test_client_is_constructed_fresh_every_fetch(self) -> None:
        """Unlike AWS/GCP: hvac.Client() doesn't refresh VAULT_TOKEN on its
        own, so caching it would go stale under a long-lived `drt serve`
        rather than just serving a possibly-rotated value (#929)."""
        client = _fake_hvac_client(data={"password": "a"})
        modules = _mock_hvac_modules(client)
        provider = VaultProvider()
        with patch.dict("sys.modules", modules):
            provider.fetch(SecretRef(path="secret/data/x", key="password"))
            provider.fetch(SecretRef(path="secret/data/y", key="password"))
        assert modules["hvac"].Client.call_count == 2


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

    def test_explicit_value_beats_provider_uri(self, _fake_provider: tuple[str, MagicMock]) -> None:
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
