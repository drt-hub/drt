"""Tests for configured, process-shared state-store construction (#756)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from drt.config.base import HistoryConfig, ProjectConfig, StateConfig
from drt.state.dlq import LocalDlqStore
from drt.state.factory import build_state_bundle
from drt.state.history import LocalHistoryManager
from drt.state.manager import LocalStateManager


def test_local_backend_returns_existing_local_implementations(tmp_path: Path) -> None:
    bundle = build_state_bundle(ProjectConfig(name="test"), tmp_path)

    assert isinstance(bundle.state, LocalStateManager)
    assert isinstance(bundle.history, LocalHistoryManager)
    assert isinstance(bundle.dlq, LocalDlqStore)


def test_same_project_and_config_return_same_instances(tmp_path: Path) -> None:
    project = ProjectConfig(name="test")

    first = build_state_bundle(project, tmp_path)
    second = build_state_bundle(project, tmp_path / ".")

    assert first is second
    assert first.state is second.state
    assert first.history is second.history
    assert first.dlq is second.dlq


def test_different_project_dirs_return_different_instances(tmp_path: Path) -> None:
    project = ProjectConfig(name="test")
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"

    first = build_state_bundle(project, first_dir)
    second = build_state_bundle(project, second_dir)

    assert first is not second
    assert first.state is not second.state
    assert first.history is not second.history
    assert first.dlq is not second.dlq


@pytest.mark.parametrize(
    "field",
    [
        "bucket",
        "prefix",
        "region",
        "aws_profile",
        "aws_access_key_id_env",
        "aws_secret_access_key_env",
        "aws_session_token_env",
        "endpoint_url",
    ],
)
def test_local_backend_rejects_remote_only_fields(field: str) -> None:
    with pytest.raises(ValidationError, match="not valid when backend is 'local'"):
        StateConfig(**{field: "configured"})


def test_gcs_backend_requires_bucket() -> None:
    with pytest.raises(ValidationError, match="bucket is required.*gcs"):
        StateConfig(backend="gcs")


@pytest.mark.parametrize(
    "field",
    [
        "region",
        "aws_profile",
        "aws_access_key_id_env",
        "aws_secret_access_key_env",
        "aws_session_token_env",
        "endpoint_url",
    ],
)
def test_gcs_backend_rejects_s3_only_fields(field: str) -> None:
    with pytest.raises(ValidationError, match="only valid when backend is 's3'"):
        StateConfig(backend="gcs", bucket="state-bucket", **{field: "configured"})


def test_s3_backend_requires_bucket() -> None:
    with pytest.raises(ValidationError, match="bucket is required.*s3"):
        StateConfig(backend="s3")


def test_gcs_backend_builds_object_store_bundle(tmp_path: Path) -> None:
    from drt.state._objectstore import (
        ObjectStoreDlqBackend,
        ObjectStoreHistoryStore,
        ObjectStoreStateStore,
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(backend="gcs", bucket="state-bucket", prefix="team/drt"),
        history=HistoryConfig(max_entries=123),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert isinstance(bundle.state, ObjectStoreStateStore)
    assert isinstance(bundle.history, ObjectStoreHistoryStore)
    assert isinstance(bundle.dlq, ObjectStoreDlqBackend)
    assert bundle.state._client is bundle.history._client is bundle.dlq._client
    assert bundle.history._max_entries == 123


def test_s3_backend_builds_object_store_bundle_with_client_options(
    tmp_path: Path,
) -> None:
    from drt.state._objectstore import (
        ObjectStoreDlqBackend,
        ObjectStoreHistoryStore,
        ObjectStoreStateStore,
    )
    from drt.state.s3 import S3ObjectClient

    project = ProjectConfig(
        name="test",
        state=StateConfig(
            backend="s3",
            bucket="state-bucket",
            prefix="team/drt",
            region="ap-northeast-1",
            endpoint_url="http://localhost:4566",
            aws_profile="state",
            aws_access_key_id_env="STATE_AWS_KEY",
            aws_secret_access_key_env="STATE_AWS_SECRET",
            aws_session_token_env="STATE_AWS_TOKEN",
        ),
        history=HistoryConfig(max_entries=321),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert isinstance(bundle.state, ObjectStoreStateStore)
    assert isinstance(bundle.history, ObjectStoreHistoryStore)
    assert isinstance(bundle.dlq, ObjectStoreDlqBackend)
    assert bundle.state._client is bundle.history._client is bundle.dlq._client
    assert isinstance(bundle.state._client, S3ObjectClient)
    assert bundle.state._client._client_options == {
        "region": "ap-northeast-1",
        "endpoint_url": "http://localhost:4566",
        "aws_profile": "state",
        "aws_access_key_id_env": "STATE_AWS_KEY",
        "aws_secret_access_key_env": "STATE_AWS_SECRET",
        "aws_session_token_env": "STATE_AWS_TOKEN",
    }
    assert bundle.history._max_entries == 321


def test_history_max_entries_must_be_positive() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        HistoryConfig(max_entries=0)


def test_bypassed_unknown_backend_fails_explicitly(tmp_path: Path) -> None:
    state = StateConfig.model_construct(backend="future")
    project = ProjectConfig(name="test", state=state)

    with pytest.raises(NotImplementedError, match="future.*#756.*#920"):
        build_state_bundle(project, tmp_path)


def test_warehouse_backend_requires_connection_profile() -> None:
    with pytest.raises(ValidationError, match="state.connection_profile is required.*warehouse"):
        StateConfig(backend="warehouse")


@pytest.mark.parametrize(
    "field",
    ["bucket", "prefix", "region", "aws_profile", "endpoint_url"],
)
def test_warehouse_backend_rejects_object_store_fields(field: str) -> None:
    with pytest.raises(ValidationError, match="not valid when backend is 'warehouse'"):
        StateConfig(backend="warehouse", connection_profile="pg_main", **{field: "configured"})


def test_connection_profile_field_rejected_outside_warehouse_backend() -> None:
    with pytest.raises(ValidationError, match="state.connection_profile is only valid.*warehouse"):
        StateConfig(backend="local", connection_profile="pg_main")


def test_warehouse_backend_builds_postgres_bundle(tmp_path: Path, monkeypatch) -> None:
    from drt.config.credentials import PostgresProfile
    from drt.state.warehouse import (
        PostgresWarehouseDlqBackend,
        PostgresWarehouseHistoryStore,
        PostgresWarehouseStateStore,
    )

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(backend="warehouse", connection_profile="pg_main"),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert isinstance(bundle.state, PostgresWarehouseStateStore)
    assert isinstance(bundle.history, PostgresWarehouseHistoryStore)
    assert isinstance(bundle.dlq, PostgresWarehouseDlqBackend)
    assert bundle.state._profile is profile
    assert bundle.ledger is None


def test_idempotency_requires_warehouse_backend() -> None:
    with pytest.raises(ValidationError, match="state.idempotency is only valid.*warehouse"):
        StateConfig(backend="local", idempotency=True)


def test_warehouse_backend_with_idempotency_builds_ledger(tmp_path: Path, monkeypatch) -> None:
    from drt.config.credentials import PostgresProfile
    from drt.state.warehouse import PostgresWarehouseIdempotencyLedger

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(backend="warehouse", connection_profile="pg_main", idempotency=True),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert isinstance(bundle.ledger, PostgresWarehouseIdempotencyLedger)
    assert bundle.ledger._profile is profile


def test_idempotency_flag_differentiates_cache_key(tmp_path: Path, monkeypatch) -> None:
    """Two projects differing only in state.idempotency must not share a
    cached bundle — otherwise whichever built the bundle first silently
    decides `.ledger` for every later caller with the same connection
    profile, regardless of what its own config actually says."""
    from drt.config.credentials import PostgresProfile

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    without = build_state_bundle(
        ProjectConfig(
            name="test",
            state=StateConfig(backend="warehouse", connection_profile="pg_main"),
        ),
        tmp_path,
    )
    with_ledger = build_state_bundle(
        ProjectConfig(
            name="test",
            state=StateConfig(backend="warehouse", connection_profile="pg_main", idempotency=True),
        ),
        tmp_path,
    )

    assert without.ledger is None
    assert with_ledger.ledger is not None


def test_warehouse_backend_with_audit_trail_builds_it(tmp_path: Path, monkeypatch) -> None:
    from drt.config.base import AuditTrailConfig
    from drt.config.credentials import PostgresProfile
    from drt.state.warehouse import PostgresComplianceAuditTrail

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(
            backend="warehouse",
            connection_profile="pg_main",
            audit_trail=AuditTrailConfig(enabled=True, retain_days=30, fields=["email"]),
        ),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert isinstance(bundle.audit_trail, PostgresComplianceAuditTrail)
    assert bundle.audit_trail._profile is profile


def test_audit_trail_disabled_by_default(tmp_path: Path, monkeypatch) -> None:
    from drt.config.credentials import PostgresProfile

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(backend="warehouse", connection_profile="pg_main"),
    )
    bundle = build_state_bundle(project, tmp_path)

    assert bundle.audit_trail is None


def test_audit_trail_config_differentiates_cache_key(tmp_path: Path, monkeypatch) -> None:
    """Same non-novel gap as the idempotency-flag cache key test above:
    two projects differing only in state.audit_trail must not share a
    cached bundle, or one project's compliance policy silently applies (or
    fails to apply) to another's runs."""
    from drt.config.base import AuditTrailConfig
    from drt.config.credentials import PostgresProfile

    profile = PostgresProfile(type="postgres", host="h", dbname="d", user="u")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    without = build_state_bundle(
        ProjectConfig(
            name="test",
            state=StateConfig(backend="warehouse", connection_profile="pg_main"),
        ),
        tmp_path,
    )
    with_audit = build_state_bundle(
        ProjectConfig(
            name="test",
            state=StateConfig(
                backend="warehouse",
                connection_profile="pg_main",
                audit_trail=AuditTrailConfig(enabled=True, retain_days=30, fields=["email"]),
            ),
        ),
        tmp_path,
    )

    assert without.audit_trail is None
    assert with_audit.audit_trail is not None


def test_warehouse_backend_rejects_non_postgres_profiles(tmp_path: Path, monkeypatch) -> None:
    from drt.config.credentials import SnowflakeProfile

    profile = SnowflakeProfile(type="snowflake", account="a", user="u", database="d")
    monkeypatch.setattr(
        "drt.config.credentials.load_profile", lambda name, config_dir=None: profile
    )

    project = ProjectConfig(
        name="test",
        state=StateConfig(backend="warehouse", connection_profile="sf_main"),
    )

    with pytest.raises(NotImplementedError, match="only supports Postgres.*snowflake"):
        build_state_bundle(project, tmp_path)
