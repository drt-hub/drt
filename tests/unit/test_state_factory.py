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


@pytest.mark.parametrize("field", ["bucket", "prefix"])
def test_local_backend_rejects_remote_only_fields(field: str) -> None:
    with pytest.raises(ValidationError, match="not valid when backend is 'local'"):
        StateConfig(**{field: "configured"})


def test_gcs_backend_requires_bucket() -> None:
    with pytest.raises(ValidationError, match="bucket is required.*gcs"):
        StateConfig(backend="gcs")


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


def test_history_max_entries_must_be_positive() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        HistoryConfig(max_entries=0)


def test_bypassed_unknown_backend_fails_explicitly(tmp_path: Path) -> None:
    state = StateConfig.model_construct(backend="future")
    project = ProjectConfig(name="test", state=state)

    with pytest.raises(NotImplementedError, match="future.*#756"):
        build_state_bundle(project, tmp_path)
