"""Engine wiring for ``sync.incremental_strategy: diff`` (#755).

The dialect SQL diff computation is covered by the Postgres local smoke and
the gated Snowflake DWH smoke (a mock cursor cannot prove JOIN/hash syntax).
These tests cover what the engine itself is responsible for: routing
added+changed into the normal batch/load path unchanged, masking and surfacing
removed_keys on SyncResult, and the commit-only-on-success gating.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync
from drt.sources.base import SnapshotDiffResult


class FakeSnapshotDiffSource:
    """Implements Source + SnapshotDiffSource, pre-scripted with a fixed result."""

    def __init__(
        self,
        added: list[dict],
        changed: list[dict],
        removed_keys: list[dict],
        snapshot_rows: list[dict] | None = None,
        snapshot_columns: list[str] | None = None,
        is_first_run: bool = False,
    ) -> None:
        self._added = added
        self._changed = changed
        self._removed_keys = removed_keys
        self._snapshot_rows = snapshot_rows if snapshot_rows is not None else added + changed
        self._snapshot_columns = snapshot_columns
        self._is_first_run = is_first_run
        self.commit_calls: list[str] = []
        self.extract_calls: list[dict[str, Any]] = []

    def extract(
        self, query: str, config: ProfileConfig, *, query_tags: dict[str, str] | None = None
    ) -> Iterator[dict]:
        raise AssertionError("extract() should not be called for incremental_strategy: diff")

    def test_connection(self, config: ProfileConfig) -> bool:
        return True

    def extract_snapshot_diff(
        self,
        query: str,
        config: ProfileConfig,
        *,
        sync_name: str,
        key_columns: list[str],
        hash_columns: Any,
        query_tags: dict[str, str] | None = None,
    ) -> SnapshotDiffResult:
        self.extract_calls.append(
            {"sync_name": sync_name, "key_columns": key_columns, "hash_columns": hash_columns}
        )
        return SnapshotDiffResult(
            snapshot_sample=iter(self._snapshot_rows[:1]),
            added=iter(self._added),
            changed=iter(self._changed),
            removed_keys=iter(self._removed_keys),
            is_first_run=self._is_first_run,
            snapshot_columns=self._snapshot_columns,
        )

    def commit_snapshot_diff(self, config: ProfileConfig, sync_name: str) -> None:
        self.commit_calls.append(sync_name)


class FakeDestination:
    def __init__(self, fail_indices: set[int] | None = None) -> None:
        self.calls: list[list[dict]] = []
        self._fail_indices = fail_indices or set()

    def load(
        self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        self.calls.append(records)
        result = SyncResult()
        for i, _ in enumerate(records):
            global_idx = sum(len(c) for c in self.calls[:-1]) + i
            if global_idx in self._fail_indices:
                result.failed += 1
                result.errors.append(f"Forced failure at index {global_idx}")
            else:
                result.success += 1
        return result


class FakeMirrorDestination(FakeDestination):
    def __init__(self) -> None:
        super().__init__()
        self.deleted: list[dict[str, Any]] = []

    def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
        self.deleted.extend(sync_options._diff_removed_keys or [])
        return SyncResult()

    def supported_modes(self) -> frozenset[str]:
        return frozenset({"mirror"})


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_diff_sync(
    mode: str = "upsert",
    batch_size: int = 10,
    mask: dict[str, Any] | None = None,
    columns: list[dict[str, str]] | None = None,
) -> SyncConfig:
    sync_opts: dict[str, Any] = {
        "mode": mode,
        "incremental_strategy": "diff",
        "batch_size": batch_size,
        **({"mirror": {"strategy": "destination"}} if mode == "mirror" else {}),
    }
    if mask:
        sync_opts["mask"] = mask
    config: dict[str, Any] = {
        "name": "diff_sync",
        "model": "ref('table')",
        "destination": {
            "type": "postgres",
            "host": "localhost",
            "dbname": "db",
            "user": "u",
            "password": "p",
            "table": "public.scores",
            "upsert_key": ["id"],
        },
        "sync": sync_opts,
    }
    if columns is not None:
        config["columns"] = columns
        config["contract"] = {"enforced": True}
    return SyncConfig.model_validate(config)


def test_diff_strategy_chains_added_and_changed_into_the_load_path(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(
        added=[{"id": 1}, {"id": 2}],
        changed=[{"id": 3}],
        removed_keys=[{"id": 4}],
    )
    dest = FakeDestination()
    sync = _make_diff_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    all_sent = [r for batch in dest.calls for r in batch]
    assert {r["id"] for r in all_sent} == {1, 2, 3}
    assert result.success == 3
    assert result.diff_removed_keys == [{"id": 4}]


def test_diff_removed_keys_are_masked_like_the_main_record_batch(tmp_path: Path) -> None:
    """Regression for a Codex-review finding on #1110:
    _finalize_mirror_diff deletes destination rows using these exact key
    values. If `upsert_key` is also a masked column, the destination stores
    the *masked* value (mask applies to every batch before load()), so an
    unmasked delete key would never match the row actually sitting there —
    permanently stranding it. Removed keys must go through the same mask
    transform the main record_batch does."""
    source = FakeSnapshotDiffSource(
        added=[],
        changed=[],
        removed_keys=[{"id": "alice@example.com"}],
    )
    dest = FakeDestination()
    sync = _make_diff_sync(mask={"id": "hash"})

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.diff_removed_keys is not None
    assert result.diff_removed_keys[0]["id"] != "alice@example.com"
    # hash strategy is deterministic -- same value, hashed the same way
    # apply_mask would hash it directly, proven by re-running it.
    from drt.engine.masking import apply_mask

    expected = apply_mask([{"id": "alice@example.com"}], {"id": "hash"})
    assert result.diff_removed_keys == expected


def test_diff_strategy_commits_snapshot_on_success(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(added=[{"id": 1}], changed=[], removed_keys=[])
    dest = FakeDestination()
    sync = _make_diff_sync()

    run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert source.commit_calls == ["diff_sync"]


def test_diff_contract_failure_blocks_removal_and_snapshot_commit(tmp_path: Path) -> None:
    """An unchanged row still defines the full snapshot's source schema."""

    class MirrorDestination(FakeDestination):
        def __init__(self) -> None:
            super().__init__()
            self.deleted: list[dict[str, Any]] = []

        def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
            self.deleted.extend(sync_options._diff_removed_keys or [])
            return SyncResult()

        def supported_modes(self) -> frozenset[str]:
            return frozenset({"mirror"})

    source = FakeSnapshotDiffSource(
        added=[],
        changed=[],
        removed_keys=[{"id": 2}],
        snapshot_rows=[{"id": 1}],
    )
    destination = MirrorDestination()
    sync = SyncConfig.model_validate(
        {
            "name": "diff_sync",
            "model": "ref('table')",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "email", "type": "string"},
            ],
            "contract": {"enforced": True},
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "db",
                "user": "u",
                "password": "p",
                "table": "public.scores",
                "upsert_key": ["id"],
            },
            "sync": {
                "mode": "mirror",
                "incremental_strategy": "diff",
                "diff": {"hash_columns": ["id"]},
                "mirror": {"strategy": "diff"},
            },
        }
    )

    result = run_sync(sync, source, destination, _make_profile(), tmp_path)

    assert result.failed == 1
    assert "'email' expected string, actual missing" in result.errors[0]
    assert destination.calls == []
    assert destination.deleted == []
    assert source.commit_calls == []


def test_empty_diff_snapshot_schema_failure_blocks_removals(tmp_path: Path) -> None:
    """An empty changed snapshot must not turn schema drift into delete-all."""

    class MirrorDestination(FakeDestination):
        def __init__(self) -> None:
            super().__init__()
            self.deleted: list[dict[str, Any]] = []

        def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
            self.deleted.extend(sync_options._diff_removed_keys or [])
            return SyncResult()

        def supported_modes(self) -> frozenset[str]:
            return frozenset({"mirror"})

    source = FakeSnapshotDiffSource(
        added=[],
        changed=[],
        removed_keys=[{"id": 1}],
        snapshot_rows=[],
        snapshot_columns=["id", "renamed_email"],
    )
    destination = MirrorDestination()
    sync = SyncConfig.model_validate(
        {
            "name": "diff_sync",
            "model": "ref('table')",
            "columns": [
                {"name": "id", "type": "integer"},
                {"name": "email", "type": "string"},
            ],
            "contract": {"enforced": True},
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "db",
                "user": "u",
                "password": "p",
                "table": "public.scores",
                "upsert_key": ["id"],
            },
            "sync": {
                "mode": "mirror",
                "incremental_strategy": "diff",
                "diff": {"hash_columns": ["id"]},
                "mirror": {"strategy": "diff"},
            },
        }
    )

    result = run_sync(sync, source, destination, _make_profile(), tmp_path)

    assert result.failed == 1
    assert "'email' expected string, actual missing" in result.errors[0]
    assert "'renamed_email' expected not declared, actual null" in result.errors[0]
    assert destination.calls == []
    assert destination.deleted == []
    assert source.commit_calls == []


def test_legacy_diff_result_without_snapshot_metadata_only_fails_when_empty(
    tmp_path: Path,
) -> None:
    """An old plugin cannot authorize deletes without blocking non-empty diffs."""

    class PluginSnapshotDiffResult:
        def __init__(
            self, added: list[dict], changed: list[dict], removed_keys: list[dict]
        ) -> None:
            self.added = iter(added)
            self.changed = iter(changed)
            self.removed_keys = iter(removed_keys)
            self.is_first_run = False

    class PluginSnapshotDiffSource(FakeSnapshotDiffSource):
        def extract_snapshot_diff(
            self,
            query: str,
            config: ProfileConfig,
            *,
            sync_name: str,
            key_columns: list[str],
            hash_columns: Any,
            query_tags: dict[str, str] | None = None,
        ) -> Any:
            return PluginSnapshotDiffResult(self._added, self._changed, self._removed_keys)

    source = PluginSnapshotDiffSource(added=[], changed=[], removed_keys=[{"id": 1}])
    destination = FakeMirrorDestination()
    sync = _make_diff_sync(mode="mirror", columns=[{"name": "id", "type": "integer"}])

    result = run_sync(sync, source, destination, _make_profile(), tmp_path)

    assert result.failed == 1
    assert "return snapshot_columns or disable the contract" in result.errors[0]
    assert result.diff_removed_keys is None
    assert destination.calls == []
    assert destination.deleted == []
    assert source.commit_calls == []

    nonempty_source = PluginSnapshotDiffSource(added=[{"id": 2}], changed=[], removed_keys=[])
    nonempty_destination = FakeDestination()
    nonempty_result = run_sync(
        _make_diff_sync(columns=[{"name": "id", "type": "integer"}]),
        nonempty_source,
        nonempty_destination,
        _make_profile(),
        tmp_path,
    )

    assert nonempty_result.success == 1
    assert nonempty_destination.calls == [[{"id": 2}]]
    assert nonempty_source.commit_calls == ["diff_sync"]


def test_diff_snapshot_sample_does_not_replace_first_batch_type_check(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(
        added=[
            {"id": 1, "payload": {"kind": "object"}},
            {"id": 2, "payload": "scalar"},
        ],
        changed=[],
        removed_keys=[],
    )
    destination = FakeDestination()
    sync = _make_diff_sync(
        columns=[
            {"name": "id", "type": "integer"},
            {"name": "payload", "type": "json"},
        ]
    )

    result = run_sync(sync, source, destination, _make_profile(), tmp_path)

    assert result.failed == 1
    assert "'payload' expected json, actual string" in result.errors[0]
    assert destination.calls == []
    assert source.commit_calls == []


def test_diff_strategy_does_not_commit_on_limited_run(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(added=[{"id": 1}, {"id": 2}], changed=[], removed_keys=[])
    dest = FakeDestination()

    result = run_sync(_make_diff_sync(), source, dest, _make_profile(), tmp_path, extract_limit=1)

    assert result.success == 1
    assert source.commit_calls == []


def test_diff_strategy_does_not_commit_on_row_failure(tmp_path: Path) -> None:
    """On_error: skip lets the run continue with a failure recorded; the
    baseline must stay stale so the failed row is reclassified next run
    rather than being silently treated as delivered."""
    source = FakeSnapshotDiffSource(added=[{"id": 1}], changed=[], removed_keys=[])
    dest = FakeDestination(fail_indices={0})
    sync = SyncConfig.model_validate(
        {
            "name": "diff_sync",
            "model": "ref('table')",
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "db",
                "user": "u",
                "password": "p",
                "table": "public.scores",
                "upsert_key": ["id"],
            },
            "sync": {"mode": "upsert", "incremental_strategy": "diff", "on_error": "skip"},
        }
    )

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.failed == 1
    assert source.commit_calls == []


def test_diff_strategy_does_not_commit_on_dry_run(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(added=[{"id": 1}], changed=[], removed_keys=[])
    dest = FakeDestination()
    sync = _make_diff_sync()

    run_sync(sync, source, dest, _make_profile(), tmp_path, dry_run=True)

    assert source.commit_calls == []


def test_diff_strategy_passes_upsert_key_and_hash_columns_through(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(added=[], changed=[], removed_keys=[])
    dest = FakeDestination()
    sync = _make_diff_sync()

    run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert len(source.extract_calls) == 1
    call = source.extract_calls[0]
    assert call["sync_name"] == "diff_sync"
    assert call["key_columns"] == ["id"]
    assert call["hash_columns"] == "all"


def test_diff_strategy_rejected_for_a_source_without_the_capability(tmp_path: Path) -> None:
    class PlainFakeSource:
        def extract(
            self, query: str, config: ProfileConfig, *, query_tags: dict[str, str] | None = None
        ) -> Iterator[dict]:
            yield from []

        def test_connection(self, config: ProfileConfig) -> bool:
            return True

    dest = FakeDestination()
    sync = _make_diff_sync()

    with pytest.raises(
        NotImplementedError,
        match="supported sources are Postgres, Snowflake, Databricks, and BigQuery",
    ):
        run_sync(sync, PlainFakeSource(), dest, _make_profile(), tmp_path)


def test_diff_strategy_requires_destination_upsert_key(tmp_path: Path) -> None:
    source = FakeSnapshotDiffSource(added=[], changed=[], removed_keys=[])
    dest = FakeDestination()
    sync = SyncConfig.model_validate(
        {
            "name": "diff_sync",
            "model": "ref('table')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "upsert", "incremental_strategy": "diff"},
        }
    )

    with pytest.raises(ValueError, match="requires destination.upsert_key"):
        run_sync(sync, source, dest, _make_profile(), tmp_path)


def test_diff_removed_keys_is_none_for_cursor_strategy(tmp_path: Path) -> None:
    """diff_removed_keys must stay None outside the diff strategy — not an
    empty list, which would read as 'diff ran and found nothing removed'."""

    class PlainFakeSource:
        def extract(
            self, query: str, config: ProfileConfig, *, query_tags: dict[str, str] | None = None
        ) -> Iterator[dict]:
            yield {"id": 1}

        def test_connection(self, config: ProfileConfig) -> bool:
            return True

    dest = FakeDestination()
    sync = SyncConfig.model_validate(
        {
            "name": "plain_sync",
            "model": "ref('table')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "full"},
        }
    )

    result = run_sync(sync, PlainFakeSource(), dest, _make_profile(), tmp_path)

    assert result.diff_removed_keys is None
