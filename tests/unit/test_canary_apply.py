"""Canary wave partitioning and engine completion gates (#1255)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from drt.cli._apply_flow import canary_bucket, canary_filter, normalize_canary_waves
from drt.config.credentials import DuckDBProfile
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync


class _Source:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(
        self, query: str, config: Any, *, query_tags: dict[str, str] | None = None
    ) -> Iterator[dict[str, Any]]:
        yield from self.rows


class _Destination:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.finalized = 0

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.rows.extend(records)
        return SyncResult(success=len(records))

    def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
        self.finalized += 1
        return SyncResult()


class _MirrorDestination(_Destination):
    def __init__(self) -> None:
        super().__init__()
        self.observed: list[int] = []
        self.finalized_keys: list[int] = []

    def supported_modes(self) -> frozenset[str]:
        return frozenset({"mirror"})

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.rows.extend(records)
        self.observed.extend(int(record["id"]) for record in records)
        return SyncResult(success=len(records))

    def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
        self.finalized += 1
        self.finalized_keys = list(self.observed)
        return SyncResult()


def _sync() -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "canary",
            "model": "select 1",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "upsert", "batch_size": 17},
        }
    )


def test_wave_normalization_and_validation() -> None:
    assert normalize_canary_waves([1, 10]) == [1.0, 10.0, 100.0]
    with pytest.raises(ValueError, match="duplicates"):
        normalize_canary_waves([1, 1, 100])
    with pytest.raises(ValueError, match="ascending"):
        normalize_canary_waves([10, 1])


def test_hash_waves_are_a_stable_disjoint_cover(tmp_path: Path) -> None:
    rows = [{"id": index, "payload": f"row-{index}"} for index in range(500)]
    destination = _Destination()
    profile = DuckDBProfile(type="duckdb")
    waves = normalize_canary_waves([1, 10])
    selected: list[list[int]] = []
    lower = 0.0

    for upper in waves:
        predicate = canary_filter(lower, upper, ["id"])
        chosen = [row["id"] for row in rows if predicate(row)]
        selected.append(chosen)
        result = run_sync(
            _sync(),
            _Source(rows),
            destination,
            profile,
            tmp_path,
            row_filter=predicate,
            finalize_sync=False,
            save_watermark=False,
        )
        assert result.rows_extracted == len(chosen)
        lower = upper

    flattened = [value for wave in selected for value in wave]
    assert sorted(flattened) == list(range(500))
    assert len(flattened) == len(set(flattened))
    assert selected == [
        [
            row["id"]
            for row in rows
            if canary_filter(0 if i == 0 else waves[i - 1], wave, ["id"])(row)
        ]
        for i, wave in enumerate(waves)
    ]


def test_hash_uses_transformed_destination_key() -> None:
    source = {"raw_id": 7}
    masked_key = hashlib.sha256(b"8").hexdigest()
    bucket = canary_bucket({"customer_id": masked_key}, ["customer_id"])
    transformed = canary_filter(
        bucket / 100,
        (bucket + 1) / 100,
        ["customer_id"],
        {"derived": "customer_id"},
        {"derived": "{{ row.raw_id + 1 }}"},
        {"customer_id": "hash"},
    )

    assert transformed(source)
    # Determinism includes the transform pipeline and does not mutate extraction rows.
    assert transformed(source) == transformed({"raw_id": 7})
    assert source == {"raw_id": 7}


def test_nonfinal_wave_skips_finalize_and_watermark_and_final_wave_runs_both(
    tmp_path: Path,
) -> None:
    observer = MagicMock()
    destination = _Destination()
    args = (_sync(), _Source([{"id": 1}]), destination, DuckDBProfile(type="duckdb"), tmp_path)

    run_sync(*args, finalize_sync=False, save_watermark=False, observer=observer)
    assert destination.finalized == 0
    observer.on_sync_completed.assert_not_called()

    run_sync(*args, finalize_sync=True, save_watermark=True, observer=observer)
    assert destination.finalized == 1
    observer.on_sync_completed.assert_called_once()


def test_failed_completion_gate_skips_finalize_and_watermark(tmp_path: Path) -> None:
    observer = MagicMock()
    destination = _Destination()

    run_sync(
        _sync(),
        _Source([{"id": 1}]),
        destination,
        DuckDBProfile(type="duckdb"),
        tmp_path,
        observer=observer,
        completion_gate=lambda _result: False,
    )

    assert destination.finalized == 0
    observer.on_sync_completed.assert_not_called()


def test_mirror_finalize_observes_keys_from_every_wave(tmp_path: Path) -> None:
    rows = [{"id": index} for index in range(200)]
    sync = SyncConfig.model_validate(
        {
            "name": "mirror",
            "model": "select 1",
            "destination": {
                "type": "postgres",
                "host": "localhost",
                "dbname": "d",
                "user": "u",
                "password": "p",
                "table": "target",
                "upsert_key": ["id"],
            },
            "sync": {"mode": "mirror"},
        }
    )
    destination = _MirrorDestination()
    profile = DuckDBProfile(type="duckdb")

    run_sync(
        sync,
        _Source(rows),
        destination,
        profile,
        tmp_path,
        row_filter=canary_filter(0, 10, ["id"]),
        finalize_sync=False,
        save_watermark=False,
    )
    assert destination.finalized == 0
    run_sync(
        sync,
        _Source(rows),
        destination,
        profile,
        tmp_path,
        row_filter=canary_filter(10, 100, ["id"]),
    )

    assert destination.finalized == 1
    assert sorted(destination.finalized_keys) == list(range(200))


def test_failed_final_gate_and_zero_row_wave() -> None:
    failed = SyncResult(success=98, failed=3)
    empty = SyncResult()

    def passes(result: SyncResult) -> bool:
        attempted = result.success + result.failed
        return attempted == 0 or result.failed * 100 / attempted <= 2

    assert not passes(failed)
    assert passes(empty)


def test_rollout_config_appends_100_and_rejects_unknown_keys() -> None:
    sync = SyncConfig.model_validate(
        {
            "name": "configured",
            "model": "select 1",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"rollout": {"waves": [1, 10], "max_error_pct": 3, "soak": "30s"}},
        }
    )
    assert sync.sync.rollout is not None
    assert sync.sync.rollout.waves == [1.0, 10.0, 100.0]
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SyncConfig.model_validate(
            {
                "name": "bad",
                "model": "select 1",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"rollout": {"waves": [1], "typo": True}},
            }
        )
