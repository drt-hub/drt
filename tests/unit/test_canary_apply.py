"""Safe-by-construction canary wave partitioning (#1255)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from drt.cli._apply_flow import (
    ApplyRefused,
    _canary_health,
    _validate_canary_compatibility,
    canary_bucket,
    canary_filter,
    normalize_canary_waves,
    parse_canary_waves,
)
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


def _sync(**sync_overrides: Any) -> SyncConfig:
    sync = {"mode": "upsert", "batch_size": 17, **sync_overrides}
    return SyncConfig.model_validate(
        {
            "name": "canary",
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
            "sync": sync,
        }
    )


@pytest.mark.parametrize(
    "waves",
    [
        [True, 10],
        ["1", 10],
        [],
        [float("nan"), 10],
        [float("inf"), 10],
        [0.005, 10],
        [1.234, 10],
        [0, 10],
        [10, 10],
        [10, 1],
    ],
)
def test_wave_normalization_rejects_invalid_values(waves: list[Any]) -> None:
    with pytest.raises(ValueError):
        normalize_canary_waves(waves)


def test_wave_normalization_and_cli_parser_append_100_once() -> None:
    assert normalize_canary_waves([1, 10]) == [1.0, 10.0, 100.0]
    assert normalize_canary_waves([1, 100]) == [1.0, 100.0]
    assert parse_canary_waves("1,10") == [1.0, 10.0, 100.0]
    for text in ("true,10", "nan,10", "1,,10", "0.005,10"):
        with pytest.raises(ValueError):
            parse_canary_waves(text)


def test_canonical_key_equivalence_and_type_separation() -> None:
    numeric = {canary_bucket({"id": value}, ["id"]) for value in (1, 1.0, Decimal("1"), True)}
    assert len(numeric) == 1
    assert canary_bucket({"id": "1"}, ["id"]) not in numeric
    assert canary_bucket({"id": "e\u0301"}, ["id"]) == canary_bucket({"id": "é"}, ["id"])
    assert canary_bucket({"id": -0.0}, ["id"]) == canary_bucket({"id": 0}, ["id"])


def test_canonical_key_dates_bytes_null_missing_and_unsupported() -> None:
    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)
    offset = instant.astimezone(timezone(timedelta(hours=9)))
    assert canary_bucket({"id": instant}, ["id"]) == canary_bucket({"id": offset}, ["id"])
    for value in (None, date(2026, 1, 1), b"\x00\xff"):
        assert isinstance(canary_bucket({"id": value}, ["id"]), int)
    with pytest.raises(ValueError, match="missing is not null"):
        canary_bucket({}, ["id"])
    with pytest.raises(ValueError, match="Unsupported canary key type"):
        canary_bucket({"id": {"nested": "value"}}, ["id"])
    for value in (float("nan"), float("inf"), Decimal("NaN")):
        with pytest.raises(ValueError, match="non-finite"):
            canary_bucket({"id": value}, ["id"])
    with pytest.raises(ValueError, match="requires destination.upsert_key"):
        canary_bucket({"id": 1})


def test_hash_waves_are_stable_exactly_once_and_each_run_is_normal(tmp_path: Path) -> None:
    rows = [{"id": index, "payload": f"row-{index}"} for index in range(500)]
    destination = _Destination()
    observer = MagicMock()
    profile = DuckDBProfile(type="duckdb")
    waves = normalize_canary_waves([1, 10])
    selected: list[list[int]] = []

    for index, upper in enumerate(waves):
        lower = 0.0 if index == 0 else waves[index - 1]
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
            observer=observer,
        )
        assert result.rows_extracted == len(chosen)

    flattened = [value for wave in selected for value in wave]
    assert sorted(flattened) == list(range(500))
    assert len(flattened) == len(set(flattened))
    assert sorted(row["id"] for row in destination.rows) == list(range(500))
    assert destination.finalized == len(waves)
    assert observer.on_sync_completed.call_count == len(waves)


def test_row_filter_default_path_is_unchanged(tmp_path: Path) -> None:
    rows = [{"id": 1}, {"id": 2}]
    destination = _Destination()
    result = run_sync(_sync(), _Source(rows), destination, DuckDBProfile(type="duckdb"), tmp_path)
    assert result.success == 2
    assert destination.rows == rows


def test_health_halts_on_transform_skip_and_empty_wave_passes() -> None:
    transform_failure = {
        "rows_extracted": 4,
        "rows_synced": 3,
        "rows_failed": 0,
        "_row_error_count": 1,
    }
    rows, ok, failed, rate, passed = _canary_health(
        transform_failure, had_error=False, on_error="skip", max_error_pct=20
    )
    assert (rows, ok, failed, rate, passed) == (4, 3, 1, 25.0, False)
    assert "_row_error_count" not in transform_failure

    assert _canary_health(
        {"rows_extracted": 0, "rows_synced": 0, "rows_failed": 0},
        had_error=False,
        on_error="skip",
        max_error_pct=0,
    ) == (0, 0, 0, 0.0, True)


def test_real_computed_field_skip_counts_as_wave_error(tmp_path: Path) -> None:
    sync = _sync(computed_fields={"derived": "{{ row.missing }}"}, on_error="skip")
    result = run_sync(
        sync,
        _Source([{"id": 1}]),
        _Destination(),
        DuckDBProfile(type="duckdb"),
        tmp_path,
        row_filter=canary_filter(0, 100, ["id"], computed_fields=sync.sync.computed_fields),
    )
    assert result.rows_extracted == 1
    assert result.skipped == 1
    assert len(result.row_errors) == 1
    assert not _canary_health(
        {
            "rows_extracted": result.rows_extracted,
            "rows_synced": result.success,
            "rows_failed": result.failed,
            "_row_error_count": len(result.row_errors),
        },
        had_error=False,
        on_error="skip",
        max_error_pct=0,
    )[-1]


def test_on_error_fail_destination_failure_halts_regardless_of_threshold() -> None:
    assert not _canary_health(
        {"rows_extracted": 100, "rows_synced": 99, "rows_failed": 1},
        had_error=False,
        on_error="fail",
        max_error_pct=100,
    )[-1]


def test_hash_uses_transformed_destination_key_without_mutating_source() -> None:
    source = {"raw_id": 7}
    transformed = canary_filter(
        0,
        100,
        ["customer_id"],
        {"derived": "customer_id"},
        {"derived": "{{ row.raw_id + 1 }}"},
        {"customer_id": "hash"},
    )
    assert transformed(source)
    assert source == {"raw_id": 7}


def test_filter_without_a_key_fails_closed_when_evaluated() -> None:
    predicate = canary_filter(0, 100, None)

    with pytest.raises(ValueError, match="requires destination.upsert_key"):
        predicate({"id": 1})


def _config(
    *, mode: str = "upsert", destination_type: str = "postgres", **sync: Any
) -> dict[str, Any]:
    destination: dict[str, Any]
    if destination_type == "staged_upload":
        destination = {
            "type": "staged_upload",
            "stage": {"url": "https://example.com/upload"},
            "trigger": {"url": "https://example.com/trigger"},
        }
    else:
        destination = {
            "type": "postgres",
            "host": "localhost",
            "dbname": "d",
            "user": "u",
            "password": "p",
            "table": "target",
            "upsert_key": ["id"],
        }
    return {
        "name": "configured",
        "model": "select 1",
        "destination": destination,
        "sync": {"mode": mode, "rollout": {"waves": [1, 10]}, **sync},
    }


def test_rollout_config_is_strict_and_schema_keeps_numeric_items() -> None:
    configured = SyncConfig.model_validate(_config())
    assert configured.sync.rollout is not None
    assert configured.sync.rollout.waves == [1.0, 10.0, 100.0]
    schema = SyncConfig.model_json_schema()
    rollout = schema["$defs"]["RolloutConfig"]
    assert rollout["properties"]["waves"]["items"]["type"] == "number"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SyncConfig.model_validate(_config(rollout={"waves": [1], "typo": True}))


def test_rollout_preserves_an_explicit_final_wave_and_accepts_compact_soak() -> None:
    configured = SyncConfig.model_validate(_config(rollout={"waves": [1, 100], "soak": "1s"}))
    explicit_none = SyncConfig.model_validate(_config(rollout={"waves": [1], "soak": None}))

    assert configured.sync.rollout is not None
    assert configured.sync.rollout.waves == [1.0, 100.0]
    assert configured.sync.rollout.soak == "1s"
    assert explicit_none.sync.rollout is not None
    assert explicit_none.sync.rollout.soak is None


@pytest.mark.parametrize(
    "waves",
    [
        [],
        [True, 10],
        ["1", 10],
        [float("nan"), 10],
        [float("inf"), 10],
        [0.005, 10],
        [1.234],
        [10, 10],
        [10, 1],
    ],
)
def test_rollout_config_rejects_invalid_wave_values(waves: list[Any]) -> None:
    with pytest.raises(ValidationError):
        SyncConfig.model_validate(_config(rollout={"waves": waves}))


@pytest.mark.parametrize("max_error_pct", [True, float("nan"), float("inf"), -1, 101])
def test_rollout_config_rejects_invalid_error_threshold(max_error_pct: Any) -> None:
    with pytest.raises(ValidationError):
        SyncConfig.model_validate(_config(rollout={"waves": [1], "max_error_pct": max_error_pct}))


def test_rollout_config_requires_key_and_validates_soak() -> None:
    without_key = _config()
    without_key["destination"] = {"type": "rest_api", "url": "https://example.com"}
    with pytest.raises(ValidationError, match="requires destination.upsert_key"):
        SyncConfig.model_validate(without_key)
    with pytest.raises(ValidationError, match="Invalid sync.rollout.soak"):
        SyncConfig.model_validate(_config(rollout={"waves": [1], "soak": "eventually"}))


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (_config(mode="incremental", cursor_field="updated_at"), "non-incremental"),
        (_config(mode="upsert", incremental_strategy="diff"), "snapshot baseline"),
        (_config(mode="mirror"), "non-incremental"),
        (_config(mode="replace"), "non-incremental"),
        (_config(destination_type="staged_upload"), "non-staged"),
    ],
)
def test_rollout_config_rejects_unsupported_combinations(
    config: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        SyncConfig.model_validate(config)


def test_rollout_rejects_lookup_and_run_metadata_derived_keys() -> None:
    lookup = _config()
    lookup["destination"]["lookups"] = {
        "id": {"table": "parents", "match": {"raw_id": "raw_id"}, "select": "id"}
    }
    with pytest.raises(ValidationError, match="destination lookup"):
        SyncConfig.model_validate(lookup)

    metadata = _config(metadata_columns={"run_id": "id"})
    with pytest.raises(ValidationError, match="run metadata"):
        SyncConfig.model_validate(metadata)


def test_apply_override_rejects_missing_and_derived_keys() -> None:
    missing_key = SyncConfig.model_validate(
        {
            "name": "missing-key",
            "model": "select 1",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "upsert"},
        }
    )
    with pytest.raises(ApplyRefused, match="requires destination.upsert_key"):
        _validate_canary_compatibility(missing_key, _Destination())

    lookup = _config()
    lookup["sync"].pop("rollout")
    lookup["destination"]["lookups"] = {
        "id": {"table": "parents", "match": {"raw_id": "raw_id"}, "select": "id"}
    }
    with pytest.raises(ApplyRefused, match="destination lookup"):
        _validate_canary_compatibility(SyncConfig.model_validate(lookup), _Destination())

    metadata = _config(metadata_columns={"run_id": "id"})
    metadata["sync"].pop("rollout")
    with pytest.raises(ApplyRefused, match="run metadata"):
        _validate_canary_compatibility(SyncConfig.model_validate(metadata), _Destination())
