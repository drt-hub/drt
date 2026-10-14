"""Safe-by-construction canary wave partitioning (#1255)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, getcontext, localcontext
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

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
from drt.cli._canary_spool import (
    SnapshotSpoolSource,
    SnapshotSpoolWriter,
    SpoolError,
    _decode,
    verify_spool,
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

    def supports_keyed_upsert(self, config: DestinationConfig) -> bool:
        return True


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
    assert normalize_canary_waves([1, 10]) == [100, 1000, 10000]
    assert normalize_canary_waves([1, 100]) == [100, 10000]
    assert parse_canary_waves("1,10") == [100, 1000, 10000]
    for text in ("true,10", "nan,10", "1,,10", "0.005,10"):
        with pytest.raises(ValueError):
            parse_canary_waves(text)


def test_canonical_key_equivalence_and_type_separation() -> None:
    numeric = {canary_bucket({"id": value}, ["id"]) for value in (1, 1.0, Decimal("1"), True)}
    assert len(numeric) == 1
    assert canary_bucket({"id": "1"}, ["id"]) not in numeric
    assert canary_bucket({"id": "e\u0301"}, ["id"]) == canary_bucket({"id": "é"}, ["id"])
    assert canary_bucket({"id": -0.0}, ["id"]) == canary_bucket({"id": 0}, ["id"])


def test_decimal_key_bucket_does_not_depend_on_decimal_context() -> None:
    value = Decimal("12345678901234567890.0012300")
    original = getcontext().prec
    try:
        getcontext().prec = 3
        low_precision = canary_bucket({"id": value}, ["id"])
        getcontext().prec = 50
        high_precision = canary_bucket({"id": value}, ["id"])
    finally:
        getcontext().prec = original
    assert low_precision == high_precision


def test_canonical_key_dates_bytes_null_missing_and_unsupported() -> None:
    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)
    offset = instant.astimezone(timezone(timedelta(hours=9)))
    assert canary_bucket({"id": instant}, ["id"]) == canary_bucket({"id": offset}, ["id"])
    for value in (date(2026, 1, 1), b"\x00\xff", UUID(int=1)):
        assert isinstance(canary_bucket({"id": value}, ["id"]), int)
    with pytest.raises(ValueError, match="cannot be null"):
        canary_bucket({"id": None}, ["id"])
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
        lower = 0 if index == 0 else waves[index - 1]
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
        row_filter=canary_filter(0, 10000, ["id"], computed_fields=sync.sync.computed_fields),
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


def test_on_error_skip_destination_failures_use_only_the_threshold() -> None:
    entry = {"rows_extracted": 10, "rows_synced": 9, "rows_failed": 1}
    assert _canary_health(dict(entry), had_error=True, on_error="skip", max_error_pct=10)[-1]
    assert not _canary_health(dict(entry), had_error=True, on_error="skip", max_error_pct=9.99)[-1]


def test_hash_uses_transformed_destination_key_without_mutating_source() -> None:
    source = {"raw_id": 7}
    transformed = canary_filter(
        0,
        10000,
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


def test_snapshot_spool_round_trips_tagged_values_and_verifies_hash(tmp_path: Path) -> None:
    path = tmp_path / "snapshot.jsonl"
    row = {
        "id": 1,
        "decimal": Decimal("1.2300"),
        "bytes": b"\x00\xff",
        "bytearray": bytearray(b"ab"),
        "uuid": UUID("12345678-1234-5678-1234-567812345678"),
        "date": date(2026, 1, 2),
        "datetime": datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc),
        "time": time(3, 4, 5, fold=1),
        "nested": [None, True, ("x", 4.5), {"n": -3}],
    }
    writer = SnapshotSpoolWriter(path, max_rows=2, max_bytes=100_000, bucket_of=lambda _: 7)
    assert writer.append(row)
    rows, size, digest = writer.close()
    assert writer.close() == (rows, size, digest)
    writer.abort()

    assert rows == 1 and size == path.stat().st_size and writer.buckets[7] == 1
    verify_spool(path, digest)
    assert list(SnapshotSpoolSource(path).extract("ignored", object())) == [row]
    with localcontext() as context:
        context.prec = 2
        replayed = list(SnapshotSpoolSource(path).extract("ignored", object()))
        assert replayed[0]["decimal"] == Decimal("1.2300")

    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(SpoolError, match="hash changed"):
        verify_spool(path, digest)
    with pytest.raises(SpoolError, match="unavailable"):
        verify_spool(tmp_path / "missing.jsonl", digest)


def test_snapshot_spool_rejects_unrepresentable_and_malformed_values(tmp_path: Path) -> None:
    writer = SnapshotSpoolWriter(
        tmp_path / "bad.jsonl", max_rows=10, max_bytes=1000, bucket_of=lambda _: 0
    )
    non_string_keys: dict[Any, Any] = {1: "not-string"}
    with pytest.raises(SpoolError, match="string keys"):
        writer.append({"id": 1, "nested": non_string_keys})
    with pytest.raises(SpoolError, match="cannot preserve values of type set"):
        writer.append({"id": 1, "unsupported": {1, 2}})
    writer.abort()

    with pytest.raises(SpoolError, match="invalid type-tagged"):
        _decode([])
    with pytest.raises(SpoolError, match="unknown type tag"):
        _decode({"t": "future"})
    assert _decode({"t": "time", "v": "01:02:03", "f": 1}) == time(1, 2, 3, fold=1)

    scalar_path = tmp_path / "scalar.jsonl"
    scalar_path.write_text('{"t":"int","v":"1"}\n')
    with pytest.raises(SpoolError, match="row is not an object"):
        list(SnapshotSpoolSource(scalar_path).extract("", object()))


@pytest.mark.parametrize(
    ("max_rows", "max_bytes", "message"),
    [(0, 1000, "max_spool_rows"), (10, 1, "max_spool_bytes")],
)
def test_snapshot_spool_limits_fail_before_writing_the_excess_row(
    tmp_path: Path, max_rows: int, max_bytes: int, message: str
) -> None:
    writer = SnapshotSpoolWriter(
        tmp_path / f"{message}.jsonl",
        max_rows=max_rows,
        max_bytes=max_bytes,
        bucket_of=lambda _: 0,
    )
    with pytest.raises(SpoolError, match=message):
        writer.append({"id": 1})
    writer.abort()
    assert writer.rows == 0


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


@pytest.mark.parametrize(
    ("sync_overrides", "message"),
    [
        ({"metadata_columns": {"sync_name": "_sync"}}, "metadata_columns"),
        ({"match_policy": "update_only"}, "match_policy: upsert"),
        ({"match_policy": "create_only"}, "match_policy: upsert"),
        ({"mask": {"id": "redact"}}, "injective"),
        ({"mask": {"id": {"strategy": "truncate", "length": 2}}}, "injective"),
    ],
)
def test_rollout_config_rejects_unreplannable_row_identity(
    sync_overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        SyncConfig.model_validate(_config(**sync_overrides))


def test_rollout_config_allows_hashed_key_and_validates_spool_caps() -> None:
    configured = SyncConfig.model_validate(
        _config(
            mask={"id": "hash"},
            rollout={"waves": [1], "max_spool_rows": 7, "max_spool_bytes": 1234},
        )
    )
    assert configured.sync.rollout is not None
    assert configured.sync.rollout.max_spool_rows == 7
    assert configured.sync.rollout.max_spool_bytes == 1234
    for field in ("max_spool_rows", "max_spool_bytes"):
        with pytest.raises(ValidationError, match=field):
            SyncConfig.model_validate(_config(rollout={"waves": [1], field: 0}))


def test_rollout_config_rejects_destinations_without_keyed_upsert() -> None:
    config = _config()
    config["destination"] = {
        "type": "clickhouse",
        "host": "localhost",
        "database": "d",
        "table": "target",
        "upsert_key": ["id"],
    }
    with pytest.raises(ValidationError, match="keyed-upsert destination"):
        SyncConfig.model_validate(config)


def test_warehouse_rollout_requires_merge_mode() -> None:
    config = _config()
    config["destination"] = {
        "type": "bigquery",
        "project": "p",
        "dataset": "d",
        "table": "t",
        "upsert_key": ["id"],
    }
    with pytest.raises(ValidationError, match="destination.mode: merge"):
        SyncConfig.model_validate(config)
    config["destination"]["mode"] = "merge"
    assert SyncConfig.model_validate(config).sync.rollout is not None


def test_builtin_keyed_upsert_capability_matrix() -> None:
    from drt.config.models import (
        BigQueryDestinationConfig,
        DatabricksDestinationConfig,
        MySQLDestinationConfig,
        PostgresDestinationConfig,
        SnowflakeDestinationConfig,
    )
    from drt.destinations.bigquery import BigQueryDestination
    from drt.destinations.databricks import DatabricksDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.snowflake import SnowflakeDestination

    postgres = PostgresDestinationConfig(
        type="postgres", host="localhost", dbname="d", table="t", upsert_key=["id"]
    )
    mysql = MySQLDestinationConfig(
        type="mysql", host="localhost", dbname="d", table="t", upsert_key=["id"]
    )
    assert PostgresDestination().supports_keyed_upsert(postgres)
    assert MySQLDestination().supports_keyed_upsert(mysql)

    warehouse_cases = [
        (
            SnowflakeDestination(),
            SnowflakeDestinationConfig(
                type="snowflake",
                account_env="A",
                user_env="U",
                password_env="P",
                database="d",
                schema="s",
                table="t",
                warehouse="w",
                upsert_key=["id"],
            ),
        ),
        (
            DatabricksDestination(),
            DatabricksDestinationConfig(
                type="databricks",
                host_env="H",
                http_path_env="P",
                token_env="T",
                catalog="c",
                schema="s",
                table="t",
                upsert_key=["id"],
            ),
        ),
        (
            BigQueryDestination(),
            BigQueryDestinationConfig(
                type="bigquery", project="p", dataset="d", table="t", upsert_key=["id"]
            ),
        ),
    ]
    for destination, config in warehouse_cases:
        assert not destination.supports_keyed_upsert(config)
        assert destination.supports_keyed_upsert(config.model_copy(update={"mode": "merge"}))


def test_rollout_rejects_lookup_and_run_metadata_derived_keys() -> None:
    lookup = _config()
    lookup["destination"]["lookups"] = {
        "id": {"table": "parents", "match": {"raw_id": "raw_id"}, "select": "id"}
    }
    with pytest.raises(ValidationError, match="destination lookup"):
        SyncConfig.model_validate(lookup)

    metadata = _config(metadata_columns={"run_id": "id"})
    with pytest.raises(ValidationError, match="metadata_columns"):
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
    with pytest.raises(ApplyRefused, match="metadata_columns"):
        _validate_canary_compatibility(SyncConfig.model_validate(metadata), _Destination())

    match_policy = _config(match_policy="create_only")
    match_policy["sync"].pop("rollout")
    with pytest.raises(ApplyRefused, match="match_policy: upsert"):
        _validate_canary_compatibility(SyncConfig.model_validate(match_policy), _Destination())

    masked = _config(mask={"id": {"strategy": "truncate", "length": 2}})
    masked["sync"].pop("rollout")
    with pytest.raises(ApplyRefused, match="injective"):
        _validate_canary_compatibility(SyncConfig.model_validate(masked), _Destination())
