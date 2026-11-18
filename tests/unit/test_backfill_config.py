"""Config validation and execution preflight for ADR 0021 backfills."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft7Validator
from pydantic import ValidationError

from drt.config.models import BackfillConfig, SyncConfig, SyncOptions
from drt.config.parser import load_syncs_safe
from drt.config.schema import generate_sync_schema
from drt.config.sync_options import _find_backfill_like_unknown_key
from drt.engine.sync import run_sync

SYNC_OPTIONS_FIELDS = (
    "mode",
    "replace_strategy",
    "match_policy",
    "write_policy",
    "write_policy_overrides",
    "cursor_field",
    "incremental_strategy",
    "diff",
    "watermark",
    "backfill",
    "batch_size",
    "rate_limit",
    "retry",
    "on_error",
    "computed_fields",
    "field_mappings",
    "mask",
    "metadata_columns",
    "dlq",
    "mirror",
    "guards",
    "idempotency_key",
)
SYNC_CONFIG_FIELDS = (
    "name",
    "description",
    "tags",
    "model",
    "destination",
    "sync",
    "tests",
    "unit_tests",
    "alerts",
    "meta",
    "columns",
    "contract",
)


@pytest.mark.parametrize("window", ["1d", "6 hours"])
def test_timestamp_backfill_accepts_compact_and_long_durations(window: str) -> None:
    config = BackfillConfig(
        window=window,
        start="2025-01-01T00:00:00Z",
        end="2025-02-01T00:00:00+00:00",
    )

    assert config.window == window


def test_numeric_backfill_accepts_exact_integers_and_optional_end() -> None:
    assert BackfillConfig(window=100_000, start=0).end is None
    assert BackfillConfig(window=10, start=-20, end=5).end == 5


@pytest.mark.parametrize(
    "values",
    [
        {"window": 10, "start": -(2**63) - 1},
        {"window": 10, "start": 0, "end": 2**63},
    ],
)
def test_numeric_backfill_rejects_bounds_outside_signed_64_bit(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="signed 64-bit integer range"):
        BackfillConfig.model_validate(values)


def test_backfill_config_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        BackfillConfig.model_validate(
            {"window": "1d", "start": "2025-01-01T00:00:00Z", "typo": True}
        )


def test_targeted_typo_guard_does_not_flag_any_existing_model_field() -> None:
    assert tuple(SyncOptions.model_fields) == SYNC_OPTIONS_FIELDS
    assert tuple(SyncConfig.model_fields) == SYNC_CONFIG_FIELDS
    assert (
        _find_backfill_like_unknown_key(
            dict.fromkeys(SYNC_OPTIONS_FIELDS), SyncOptions.model_fields
        )
        is None
    )
    assert (
        _find_backfill_like_unknown_key(dict.fromkeys(SYNC_CONFIG_FIELDS), SyncConfig.model_fields)
        is None
    )


@pytest.mark.parametrize("key", ["backfil", "backfills", "backfill_", "BackFill"])
def test_sync_options_rejects_likely_backfill_typos(key: str) -> None:
    with pytest.raises(ValidationError, match=r"did you mean sync\.backfill\?"):
        SyncOptions.model_validate(
            {
                "mode": "incremental",
                "cursor_field": "updated_at",
                key: {"window": "1d", "start": "2025-01-01T00:00:00Z"},
            }
        )


@pytest.mark.parametrize("key", ["backfill", "backfil", "backfills"])
def test_sync_config_rejects_backfill_at_top_level(key: str) -> None:
    with pytest.raises(ValidationError, match=r"did you mean sync\.backfill\?"):
        SyncConfig.model_validate(
            {
                "name": "users",
                "model": "select * from users",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                key: {"window": "1d", "start": "2025-01-01T00:00:00Z"},
            }
        )


def test_unrelated_unknown_keys_remain_ignored() -> None:
    options = SyncOptions.model_validate({"unrelated_typo": True})
    sync = SyncConfig.model_validate(
        {
            "name": "users",
            "model": "select * from users",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "unrelated_typo": True,
        }
    )

    assert options.mode == "full"
    assert sync.name == "users"


@pytest.mark.parametrize(
    "backfill",
    [
        {"window": 10, "start": "2025-01-01T00:00:00Z"},
        {"window": "1d", "start": 0},
    ],
)
def test_generated_schema_rejects_mixed_backfill_arithmetic(backfill: dict[str, object]) -> None:
    schema = generate_sync_schema()
    config = {
        "name": "users",
        "model": "select * from users",
        "destination": {"type": "rest_api", "url": "https://example.com"},
        "sync": {
            "mode": "incremental",
            "cursor_field": "updated_at",
            "backfill": backfill,
        },
    }

    assert list(Draft7Validator(schema).iter_errors(config))


@pytest.mark.parametrize(
    "backfill",
    [
        {
            "window": "6 hours",
            "start": "2025-01-01T00:00:00Z",
            "end": "2025-01-02T00:00:00Z",
        },
        {"window": 100, "start": 0, "end": 1_000},
    ],
)
def test_generated_schema_accepts_consistent_backfill_arithmetic(
    backfill: dict[str, object],
) -> None:
    schema = generate_sync_schema()
    config = {
        "name": "users",
        "model": "select * from users",
        "destination": {"type": "rest_api", "url": "https://example.com"},
        "sync": {
            "mode": "incremental",
            "cursor_field": "updated_at",
            "backfill": backfill,
        },
    }

    assert list(Draft7Validator(schema).iter_errors(config)) == []


@pytest.mark.parametrize("bound", [-(2**63) - 1, 2**63])
def test_generated_schema_rejects_integer_bounds_outside_signed_64_bit(bound: int) -> None:
    schema = generate_sync_schema()
    config = {
        "name": "users",
        "model": "select * from users",
        "destination": {"type": "rest_api", "url": "https://example.com"},
        "sync": {
            "mode": "incremental",
            "cursor_field": "id",
            "backfill": {"window": 100, "start": bound},
        },
    }

    assert list(Draft7Validator(schema).iter_errors(config))


def test_generated_schema_documents_backfill_shapes_and_runtime_eligibility() -> None:
    backfill_schema = generate_sync_schema()["$defs"]["BackfillConfig"]

    assert len(backfill_schema["oneOf"]) == 2
    assert backfill_schema["oneOf"][0]["properties"]["window"]["pattern"]
    assert backfill_schema["oneOf"][0]["properties"]["start"]["format"] == "date-time"
    assert "drt validate" in backfill_schema["description"]


@pytest.mark.parametrize("window", [True, False, 0, -1, 1.5])
def test_backfill_rejects_invalid_windows(window: object) -> None:
    with pytest.raises(ValidationError, match=r"sync\.backfill\.window"):
        BackfillConfig.model_validate({"window": window, "start": 0})


@pytest.mark.parametrize(
    ("values", "field"),
    [
        ({"window": 10, "start": True}, "start"),
        ({"window": 10, "start": 1.5}, "start"),
        ({"window": 10, "start": 0, "end": False}, "end"),
        ({"window": 10, "start": 0, "end": 1.5}, "end"),
    ],
)
def test_backfill_rejects_boolean_and_float_bounds(values: dict[str, object], field: str) -> None:
    with pytest.raises(ValidationError, match=rf"sync\.backfill\.{field}"):
        BackfillConfig.model_validate(values)


@pytest.mark.parametrize(
    "values",
    [
        {"window": 10, "start": "2025-01-01T00:00:00Z"},
        {"window": 10, "start": 0, "end": "2025-01-01T00:00:00Z"},
        {"window": "1d", "start": 0},
        {"window": "1d", "start": "2025-01-01T00:00:00Z", "end": 10},
    ],
)
def test_backfill_rejects_mixed_range_types(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match=r"sync\.backfill\.(start|end)"):
        BackfillConfig.model_validate(values)


@pytest.mark.parametrize("field", ["start", "end"])
def test_backfill_rejects_non_iso_timestamps(field: str) -> None:
    values = {
        "window": "1d",
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-02-01T00:00:00Z",
    }
    values[field] = "not-a-timestamp"

    with pytest.raises(ValidationError, match=rf"sync\.backfill\.{field}"):
        BackfillConfig.model_validate(values)


def test_backfill_rejects_sub_microsecond_timestamp_bound() -> None:
    with pytest.raises(ValidationError, match="sub-microsecond bounds are not supported"):
        BackfillConfig.model_validate({"window": "1d", "start": "2025-01-01T00:00:00.1234567Z"})


@pytest.mark.parametrize(
    "values",
    [
        {"window": 10, "start": 100, "end": 100},
        {"window": 10, "start": 101, "end": 100},
        {
            "window": "1d",
            "start": "2025-01-02T00:00:00Z",
            "end": "2025-01-01T00:00:00Z",
        },
    ],
)
def test_backfill_rejects_start_at_or_after_fixed_end(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="start must be less than"):
        BackfillConfig.model_validate(values)


def _options(**overrides: Any) -> SyncOptions:
    values: dict[str, Any] = {
        "mode": "incremental",
        "cursor_field": "updated_at",
        "backfill": {"window": "1d", "start": "2025-01-01T00:00:00Z"},
    }
    values.update(overrides)
    return SyncOptions.model_validate(values)


def test_backfill_is_valid_for_incremental_cursor_strategy() -> None:
    assert _options().backfill is not None
    assert _options(incremental_strategy="cursor").backfill is not None


@pytest.mark.parametrize("mode", ["full", "upsert", "replace", "mirror"])
def test_backfill_rejects_non_incremental_modes(mode: str) -> None:
    with pytest.raises(ValidationError, match="sync.backfill.*mode: incremental"):
        _options(mode=mode)


def test_backfill_rejects_diff_strategy() -> None:
    with pytest.raises(ValidationError, match="sync.backfill.*incremental_strategy: diff"):
        _options(incremental_strategy="diff")


def test_backfill_rejects_watermark_default_value() -> None:
    with pytest.raises(ValidationError, match="sync.backfill.*watermark.default_value"):
        _options(watermark={"default_value": "2024-01-01T00:00:00Z"})


def test_backfill_rejects_nested_cursor_path() -> None:
    with pytest.raises(ValidationError, match="nested cursor paths are not supported"):
        _options(cursor_field="payload.updated_at")


def _sync() -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "users",
            "model": "select * from users",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "updated_at",
                "backfill": {"window": "1d", "start": "2025-01-01T00:00:00Z"},
            },
        }
    )


def test_config_loader_accepts_executable_backfill(tmp_path: Path) -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "users.yml").write_text(
        """
name: users
model: select * from users
destination:
  type: rest_api
  url: https://example.com
sync:
  mode: incremental
  cursor_field: updated_at
  backfill:
    window: 1d
    start: "2025-01-01T00:00:00Z"
"""
    )

    result = load_syncs_safe(tmp_path)

    assert [sync.name for sync in result.syncs] == ["users"]
    assert result.errors == {}


def test_run_sync_requires_checkpoint_observer_before_any_engine_io(tmp_path: Path) -> None:
    with pytest.raises(
        RuntimeError,
        match=r"checkpoint-capable observer",
    ):
        run_sync(
            _sync(),
            object(),
            object(),
            object(),
            tmp_path,
            allow_backfill_execution=True,
        )
