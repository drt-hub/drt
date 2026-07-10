"""Tests for ``watermark.lag`` — overlap window for late-arriving rows (#759).

Three layers:
- ``parse_duration`` grammar (shared config duration parser)
- ``_apply_watermark_lag`` value arithmetic (numeric + timestamp cursors,
  #475-compatible re-stringification)
- ``run_sync`` integration — lag widens only the extraction predicate for
  storage-sourced watermarks, never the persisted watermark, and never
  applies to ``--cursor-value`` overrides or ``default_value`` first runs.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.duration import parse_duration
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions, WatermarkConfig
from drt.destinations.base import SyncResult
from drt.engine.observer import StatePersistingObserver
from drt.engine.sync import _apply_watermark_lag, run_sync

# ---------------------------------------------------------------------------
# parse_duration
# ---------------------------------------------------------------------------


def test_parse_duration_all_units() -> None:
    assert parse_duration("1 hour") == timedelta(hours=1)
    assert parse_duration("7 days") == timedelta(days=7)
    assert parse_duration("30 minutes") == timedelta(minutes=30)
    assert parse_duration("45 seconds") == timedelta(seconds=45)
    assert parse_duration("2 weeks") == timedelta(weeks=2)
    assert parse_duration("1 day") == timedelta(days=1)


def test_parse_duration_rejects_single_token() -> None:
    with pytest.raises(ValueError, match="Invalid watermark.lag format"):
        parse_duration("1hour", field_name="watermark.lag")


def test_parse_duration_rejects_non_integer_value() -> None:
    with pytest.raises(ValueError, match="Must be an integer"):
        parse_duration("1.5 hours")


def test_parse_duration_rejects_non_positive_value() -> None:
    with pytest.raises(ValueError, match="Must be a positive integer"):
        parse_duration("0 hours")


def test_parse_duration_rejects_unknown_unit() -> None:
    with pytest.raises(ValueError, match="Invalid duration unit"):
        parse_duration("1 fortnight")


# ---------------------------------------------------------------------------
# _apply_watermark_lag
# ---------------------------------------------------------------------------


def test_lag_naive_timestamp() -> None:
    assert _apply_watermark_lag("2026-07-10 12:00:00", "1 hour") == "2026-07-10 11:00:00"


def test_lag_t_separator_normalizes_to_space() -> None:
    # Output uses the persisted-watermark convention (str(datetime) → space sep).
    assert _apply_watermark_lag("2026-07-10T12:00:00", "1 hour") == "2026-07-10 11:00:00"


def test_lag_z_suffix_normalizes_to_naive_utc() -> None:
    # tz-aware inputs collapse to naive UTC, matching #475 stringification.
    assert _apply_watermark_lag("2026-07-10T12:00:00Z", "2 hours") == "2026-07-10 10:00:00"


def test_lag_utc_offset_normalizes_to_naive_utc() -> None:
    assert _apply_watermark_lag("2026-07-10 12:00:00+09:00", "1 hour") == "2026-07-10 02:00:00"


def test_lag_date_only_cursor() -> None:
    assert _apply_watermark_lag("2026-07-10", "1 day") == "2026-07-09 00:00:00"


def test_lag_numeric_cursor_with_int_lag() -> None:
    assert _apply_watermark_lag("1000", 100) == "900"


def test_lag_numeric_cursor_rejects_duration_lag() -> None:
    with pytest.raises(ValueError, match="numeric cursors take an integer lag"):
        _apply_watermark_lag("1000", "1 hour")


def test_lag_timestamp_cursor_rejects_int_lag() -> None:
    with pytest.raises(ValueError, match="duration string"):
        _apply_watermark_lag("2026-07-10 12:00:00", 100)


def test_lag_unparseable_cursor_raises() -> None:
    with pytest.raises(ValueError, match="could not parse stored watermark"):
        _apply_watermark_lag("not-a-date", "1 hour")


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_watermark_config_accepts_duration_lag() -> None:
    assert WatermarkConfig(lag="1 hour").lag == "1 hour"


def test_watermark_config_accepts_int_lag() -> None:
    assert WatermarkConfig(lag=3600).lag == 3600


def test_watermark_config_rejects_malformed_duration() -> None:
    with pytest.raises(ValueError, match="watermark.lag"):
        WatermarkConfig(lag="1 fortnight")


def test_watermark_config_rejects_non_positive_int() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        WatermarkConfig(lag=0)


def test_sync_options_lag_requires_incremental_mode() -> None:
    with pytest.raises(ValueError, match="requires mode='incremental'"):
        SyncOptions.model_validate({"mode": "full", "watermark": {"lag": "1 hour"}})


def test_sync_options_lag_valid_with_incremental_mode() -> None:
    opts = SyncOptions.model_validate(
        {
            "mode": "incremental",
            "cursor_field": "updated_at",
            "watermark": {"lag": "1 hour"},
        }
    )
    assert opts.watermark is not None
    assert opts.watermark.lag == "1 hour"


# ---------------------------------------------------------------------------
# run_sync integration
# ---------------------------------------------------------------------------


class QueryCapturingSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.queries: list[str] = []

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        self.queries.append(query)
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


class CollectDestination:
    def load(
        self,
        records: list[dict],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        result.success = len(records)
        return result


class FakeWatermarkStorage:
    def __init__(self, value: str | None) -> None:
        self._value = value
        self.saved: list[tuple[str, str]] = []

    def get(self, sync_name: str) -> str | None:
        return self._value

    def save(self, sync_name: str, value: str) -> None:
        self.saved.append((sync_name, value))


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_incremental_sync(lag: str | int | None = None) -> SyncConfig:
    watermark: dict[str, object] = {"default_value": "2026-07-01 00:00:00"}
    if lag is not None:
        watermark["lag"] = lag
    return SyncConfig.model_validate(
        {
            "name": "lag_sync",
            "model": "SELECT * FROM t",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "updated_at",
                "watermark": watermark,
            },
        }
    )


def test_run_sync_lag_widens_extraction_predicate(tmp_path: Path) -> None:
    source = QueryCapturingSource([{"id": 1, "updated_at": "2026-07-10 12:34:56"}])
    storage = FakeWatermarkStorage("2026-07-10 12:00:00")
    sync = _make_incremental_sync(lag="1 hour")

    result = run_sync(
        sync, source, CollectDestination(), _make_profile(), tmp_path, watermark_storage=storage
    )

    assert "2026-07-10 11:00:00" in source.queries[0]
    assert "2026-07-10 12:00:00" not in source.queries[0]
    assert result.watermark_source == "storage"
    assert result.cursor_value_used == "2026-07-10 11:00:00"
    assert result.watermark_lag == "1 hour"


def test_run_sync_lag_never_regresses_persisted_watermark(tmp_path: Path) -> None:
    """An empty run must persist the original watermark, not the lagged one."""
    source = QueryCapturingSource([])
    storage = FakeWatermarkStorage("2026-07-10 12:00:00")
    sync = _make_incremental_sync(lag="1 hour")
    observer = StatePersistingObserver(state_manager=None, watermark_storage=storage)

    run_sync(
        sync,
        source,
        CollectDestination(),
        _make_profile(),
        tmp_path,
        watermark_storage=storage,
        observer=observer,
    )

    assert storage.saved == [("lag_sync", "2026-07-10 12:00:00")]


def test_run_sync_lag_skipped_for_cli_override(tmp_path: Path) -> None:
    source = QueryCapturingSource([])
    storage = FakeWatermarkStorage("2026-07-10 12:00:00")
    sync = _make_incremental_sync(lag="1 hour")

    result = run_sync(
        sync,
        source,
        CollectDestination(),
        _make_profile(),
        tmp_path,
        watermark_storage=storage,
        cursor_value_override="2026-07-10 06:00:00",
    )

    assert "2026-07-10 06:00:00" in source.queries[0]
    assert result.watermark_lag is None
    assert result.watermark_source == "cli_override"


def test_run_sync_lag_skipped_for_default_value_first_run(tmp_path: Path) -> None:
    source = QueryCapturingSource([])
    storage = FakeWatermarkStorage(None)  # nothing stored yet — first run
    sync = _make_incremental_sync(lag="1 hour")

    result = run_sync(
        sync, source, CollectDestination(), _make_profile(), tmp_path, watermark_storage=storage
    )

    assert "2026-07-01 00:00:00" in source.queries[0]
    assert result.watermark_lag is None
    assert result.watermark_source == "default_value"


def test_run_sync_without_lag_unchanged(tmp_path: Path) -> None:
    source = QueryCapturingSource([])
    storage = FakeWatermarkStorage("2026-07-10 12:00:00")
    sync = _make_incremental_sync(lag=None)

    result = run_sync(
        sync, source, CollectDestination(), _make_profile(), tmp_path, watermark_storage=storage
    )

    assert "2026-07-10 12:00:00" in source.queries[0]
    assert result.watermark_lag is None
    assert result.cursor_value_used == "2026-07-10 12:00:00"
