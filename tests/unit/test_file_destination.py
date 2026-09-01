"""Unit tests for CSV/JSON/JSONL file destination.

Uses tmp_path for real file writes — no mocking needed, no extra dependencies.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import FileDestinationConfig, SyncConfig, SyncOptions
from drt.connectors import get_destination
from drt.destinations.file import FileDestination
from drt.engine.sync import run_sync

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(tmp_path: Path, **overrides: Any) -> FileDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "file",
        "path": str(tmp_path / "output.csv"),
        "format": "csv",
    }
    defaults.update(overrides)
    return FileDestinationConfig(**defaults)


class _RowsSource:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _sync(
    path: Path,
    file_format: str,
    *,
    batch_size: int = 100,
    on_error: str = "fail",
    name: str = "file_sync",
) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": name,
            "model": "ref('rows')",
            "destination": {
                "type": "file",
                "path": str(path),
                "format": file_format,
            },
            "sync": {"batch_size": batch_size, "on_error": on_error},
        }
    )


def _read_records(path: Path, file_format: str) -> list[dict[str, Any]]:
    if file_format == "csv":
        with path.open(newline="", encoding="utf-8") as f:
            return [{"id": int(row["id"]), "name": row["name"]} for row in csv.DictReader(f)]
    if file_format == "json":
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestFileDestinationConfig:
    def test_valid_csv_config(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        assert config.format == "csv"

    def test_json_format(self, tmp_path: Path) -> None:
        config = _config(tmp_path, format="json")
        assert config.format == "json"

    def test_jsonl_format(self, tmp_path: Path) -> None:
        config = _config(tmp_path, format="jsonl")
        assert config.format == "jsonl"

    def test_invalid_format_rejected(self, tmp_path: Path) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="format"):
            _config(tmp_path, format="xml")


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


class TestCsvDestination:
    def test_csv_write(self, tmp_path: Path) -> None:
        records = [
            {"id": 1, "name": "alice", "score": 95},
            {"id": 2, "name": "bob", "score": 80},
        ]
        config = _config(tmp_path)
        result = FileDestination().load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0
        assert Path(config.path).exists()

    def test_csv_content_readable(self, tmp_path: Path) -> None:
        records = [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]
        config = _config(tmp_path)
        FileDestination().load(records, config, _options())

        with open(config.path, newline="", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
        assert len(reader) == 2
        assert reader[0]["name"] == "alice"
        assert reader[1]["name"] == "bob"

    def test_csv_creates_parent_dirs(self, tmp_path: Path) -> None:
        deep_path = str(tmp_path / "a" / "b" / "output.csv")
        config = _config(tmp_path, path=deep_path)
        result = FileDestination().load([{"id": 1}], config, _options())

        assert result.success == 1
        assert Path(deep_path).exists()


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


class TestJsonDestination:
    def test_json_write(self, tmp_path: Path) -> None:
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        config = _config(tmp_path, format="json", path=str(tmp_path / "out.json"))
        result = FileDestination().load(records, config, _options())

        assert result.success == 2
        with open(config.path, encoding="utf-8") as f:
            data = json.load(f)
        assert len(data) == 2
        assert data[0]["name"] == "alice"

    def test_json_handles_non_serializable(self, tmp_path: Path) -> None:
        from datetime import datetime

        records = [{"id": 1, "ts": datetime(2026, 1, 1)}]
        config = _config(tmp_path, format="json", path=str(tmp_path / "out.json"))
        result = FileDestination().load(records, config, _options())

        assert result.success == 1  # default=str handles it


# ---------------------------------------------------------------------------
# JSONL
# ---------------------------------------------------------------------------


class TestJsonlDestination:
    def test_jsonl_write(self, tmp_path: Path) -> None:
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        config = _config(tmp_path, format="jsonl", path=str(tmp_path / "out.jsonl"))
        result = FileDestination().load(records, config, _options())

        assert result.success == 2
        with open(config.path, encoding="utf-8") as f:
            lines = [json.loads(line) for line in f]
        assert len(lines) == 2
        assert lines[1]["name"] == "bob"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestFileDestinationEdgeCases:
    def test_empty_records(self, tmp_path: Path) -> None:
        config = _config(tmp_path)
        result = FileDestination().load([], config, _options())

        assert result.success == 0
        assert result.failed == 0
        assert not Path(config.path).exists()

    def test_error_returns_failure(self, tmp_path: Path) -> None:
        config = _config(tmp_path, path="")
        result = FileDestination().load([{"id": 1}], config, _options())

        assert result.failed == 1
        assert len(result.errors) > 0


# ---------------------------------------------------------------------------
# Engine batching regressions (#1002)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("file_format", ["csv", "json", "jsonl"])
def test_engine_sync_accumulates_all_batches(tmp_path: Path, file_format: str) -> None:
    records = [{"id": i, "name": f"user-{i}"} for i in range(250)]
    output_path = tmp_path / f"output.{file_format}"
    sync = _sync(output_path, file_format, batch_size=100)

    result = run_sync(
        sync,
        _RowsSource(records),
        FileDestination(),
        _profile(),
        tmp_path,
    )

    assert result.success == 250
    assert result.failed == 0
    assert result.rows_extracted == 250
    assert _read_records(output_path, file_format) == records
    if file_format == "csv":
        assert output_path.read_text(encoding="utf-8").splitlines().count("id,name") == 1


def test_separate_destination_instances_do_not_share_write_state(tmp_path: Path) -> None:
    first_records = [{"id": i, "name": f"first-{i}"} for i in range(150)]
    second_records = [{"id": i, "name": f"second-{i}"} for i in range(175)]
    first_path = tmp_path / "first.jsonl"
    second_path = tmp_path / "second.jsonl"
    first_path.write_text("stale first run\n", encoding="utf-8")
    second_path.write_text("stale second run\n", encoding="utf-8")
    first_sync = _sync(first_path, "jsonl", name="first_file_sync")
    second_sync = _sync(second_path, "jsonl", name="second_file_sync")
    first_destination = get_destination(first_sync.destination)
    second_destination = get_destination(second_sync.destination)

    assert isinstance(first_destination, FileDestination)
    assert isinstance(second_destination, FileDestination)
    assert first_destination is not second_destination

    first_result = run_sync(
        first_sync,
        _RowsSource(first_records),
        first_destination,
        _profile(),
        tmp_path,
    )
    second_result = run_sync(
        second_sync,
        _RowsSource(second_records),
        second_destination,
        _profile(),
        tmp_path,
    )

    assert first_result.success == 150
    assert second_result.success == 175
    assert _read_records(first_path, "jsonl") == first_records
    assert _read_records(second_path, "jsonl") == second_records


def test_csv_column_mismatch_fails_batch_and_on_error_fail_stops(
    tmp_path: Path,
) -> None:
    first_batch = [{"id": i, "name": f"user-{i}"} for i in range(100)]
    mismatched_batch = [{"id": i, "email": f"user-{i}@example.com"} for i in range(100, 200)]
    unconsumed_batch = [{"id": i, "name": f"user-{i}"} for i in range(200, 250)]
    output_path = tmp_path / "mismatch.csv"
    sync = _sync(output_path, "csv", batch_size=100, on_error="fail")

    result = run_sync(
        sync,
        _RowsSource([*first_batch, *mismatched_batch, *unconsumed_batch]),
        FileDestination(),
        _profile(),
        tmp_path,
    )

    assert result.success == 100
    assert result.failed == 100
    assert result.rows_extracted == 200
    assert len(result.errors) == 1
    assert "CSV column mismatch" in result.errors[0]
    assert "missing ['name']" in result.errors[0]
    assert "unexpected ['email']" in result.errors[0]
    assert _read_records(output_path, "csv") == first_batch


@pytest.mark.parametrize("file_format", ["csv", "json", "jsonl"])
def test_reused_instance_does_not_leak_state_into_a_later_run(
    tmp_path: Path, file_format: str
) -> None:
    """A library caller may reuse one FileDestination across separate
    run_sync() calls. The second run must still start fresh, not append
    to / fold in the first run's records (#1006 review)."""
    destination = FileDestination()
    output_path = tmp_path / f"output.{file_format}"

    first_records = [{"id": i, "name": f"first-{i}"} for i in range(5)]
    first_sync = _sync(output_path, file_format, batch_size=100)
    first_result = run_sync(
        first_sync, _RowsSource(first_records), destination, _profile(), tmp_path
    )
    assert first_result.success == 5

    second_records = [{"id": i, "name": f"second-{i}"} for i in range(3)]
    second_sync = _sync(output_path, file_format, batch_size=100, name="second_run")
    second_result = run_sync(
        second_sync, _RowsSource(second_records), destination, _profile(), tmp_path
    )
    assert second_result.success == 3

    assert _read_records(output_path, file_format) == second_records
