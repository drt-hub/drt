"""Column-contract config, pure checking, and real engine-path tests (#761)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import ColumnConfig, DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.contract import check_contract, drop_extra_columns, python_value_type
from drt.engine.observer import NullObserver
from drt.engine.plan import config_hash
from drt.engine.sync import run_sync


def _sync(**overrides: Any) -> SyncConfig:
    data: dict[str, Any] = {
        "name": "contracted",
        "model": "SELECT 1",
        "destination": {"type": "rest_api", "url": "https://example.com"},
        "columns": [
            {"name": "id", "type": "integer"},
            {"name": "email", "type": "string", "description": "Primary identifier"},
        ],
        "contract": {"enforced": True},
        "sync": {"batch_size": 2},
    }
    data.update(overrides)
    return SyncConfig.model_validate(data)


class TestColumnContractConfig:
    def test_valid_declaration_and_defaults(self) -> None:
        sync = _sync()

        assert sync.columns is not None
        assert sync.columns[1].description == "Primary identifier"
        assert sync.contract is not None
        assert sync.contract.on_extra_columns == "fail"

    @pytest.mark.parametrize(
        "portable_type",
        [
            "string",
            "number",
            "integer",
            "boolean",
            "timestamp",
            "date",
            "json",
        ],
    )
    def test_all_portable_types_validate(self, portable_type: str) -> None:
        sync = _sync(columns=[{"name": "value", "type": portable_type}])
        assert sync.columns is not None
        assert sync.columns[0].type == portable_type

    def test_columns_can_be_documentation_only(self) -> None:
        sync = _sync(contract=None)
        assert sync.columns is not None
        assert sync.contract is None

    def test_contract_without_columns_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="contract requires a non-empty columns"):
            _sync(columns=None)

    def test_empty_columns_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="at least 1 item"):
            _sync(columns=[])

    def test_duplicate_names_are_rejected(self) -> None:
        with pytest.raises(ValidationError, match="duplicate name.*'id'"):
            _sync(columns=[{"name": "id", "type": "integer"}] * 2)

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("columns", [{"name": "id", "type": "decimal"}], "literal_error"),
            ("columns", [{"name": " ", "type": "string"}], "non-empty column name"),
            ("columns", [{"name": "id", "type": "integer", "nullable": False}], "extra_forbidden"),
            ("contract", {"enforced": True, "unknown": "x"}, "extra_forbidden"),
            (
                "contract",
                {"enforced": True, "on_extra_columns": "keep"},
                "literal_error",
            ),
            ("contract", {"on_extra_columns": "fail"}, "Field required"),
        ],
    )
    def test_strict_invalid_blocks_are_rejected(self, field: str, value: Any, message: str) -> None:
        with pytest.raises(ValidationError, match=message):
            _sync(**{field: value})

    def test_contract_changes_plan_config_hash(self) -> None:
        base = _sync()
        changed = _sync(contract={"enforced": True, "on_extra_columns": "drop"})
        assert config_hash(base) != config_hash(changed)


class TestPureContractCheck:
    def test_python_type_mapping_is_documented_mapping(self) -> None:
        assert python_value_type("x") == "string"
        assert python_value_type(True) == "boolean"
        assert python_value_type(1) == "integer"
        assert python_value_type(1.5) == "number"
        assert python_value_type(Decimal("1.2")) == "number"
        assert python_value_type(datetime(2026, 1, 1)) == "timestamp"
        assert python_value_type(date(2026, 1, 1)) == "date"
        assert python_value_type({"x": 1}) == "json"
        assert python_value_type([1]) == "json"
        assert python_value_type(b"x") == "bytes"

    def test_null_is_compatible_and_number_accepts_integer_but_not_bool(self) -> None:
        columns = [ColumnConfig(name="amount", type="number")]
        assert check_contract([{"amount": None}, {"amount": 1}], columns, "fail").passed

        failed = check_contract([{"amount": True}], columns, "fail")
        assert not failed.passed
        assert failed.violations[0].actual == "boolean"

        integer_failed = check_contract(
            [{"amount": True}], [ColumnConfig(name="amount", type="integer")], "fail"
        )
        assert integer_failed.violations[0].actual == "boolean"

    def test_reports_every_missing_mismatched_and_extra_column(self) -> None:
        columns = [
            ColumnConfig(name="id", type="integer"),
            ColumnConfig(name="email", type="string"),
            ColumnConfig(name="missing", type="date"),
        ]
        checked = check_contract(
            [{"id": "one", "email": 42, "extra": True}, {"id": 2, "email": None}],
            columns,
            "fail",
        )

        assert [(v.column, v.expected, v.actual) for v in checked.violations] == [
            ("id", "integer", "string"),
            ("email", "string", "integer"),
            ("missing", "date", "missing"),
            ("extra", "not declared", "boolean"),
        ]
        message = checked.message()
        assert all(name in message for name in ("id", "email", "missing", "extra"))

    def test_sparse_batch_unions_keys_and_ignore_allows_extras(self) -> None:
        columns = [
            ColumnConfig(name="id", type="integer"),
            ColumnConfig(name="email", type="string"),
        ]
        assert check_contract([{"id": 1}, {"email": "a", "extra": 1}], columns, "ignore").passed

    def test_drop_returns_new_dicts_with_only_declared_keys(self) -> None:
        records = [{"id": 1, "extra": "x"}, {"extra": "y", "id": 2}]
        filtered = drop_extra_columns(records, [ColumnConfig(name="id", type="integer")])
        assert filtered == [{"id": 1}, {"id": 2}]
        assert records[0] == {"id": 1, "extra": "x"}


class _Source:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(
        self, query: str, config: ProfileConfig, *, query_tags: dict[str, str] | None = None
    ) -> Iterator[dict[str, Any]]:
        yield from self.rows


class _Destination:
    def __init__(self) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        self.finalized = False

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append(records)
        return SyncResult(success=len(records))

    def finalize_sync(
        self, config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult | None:
        self.finalized = True
        return None


class _Observer(NullObserver):
    def __init__(self) -> None:
        self.completed: SyncResult | None = None

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        self.completed = result


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


class TestEngineContractPath:
    def test_violation_fails_before_any_write_and_reports_through_observer(
        self, tmp_path: Path
    ) -> None:
        destination = _Destination()
        observer = _Observer()

        result = run_sync(
            _sync(),
            _Source([{"id": "wrong", "unexpected": 1}, {"id": 2}]),
            destination,
            _profile(),
            tmp_path,
            observer=observer,
        )

        assert result.failed == 1
        assert result.success == 0
        assert result.rows_extracted == 2
        assert "'id' expected integer, actual string" in result.errors[0]
        assert "'email' expected string, actual missing" in result.errors[0]
        assert "'unexpected' expected not declared, actual integer" in result.errors[0]
        assert destination.calls == []
        assert not destination.finalized
        assert observer.completed is result

    def test_drop_filters_every_batch_before_load(self, tmp_path: Path) -> None:
        destination = _Destination()
        sync = _sync(contract={"enforced": True, "on_extra_columns": "drop"})

        result = run_sync(
            sync,
            _Source(
                [
                    {"id": 1, "email": "a", "first_extra": 1},
                    {"id": 2, "email": "b"},
                    {"id": 3, "email": "c", "later_extra": 2},
                ]
            ),
            destination,
            _profile(),
            tmp_path,
        )

        assert result.failed == 0
        assert result.success == 3
        assert destination.calls == [
            [{"id": 1, "email": "a"}, {"id": 2, "email": "b"}],
            [{"id": 3, "email": "c"}],
        ]

    def test_ignore_preserves_extra_columns(self, tmp_path: Path) -> None:
        destination = _Destination()
        sync = _sync(contract={"enforced": True, "on_extra_columns": "ignore"})

        result = run_sync(
            sync,
            _Source([{"id": 1, "email": "a", "extra": True}]),
            destination,
            _profile(),
            tmp_path,
        )

        assert result.failed == 0
        assert destination.calls == [[{"id": 1, "email": "a", "extra": True}]]

    def test_type_check_is_limited_to_the_first_extracted_batch(self, tmp_path: Path) -> None:
        destination = _Destination()

        result = run_sync(
            _sync(),
            _Source(
                [
                    {"id": 1, "email": "a"},
                    {"id": 2, "email": "b"},
                    {"id": "later", "email": "c"},
                ]
            ),
            destination,
            _profile(),
            tmp_path,
        )

        assert result.failed == 0
        assert result.success == 3
        assert destination.calls[1] == [{"id": "later", "email": "c"}]

    def test_no_columns_path_and_unenforced_declaration_are_unchanged(self, tmp_path: Path) -> None:
        for sync in (
            _sync(columns=None, contract=None),
            _sync(contract={"enforced": False}),
        ):
            destination = _Destination()
            result = run_sync(
                sync,
                _Source([{"anything": object()}]),
                destination,
                _profile(),
                tmp_path,
            )
            assert result.success == 1
            assert result.failed == 0
            assert len(destination.calls) == 1

    def test_empty_extraction_has_no_batch_to_check(self, tmp_path: Path) -> None:
        destination = _Destination()
        result = run_sync(_sync(), _Source([]), destination, _profile(), tmp_path)
        assert result.failed == 0
        assert destination.calls == []

    def test_plan_dry_run_contract_failure_produces_a_failed_result_not_a_crash(
        self, tmp_path: Path
    ) -> None:
        destination = _Destination()
        result = run_sync(
            _sync(),
            _Source([{"id": "wrong"}]),
            destination,
            _profile(),
            tmp_path,
            dry_run=True,
            compute_diff=True,
        )

        assert result.failed == 1
        assert result.diff is not None
        assert "Column contract violation" in result.errors[0]
        assert destination.calls == []
