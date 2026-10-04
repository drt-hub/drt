"""Unit tests for BigQuery warehouse-backed state/history/DLQ stores (#1107)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import BigQueryProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_bigquery import (
    BigQueryWarehouseDlqBackend,
    BigQueryWarehouseHistoryStore,
    BigQueryWarehouseStateStore,
)


@dataclass
class _ScalarParameter:
    name: str | None
    type_: str
    value: Any


@dataclass
class _ScalarParameterType:
    type_: str
    name: str | None = None


class _StructParameter:
    def __init__(self, name: str | None, *sub_params: _ScalarParameter) -> None:
        self.name = name
        self.sub_params = sub_params


class _StructParameterType:
    def __init__(self, *fields: _ScalarParameterType) -> None:
        self.fields = fields


@dataclass
class _ArrayParameter:
    name: str
    array_type: Any
    values: list[Any]


@dataclass
class _SchemaField:
    name: str
    field_type: str
    mode: str = "NULLABLE"


@dataclass
class _Table:
    table_id: str
    schema: list[_SchemaField]


@dataclass
class _QueryJobConfig:
    query_parameters: list[Any]


@pytest.fixture(autouse=True)
def fake_bigquery(monkeypatch: pytest.MonkeyPatch) -> None:
    bigquery = ModuleType("google.cloud.bigquery")
    setattr(bigquery, "ScalarQueryParameter", _ScalarParameter)
    setattr(bigquery, "ScalarQueryParameterType", _ScalarParameterType)
    setattr(bigquery, "StructQueryParameter", _StructParameter)
    setattr(bigquery, "StructQueryParameterType", _StructParameterType)
    setattr(bigquery, "ArrayQueryParameter", _ArrayParameter)
    setattr(bigquery, "SchemaField", _SchemaField)
    setattr(bigquery, "Table", _Table)
    setattr(bigquery, "QueryJobConfig", _QueryJobConfig)

    cloud = ModuleType("google.cloud")
    setattr(cloud, "bigquery", bigquery)
    google = ModuleType("google")
    setattr(google, "cloud", cloud)
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery)


def _profile(**overrides: Any) -> BigQueryProfile:
    defaults: dict[str, Any] = {
        "type": "bigquery",
        "project": "my-proj",
        "dataset": "analytics",
        "managed_schema": "_drt",
    }
    defaults.update(overrides)
    return BigQueryProfile(**defaults)


def _client(*results: Any) -> MagicMock:
    client = MagicMock()
    job = MagicMock()
    job.result.side_effect = list(results)
    client.query.return_value = job
    return client


def _sqls(client: MagicMock) -> list[str]:
    return [call.args[0] for call in client.query.call_args_list]


def _params(call: Any) -> list[Any]:
    return call.kwargs["job_config"].query_parameters


def _history_entry(**overrides: Any) -> HistoryEntry:
    defaults: dict[str, Any] = {
        "sync_name": "s",
        "started_at": "t0",
        "completed_at": "t1",
        "duration_seconds": 1.5,
        "status": "success",
        "records_synced": 3,
        "records_failed": 0,
        "errors": ["boom"],
        "cursor_value_used": "c",
        "dry_run": False,
        "run_id": "run-1",
        "sync_run_id": "sync-run-1",
    }
    defaults.update(overrides)
    return HistoryEntry(**defaults)


def _dead_letter(**overrides: Any) -> DeadLetter:
    defaults: dict[str, Any] = {
        "record": {"a": 1},
        "error_message": "boom",
        "http_status": 500,
        "timestamp": "t0",
        "attempts": 1,
        "sync_run_id": "run-1",
        "id": "id-1",
    }
    defaults.update(overrides)
    return DeadLetter(**defaults)


class TestHelpersAndBootstrap:
    def test_existing_frozen_protocols_are_satisfied(self) -> None:
        from drt.state.dlq import DlqBackend
        from drt.state.history import HistoryStore
        from drt.state.manager import StateStore

        profile = _profile()
        assert isinstance(BigQueryWarehouseStateStore(profile), StateStore)
        assert isinstance(BigQueryWarehouseHistoryStore(profile), HistoryStore)
        assert isinstance(BigQueryWarehouseDlqBackend(profile), DlqBackend)

    def test_store_specific_ensure_table_wrappers(self) -> None:
        client = MagicMock()
        with patch("drt.state.warehouse_bigquery._ensure_table_exists") as ensure:
            BigQueryWarehouseStateStore(_profile())._ensure_table(client)
            BigQueryWarehouseHistoryStore(_profile())._ensure_table(client)
            BigQueryWarehouseDlqBackend(_profile())._ensure_table(client)
        assert [call.args[2] for call in ensure.call_args_list] == [
            "_drt_runs",
            "_drt_history",
            "_drt_dlq",
        ]

    def test_connect_and_table_probe_delegate_to_bigquery_source(self) -> None:
        from drt.state.warehouse_bigquery import _connect, _table_exists

        client = MagicMock()
        with (
            patch(
                "drt.sources.bigquery.BigQuerySource._build_client", return_value=client
            ) as build,
            patch(
                "drt.sources.bigquery.BigQuerySource.managed_table_exists", return_value=True
            ) as exists,
        ):
            assert _connect(_profile()) is client
            assert _table_exists(_profile(), "_drt_runs") is True
        build.assert_called_once()
        exists.assert_called_once()

    def test_qualified_identifier_is_backtick_quoted_and_validated(self) -> None:
        from drt.state.warehouse_bigquery import _qualified

        assert _qualified(_profile(), "_drt_runs") == "`my-proj._drt._drt_runs`"
        for overrides in (
            {"project": "bad`project"},
            {"managed_schema": "bad-dataset"},
        ):
            with pytest.raises(ValueError, match="Invalid BigQuery"):
                _qualified(_profile(**overrides), "_drt_runs")
        with pytest.raises(ValueError, match="Invalid BigQuery table"):
            _qualified(_profile(), "bad table")

    def test_ensure_table_creates_client_api_table(self) -> None:
        from drt.state.warehouse_bigquery import _RUNS_SCHEMA, _ensure_table_exists

        client = MagicMock()
        with (
            patch("drt.sources.bigquery.BigQuerySource.ensure_managed_schema") as ensure,
            patch("drt.sources.bigquery.BigQuerySource.managed_table_exists", return_value=False),
        ):
            _ensure_table_exists(client, _profile(), "_drt_runs", _RUNS_SCHEMA)
        ensure.assert_called_once()
        table = client.create_table.call_args.args[0]
        assert table.table_id == "my-proj._drt._drt_runs"
        assert [field.name for field in table.schema] == [
            "sync_name",
            "last_run_at",
            "records_synced",
            "status",
            "error",
            "last_cursor_value",
        ]
        client.query.assert_not_called()

    def test_ensure_table_skips_create_when_preprovisioned(self) -> None:
        from drt.state.warehouse_bigquery import _RUNS_SCHEMA, _ensure_table_exists

        client = MagicMock()
        with (
            patch("drt.sources.bigquery.BigQuerySource.ensure_managed_schema"),
            patch("drt.sources.bigquery.BigQuerySource.managed_table_exists", return_value=True),
        ):
            _ensure_table_exists(client, _profile(), "_drt_runs", _RUNS_SCHEMA)
        client.create_table.assert_not_called()

    def test_ensure_table_handles_create_race_and_reraises_real_failure(self) -> None:
        from drt.state.warehouse_bigquery import _RUNS_SCHEMA, _ensure_table_exists

        client = MagicMock()
        client.create_table.side_effect = RuntimeError("already exists")
        with (
            patch("drt.sources.bigquery.BigQuerySource.ensure_managed_schema"),
            patch(
                "drt.sources.bigquery.BigQuerySource.managed_table_exists",
                side_effect=[False, True],
            ),
        ):
            _ensure_table_exists(client, _profile(), "_drt_runs", _RUNS_SCHEMA)

        with (
            patch("drt.sources.bigquery.BigQuerySource.ensure_managed_schema"),
            patch(
                "drt.sources.bigquery.BigQuerySource.managed_table_exists",
                side_effect=[False, False],
            ),
            pytest.raises(RuntimeError, match="already exists"),
        ):
            _ensure_table_exists(client, _profile(), "_drt_runs", _RUNS_SCHEMA)


class TestBigQueryWarehouseStateStore:
    def test_empty_reads_and_reset_before_table_exists(self) -> None:
        store = BigQueryWarehouseStateStore(_profile())
        with patch("drt.state.warehouse_bigquery._table_exists", return_value=False):
            assert store.get_last_sync("s") is None
            assert store.get_all() == {}
            assert store.reset("s") is False

    def test_get_last_sync_reconstructs_and_handles_missing_row(self) -> None:
        row = ("t", 5, "success", None, "cursor")
        client = _client([row])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert BigQueryWarehouseStateStore(_profile()).get_last_sync("s") == SyncState(
                sync_name="s",
                last_run_at="t",
                records_synced=5,
                status="success",
                error=None,
                last_cursor_value="cursor",
            )
        assert "@sync_name" in _sqls(client)[0]
        assert _params(client.query.call_args_list[0])[0].value == "s"

        client = _client([])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert BigQueryWarehouseStateStore(_profile()).get_last_sync("s") is None

    def test_get_all_reconstructs_every_row(self) -> None:
        client = _client(
            [
                ("a", "t0", 1, "success", None, None),
                ("b", "t1", 2, "failed", "boom", "c"),
            ]
        )
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            result = BigQueryWarehouseStateStore(_profile()).get_all()
        assert set(result) == {"a", "b"}
        assert result["b"].error == "boom"

    @pytest.mark.parametrize("probe, verb", [([], "INSERT INTO"), ([(1,)], "UPDATE")])
    def test_save_sync_probe_then_acts_with_bound_values(
        self, probe: list[tuple[int]], verb: str
    ) -> None:
        client = _client(probe, [])
        store = BigQueryWarehouseStateStore(_profile())
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(store, "_ensure_table"),
        ):
            store.save_sync(
                SyncState(
                    sync_name="s'quoted",
                    last_run_at="t",
                    records_synced=1,
                    status="failed",
                    error="boom'); DROP TABLE x; --",
                    last_cursor_value="c",
                )
            )
        assert _sqls(client)[1].startswith(verb)
        assert "DROP TABLE" not in _sqls(client)[1]
        assert {p.name: p.value for p in _params(client.query.call_args_list[1])}["error"] == (
            "boom'); DROP TABLE x; --"
        )

    def test_reset_probes_then_deletes_and_now_is_iso(self) -> None:
        client = _client([(1,)], [])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            store = BigQueryWarehouseStateStore(_profile())
            assert store.reset("s") is True
            from datetime import datetime

            datetime.fromisoformat(store.now())
        assert _sqls(client)[1].startswith("DELETE FROM")

        client = _client([], [])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert BigQueryWarehouseStateStore(_profile()).reset("s") is False


class TestBigQueryWarehouseHistoryStore:
    def test_empty_read_and_prune_before_table_exists(self) -> None:
        store = BigQueryWarehouseHistoryStore(_profile())
        with patch("drt.state.warehouse_bigquery._table_exists", return_value=False):
            assert store.read() == []
            assert store.prune("s", 30) == 0

    def test_append_uses_bound_json_string_and_is_best_effort(self) -> None:
        client = _client([])
        store = BigQueryWarehouseHistoryStore(_profile())
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(store, "_ensure_table"),
        ):
            store.append(_history_entry(errors=["a", "b", "c", "d", "e", "ignored"]))
        params = {p.name: p.value for p in _params(client.query.call_args_list[0])}
        assert params["errors"] == '["a", "b", "c", "d", "e"]'
        assert "@errors" in _sqls(client)[0]

        with patch("drt.state.warehouse_bigquery._connect", side_effect=RuntimeError("offline")):
            store.append(_history_entry())  # must not raise

    def test_read_filtered_and_unfiltered_reconstruct_json(self) -> None:
        row = (
            "s",
            "t0",
            "t1",
            1.5,
            "success",
            3,
            0,
            '["boom"]',
            "c",
            False,
            "run-1",
            "sync-run-1",
        )
        for sync_name, expected_where in (("s", True), (None, False)):
            client = _client([row])
            with (
                patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
                patch("drt.state.warehouse_bigquery._connect", return_value=client),
            ):
                entries = BigQueryWarehouseHistoryStore(_profile()).read(sync_name, limit=7)
            assert entries == [_history_entry()]
            assert ("WHERE sync_name" in _sqls(client)[0]) is expected_where

    def test_prune_counts_then_deletes_or_skips(self) -> None:
        client = _client([(3,)], [])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert BigQueryWarehouseHistoryStore(_profile()).prune("s", 30) == 3
        assert len(_sqls(client)) == 2

        for rows in ([(0,)], []):
            client = _client(rows)
            with (
                patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
                patch("drt.state.warehouse_bigquery._connect", return_value=client),
            ):
                assert BigQueryWarehouseHistoryStore(_profile()).prune("s", 30) == 0
            assert len(_sqls(client)) == 1


class TestBigQueryWarehouseDlqBackend:
    def test_empty_reads_before_table_exists(self) -> None:
        backend = BigQueryWarehouseDlqBackend(_profile())
        with patch("drt.state.warehouse_bigquery._table_exists", return_value=False):
            assert backend.read("s") == []
            assert backend.depth("s") == 0
            assert backend.all_depths() == {}

    def test_append_empty_is_depth_only(self) -> None:
        backend = BigQueryWarehouseDlqBackend(_profile())
        with patch.object(backend, "depth", return_value=4) as depth:
            assert backend.append("s", []) == 4
        depth.assert_called_once_with("s")

    def test_append_merges_deduped_batch_prunes_and_binds_values(self) -> None:
        client = _client([], [])
        backend = BigQueryWarehouseDlqBackend(_profile())
        entries = [
            _dead_letter(id="id-1", error_message="old"),
            _dead_letter(id="id-1", error_message="new'; DROP TABLE x; --"),
            _dead_letter(id="id-2", record={"n": 2}),
        ]
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(backend, "depth", return_value=2),
        ):
            assert backend.append("s", entries, max_records=2) == 2
        sqls = _sqls(client)
        assert len(sqls) == 2
        assert sqls[0].startswith("MERGE")
        assert "ON t.sync_name = s.sync_name AND t.id = s.id" in sqls[0]
        assert sqls[-1].startswith("DELETE FROM")
        assert all("DROP TABLE" not in sql for sql in sqls)
        array_param = _params(client.query.call_args_list[0])[0]
        assert isinstance(array_param, _ArrayParameter)
        assert len(array_param.values) == 2
        assert array_param.values[0].sub_params[3].value == "new'; DROP TABLE x; --"

    def test_append_skips_prune_when_max_records_is_not_positive(self) -> None:
        client = _client([])
        backend = BigQueryWarehouseDlqBackend(_profile())
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(backend, "depth", return_value=1),
        ):
            backend.append("s", [_dead_letter()], max_records=0)
        assert not any(sql.startswith("DELETE FROM") for sql in _sqls(client))

    def test_replace_merges_deduped_struct_rows_before_deleting_stale_ids(self) -> None:
        client = _client([("keep",), ("stale",)], [], [])
        backend = BigQueryWarehouseDlqBackend(_profile())
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
        ):
            backend.replace(
                "s",
                [
                    _dead_letter(id="keep", error_message="first"),
                    _dead_letter(id="keep", error_message="last"),
                ],
            )
        sqls = _sqls(client)
        merge_index = next(i for i, sql in enumerate(sqls) if sql.startswith("MERGE"))
        delete_index = next(i for i, sql in enumerate(sqls) if sql.startswith("DELETE"))
        assert merge_index < delete_index
        assert "USING (SELECT * FROM UNNEST(@rows)) AS s" in sqls[merge_index]
        assert "ON t.sync_name = s.sync_name AND t.id = s.id" in sqls[merge_index]
        array_param = _params(client.query.call_args_list[merge_index])[0]
        assert isinstance(array_param, _ArrayParameter)
        assert len(array_param.values) == 1
        assert array_param.values[0].sub_params[3].value == "last"
        assert _params(client.query.call_args_list[delete_index])[1].values == ["stale"]

    def test_replace_chunks_merges_and_stale_deletes(self) -> None:
        from drt.state import warehouse_bigquery

        client = _client([("stale-1",), ("stale-2",)], [], [], [], [])
        backend = BigQueryWarehouseDlqBackend(_profile())
        entries = [_dead_letter(id=f"new-{i}") for i in range(3)]
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(warehouse_bigquery, "_DLQ_MERGE_CHUNK_SIZE", 2),
        ):
            backend.replace("s", entries)
        assert sum(sql.startswith("MERGE") for sql in _sqls(client)) == 2
        assert sum(sql.startswith("DELETE") for sql in _sqls(client)) == 1

    def test_append_chunks_struct_rows_by_encoded_payload_size(self) -> None:
        from drt.state import warehouse_bigquery

        client = _client([], [])
        backend = BigQueryWarehouseDlqBackend(_profile())
        entries = [
            _dead_letter(id="id-1", record={"payload": "x" * 250}),
            _dead_letter(id="id-2", record={"payload": "y" * 250}),
        ]
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(backend, "depth", return_value=2),
            patch.object(warehouse_bigquery, "_DLQ_STRUCT_ARRAY_MAX_BYTES", 500),
        ):
            assert backend.append("s", entries, max_records=0) == 2
        assert sum(sql.startswith("MERGE") for sql in _sqls(client)) == 2

    def test_oversized_struct_row_names_entry_before_issuing_dml(self) -> None:
        from drt.state import warehouse_bigquery

        client = MagicMock()
        backend = BigQueryWarehouseDlqBackend(_profile())
        entries = [
            _dead_letter(id="small"),
            _dead_letter(id="too-large", record={"payload": "x" * 500}),
        ]
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(warehouse_bigquery, "_DLQ_STRUCT_ARRAY_MAX_BYTES", 256),
            pytest.raises(ValueError, match="DLQ entry 'too-large'.*maximum is 256 bytes"),
        ):
            backend.append("s", entries)
        client.query.assert_not_called()

    def test_replace_and_clear_empty_delete_without_merge(self) -> None:
        for method in ("replace", "clear"):
            client = _client([])
            backend = BigQueryWarehouseDlqBackend(_profile())
            with (
                patch("drt.state.warehouse_bigquery._connect", return_value=client),
                patch.object(backend, "_ensure_table"),
            ):
                if method == "replace":
                    backend.replace("s", [])
                else:
                    backend.clear("s")
            assert _sqls(client) == [
                "DELETE FROM `my-proj._drt._drt_dlq` WHERE sync_name = @sync_name"
            ]

    def test_read_depth_and_all_depths(self) -> None:
        row = ("id-1", '{"a": 1}', "boom", 500, "t0", 2, "run-1")
        backend = BigQueryWarehouseDlqBackend(_profile())
        client = _client([row])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert backend.read("s") == [
                DeadLetter(
                    record={"a": 1},
                    error_message="boom",
                    http_status=500,
                    timestamp="t0",
                    attempts=2,
                    sync_run_id="run-1",
                    id="id-1",
                )
            ]

        client = _client([(4,)])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert backend.depth("s") == 4
        client = _client([])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert backend.depth("s") == 0

        client = _client([("a", 3), ("b", 0)])
        with (
            patch("drt.state.warehouse_bigquery._table_exists", return_value=True),
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
        ):
            assert backend.all_depths() == {"a": 3}

    def test_reconcile_removes_and_update_merges_then_reads(self) -> None:
        client = _client([], [])
        backend = BigQueryWarehouseDlqBackend(_profile())
        expected = [_dead_letter(attempts=2)]
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(backend, "read", return_value=expected),
        ):
            assert (
                backend.reconcile(
                    "s",
                    remove_ids=["gone"],
                    updates={"id-1": _dead_letter(attempts=2, record={"a": 2})},
                )
                == expected
            )
        assert _sqls(client)[0].startswith("DELETE")
        assert _params(client.query.call_args_list[0])[1].values == ["gone"]
        assert _sqls(client)[1].startswith("MERGE")
        assert "WHEN NOT MATCHED" not in _sqls(client)[1]
        assert "ON t.id = s.id AND t.sync_name = @sync_name" in _sqls(client)[1]
        assert _params(client.query.call_args_list[1])[0].values[0].sub_params[1].value == (
            '{"a": 2}'
        )

    def test_reconcile_no_changes_only_reads(self) -> None:
        client = MagicMock()
        backend = BigQueryWarehouseDlqBackend(_profile())
        with (
            patch("drt.state.warehouse_bigquery._connect", return_value=client),
            patch.object(backend, "_ensure_table"),
            patch.object(backend, "read", return_value=[]) as read,
        ):
            assert backend.reconcile("s") == []
        client.query.assert_not_called()
        read.assert_called_once_with("s")
