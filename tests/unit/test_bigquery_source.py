"""Unit tests for the BigQuery source.

Uses ``sys.modules`` injection to mock ``google.cloud.bigquery`` — no real
GCP project or ``google-cloud-bigquery`` install required (matches the
pattern in test_bigquery_destination.py).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import BigQueryProfile
from drt.sources.bigquery import BigQuerySource


def _config(**overrides: Any) -> BigQueryProfile:
    defaults: dict[str, Any] = {"type": "bigquery", "project": "my-proj", "dataset": "analytics"}
    defaults.update(overrides)
    return BigQueryProfile(**defaults)


class _NotFound(Exception):
    """Stand-in for google.api_core.exceptions.NotFound — a real Exception
    subclass is required since ``except NotFound:`` can't catch a MagicMock."""


class _Conflict(Exception):
    """Stand-in for google.api_core.exceptions.Conflict (dataset-create race)."""


class _Transient(Exception):
    """Stand-in for an error accepted by google-api-core's retry predicate."""


def _mocked_bq_modules(client: MagicMock) -> dict[str, MagicMock]:
    """sys.modules entries satisfying ``from google.cloud import bigquery``."""
    bigquery_mod = MagicMock()
    bigquery_mod.Client.return_value = client
    # QueryJobConfig(labels=...) needs to round-trip its kwargs for
    # assertions below, not collapse into an opaque MagicMock.
    bigquery_mod.QueryJobConfig.side_effect = lambda **kw: kw
    bigquery_mod.CopyJobConfig.side_effect = lambda **kw: kw
    bigquery_mod.WriteDisposition.WRITE_TRUNCATE = "WRITE_TRUNCATE"
    # Dataset(ref) needs a real-ish object so `.location = ...` sticks and
    # is inspectable, rather than silently accepted by a bare MagicMock
    # attribute (which it would be either way, but this keeps intent clear).
    bigquery_mod.Dataset.side_effect = lambda ref: MagicMock(reference=ref)

    cloud = MagicMock()
    cloud.bigquery = bigquery_mod
    google = MagicMock()
    google.cloud = cloud

    api_core_exceptions = MagicMock()
    api_core_exceptions.NotFound = _NotFound
    api_core_exceptions.Conflict = _Conflict
    api_core = MagicMock()
    api_core.exceptions = api_core_exceptions
    google.api_core = api_core
    api_core_retry = MagicMock()
    api_core_retry.if_transient_error.side_effect = lambda exc: isinstance(exc, _Transient)

    return {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.bigquery": bigquery_mod,
        "google.api_core": api_core,
        "google.api_core.exceptions": api_core_exceptions,
        "google.api_core.retry": api_core_retry,
    }


def _fake_client(rows: list[dict[str, Any]]) -> MagicMock:
    client = MagicMock()
    client.query.return_value.result.return_value = rows
    return client


@pytest.fixture
def mocked_bigquery(monkeypatch: pytest.MonkeyPatch):
    def _install(rows: list[dict[str, Any]]) -> MagicMock:
        client = _fake_client(rows)
        for name, mod in _mocked_bq_modules(client).items():
            monkeypatch.setitem(__import__("sys").modules, name, mod)
        return client

    return _install


class TestExtract:
    def test_yields_rows_as_dicts(self, mocked_bigquery: Any) -> None:
        mocked_bigquery([{"id": 1, "email": "a@x.com"}])
        rows = list(BigQuerySource().extract("SELECT * FROM t", _config()))
        assert rows == [{"id": 1, "email": "a@x.com"}]

    def test_no_query_tags_passes_no_job_config(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        list(BigQuerySource().extract("SELECT 1", _config()))
        client.query.assert_called_once_with("SELECT 1", job_config=None)


class TestJobConfig:
    def test_no_tags_is_none(self) -> None:
        assert BigQuerySource()._job_config(None) is None
        assert BigQuerySource()._job_config({}) is None

    def test_tags_become_normalized_labels(self, mocked_bigquery: Any) -> None:
        mocked_bigquery([])
        job_config = BigQuerySource()._job_config({"app": "drt", "sync": "Users -> HubSpot"})
        assert job_config == {"labels": {"app": "drt", "sync": "users----hubspot"}}

    def test_extract_threads_query_tags_into_job_config(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        list(
            BigQuerySource().extract(
                "SELECT 1", _config(), query_tags={"app": "drt", "run_id": "abc123"}
            )
        )
        _, kwargs = client.query.call_args
        assert kwargs["job_config"] == {"labels": {"app": "drt", "run_id": "abc123"}}


class TestConnection:
    def test_connection_ok(self, mocked_bigquery: Any) -> None:
        mocked_bigquery([])
        assert BigQuerySource().test_connection(_config()) is True

    def test_connection_false_on_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = MagicMock()
        client.query.side_effect = RuntimeError("no dice")
        import sys

        for name, mod in _mocked_bq_modules(client).items():
            monkeypatch.setitem(sys.modules, name, mod)
        assert BigQuerySource().test_connection(_config()) is False


def _install_client(monkeypatch: pytest.MonkeyPatch, client: MagicMock) -> None:
    import sys

    for name, mod in _mocked_bq_modules(client).items():
        monkeypatch.setitem(sys.modules, name, mod)


class TestManagedTableCapable:
    """#960/#1107 — client-API based (get_dataset/create_dataset/get_table/
    delete_table), not SQL DDL. Every probe/create/drop stays on this one
    surface, matching destinations/bigquery.py's test_connection() precedent
    of avoiding the project-level bigquery.jobs.create permission."""

    def test_ensure_managed_schema_escape_hatch_skips_create_when_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        client.get_dataset.return_value = MagicMock()
        _install_client(monkeypatch, client)

        BigQuerySource().ensure_managed_schema(_config())

        client.get_dataset.assert_called_once_with("my-proj._drt")
        client.create_dataset.assert_not_called()

    def test_ensure_managed_schema_creates_when_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        client.get_dataset.side_effect = _NotFound("nope")
        _install_client(monkeypatch, client)

        BigQuerySource().ensure_managed_schema(_config())

        client.create_dataset.assert_called_once()
        (dataset,), _ = client.create_dataset.call_args
        assert dataset.reference == "my-proj._drt"
        assert dataset.location == "US"

    def test_ensure_managed_schema_survives_concurrent_create_race(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        # First get_dataset (initial probe): absent. create_dataset: another
        # session won the race. Second get_dataset (re-probe): now present.
        client.get_dataset.side_effect = [_NotFound("nope"), MagicMock()]
        client.create_dataset.side_effect = _Conflict("already exists")
        _install_client(monkeypatch, client)

        BigQuerySource().ensure_managed_schema(_config())  # must not raise

        assert client.get_dataset.call_count == 2

    def test_ensure_managed_schema_reraises_when_create_fails_for_real(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        client.get_dataset.side_effect = _NotFound("nope")
        client.create_dataset.side_effect = RuntimeError("permission denied")
        _install_client(monkeypatch, client)

        with pytest.raises(RuntimeError, match="permission denied"):
            BigQuerySource().ensure_managed_schema(_config())

    def test_managed_table_exists_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = MagicMock()
        client.get_table.return_value = MagicMock(table_type="TABLE")
        _install_client(monkeypatch, client)

        assert BigQuerySource().managed_table_exists(_config(), "_drt_runs") is True
        client.get_table.assert_called_once_with("my-proj._drt._drt_runs")

    def test_managed_table_exists_false_whether_table_or_dataset_is_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unlike Databricks, get_table() raises the same NotFound whether
        the dataset or just the table is missing — no separate dataset probe
        needed first."""
        client = MagicMock()
        client.get_table.side_effect = _NotFound("nope")
        _install_client(monkeypatch, client)

        assert BigQuerySource().managed_table_exists(_config(), "_drt_runs") is False

    def test_managed_table_exists_false_for_a_same_named_view(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A view sharing this name isn't a managed table this capability
        owns — must not be reported as existing (Codex review)."""
        client = MagicMock()
        client.get_table.return_value = MagicMock(table_type="VIEW")
        _install_client(monkeypatch, client)

        assert BigQuerySource().managed_table_exists(_config(), "_drt_runs") is False

    def test_drop_managed_table_deletes_a_plain_table(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        client.get_table.return_value = MagicMock(table_type="TABLE")
        _install_client(monkeypatch, client)

        BigQuerySource().drop_managed_table(_config(), "_drt_runs")

        client.delete_table.assert_called_once_with("my-proj._drt._drt_runs", not_found_ok=True)

    def test_drop_managed_table_is_a_noop_when_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = MagicMock()
        client.get_table.side_effect = _NotFound("nope")
        _install_client(monkeypatch, client)

        BigQuerySource().drop_managed_table(_config(), "_drt_runs")  # must not raise

        client.delete_table.assert_not_called()

    def test_drop_managed_table_never_deletes_a_same_named_view(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Must not delete a resource this capability doesn't own just
        because the name matches (Codex review)."""
        client = MagicMock()
        client.get_table.return_value = MagicMock(table_type="VIEW")
        _install_client(monkeypatch, client)

        BigQuerySource().drop_managed_table(_config(), "_drt_runs")

        client.delete_table.assert_not_called()

    def test_managed_table_capable_protocol_satisfied(self) -> None:
        from drt.sources.base import ManagedTableCapable

        assert isinstance(BigQuerySource(), ManagedTableCapable)


def _table(
    columns: list[str] | None = None,
    *,
    labels: dict[str, str] | None = None,
    table_type: str = "TABLE",
) -> SimpleNamespace:
    return SimpleNamespace(
        schema=[SimpleNamespace(name=column) for column in (columns or [])],
        labels=labels,
        table_type=table_type,
    )


class TestSnapshotDiffSource:
    """#1113 — BigQuery snapshot-diff SQL and lifecycle contract.

    Mock jobs cannot validate GoogleSQL or copy-job atomicity; the gated DWH
    smoke test covers that real-warehouse boundary.
    """

    @staticmethod
    def _extract_with_columns(
        scratch_columns: list[str],
        *,
        baseline_columns: list[str] | None,
        key_columns: list[str] | None = None,
        hash_columns: Literal["all"] | list[str] = "all",
    ) -> tuple[Any, MagicMock, BigQuerySource]:
        client = _fake_client([])
        source = BigQuerySource()
        column_results = [scratch_columns]
        if baseline_columns is not None:
            column_results.append(baseline_columns)
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_build_client", return_value=client),
            patch.object(source, "_snapshot_query_job_config", return_value={}),
            patch.object(source, "_set_snapshot_token"),
            patch.object(source, "_managed_table_columns", side_effect=column_results),
            patch.object(
                source,
                "_managed_table_exists_with_client",
                return_value=baseline_columns is not None,
            ),
            patch.object(
                source, "_stream_query", side_effect=[iter(()), iter(()), iter(())]
            ) as stream,
        ):
            result = source.extract_snapshot_diff(
                "SELECT * FROM users",
                _config(),
                sync_name="users",
                key_columns=key_columns or ["id"],
                hash_columns=hash_columns,
            )
        return result, stream, source

    def test_first_run_materializes_every_row_as_added_with_atomic_job_config(
        self, mocked_bigquery: Any
    ) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_managed_table_columns", return_value=["id", "note"]),
            patch.object(source, "_managed_table_exists_with_client", return_value=False),
            patch.object(
                source, "_stream_query", return_value=iter([{"id": 1, "note": None}])
            ) as stream,
        ):
            result = source.extract_snapshot_diff(
                "SELECT id, note FROM users",
                _config(),
                sync_name="daily-users",
                key_columns=["id"],
                hash_columns="all",
                query_tags={"sync": "Daily Users"},
            )

        assert result.is_first_run is True
        assert list(result.added) == [{"id": 1, "note": None}]
        assert list(result.changed) == []
        assert list(result.removed_keys) == []
        _, kwargs = client.query.call_args
        assert kwargs["job_config"] == {
            "labels": {"sync": "daily-users"},
            "destination": "my-proj._drt._drt_snapshot_daily_users_4c0b66a8_scratch",
            "write_disposition": "WRITE_TRUNCATE",
        }
        token = client.update_table.call_args.args[0].labels["drt_snapshot_run"]
        assert len(token) == 36
        assert stream.call_args.kwargs["expected_token"] == token

    def test_transient_setup_failure_retries_with_stable_token(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(
                source,
                "_managed_table_columns",
                side_effect=[_Transient("backend"), ["id"]],
            ),
            patch.object(source, "_managed_table_exists_with_client", return_value=False),
            patch.object(source, "_stream_query", return_value=iter(())) as stream,
            patch("drt.destinations.retry.time.sleep"),
        ):
            source.extract_snapshot_diff(
                "SELECT id FROM users",
                _config(),
                sync_name="users",
                key_columns=["id"],
                hash_columns="all",
            )

        assert client.query.call_count == 2
        tokens = [
            call.args[0].labels["drt_snapshot_run"] for call in client.update_table.call_args_list
        ]
        assert len(set(tokens)) == 1
        assert stream.call_args.kwargs["expected_token"] == tokens[0]

    def test_non_transient_setup_failure_is_not_retried(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        client.query.side_effect = ValueError("bad SQL")
        source = BigQuerySource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch("drt.destinations.retry.time.sleep") as sleep,
            pytest.raises(ValueError, match="bad SQL"),
        ):
            source.extract_snapshot_diff(
                "not sql",
                _config(),
                sync_name="users",
                key_columns=["id"],
                hash_columns="all",
            )
        assert client.query.call_count == 1
        sleep.assert_not_called()

    def test_missing_key_and_hash_columns_fail_loudly(self) -> None:
        client = _fake_client([])
        source = BigQuerySource()
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_build_client", return_value=client),
            patch.object(source, "_snapshot_query_job_config", return_value={}),
            patch.object(source, "_set_snapshot_token"),
            patch.object(source, "_managed_table_columns", return_value=["id", "note"]),
            pytest.raises(ValueError, match=r"upsert_key.*\['ID'\]"),
        ):
            source.extract_snapshot_diff(
                "SELECT id, note FROM users",
                _config(),
                sync_name="users",
                key_columns=["ID"],
                hash_columns="all",
            )
        with (
            patch.object(source, "ensure_managed_schema"),
            patch.object(source, "_build_client", return_value=client),
            patch.object(source, "_snapshot_query_job_config", return_value={}),
            patch.object(source, "_set_snapshot_token"),
            patch.object(source, "_managed_table_columns", return_value=["id", "note"]),
            pytest.raises(ValueError, match=r"hash_columns.*\['ntoe'\].*typo"),
        ):
            source.extract_snapshot_diff(
                "SELECT id, note FROM users",
                _config(),
                sync_name="users",
                key_columns=["id"],
                hash_columns=["ntoe"],
            )

    def test_column_added_since_baseline_resends_every_existing_row(self) -> None:
        _, stream, _ = self._extract_with_columns(
            ["id", "note", "plan"], baseline_columns=["id", "note"]
        )
        changed_sql = stream.call_args_list[2].args[1]
        assert "FARM_FINGERPRINT" not in changed_sql
        assert " WHERE " not in changed_sql

    def test_column_dropped_hashes_only_shared_columns(self) -> None:
        _, stream, _ = self._extract_with_columns(
            ["id", "note"], baseline_columns=["id", "note", "old"]
        )
        changed_sql = stream.call_args_list[2].args[1]
        assert "old" not in changed_sql
        assert "s.`note` IS NULL AS `_drt_null_0`" in changed_sql
        assert "s.`note` AS `_drt_value_0`" in changed_sql

    def test_key_missing_from_baseline_restarts_as_first_run(self) -> None:
        result, stream, _ = self._extract_with_columns(["id", "note"], baseline_columns=["note"])
        assert result.is_first_run is True
        assert len(stream.call_args_list) == 1
        assert stream.call_args.args[1].startswith("SELECT * FROM")

    def test_hash_sql_is_typed_and_explicitly_distinguishes_null(self) -> None:
        _, stream, _ = self._extract_with_columns(
            ["id", "note"], baseline_columns=["id", "note"], hash_columns=["note"]
        )
        changed_sql = stream.call_args_list[2].args[1]
        assert "FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(" in changed_sql
        assert "s.`note` IS NULL AS `_drt_null_0`" in changed_sql
        assert "c.`note` IS NULL AS `_drt_null_0`" in changed_sql
        assert "CONCAT" not in changed_sql

    def test_diff_queries_are_backtick_quoted_and_case_sensitive(self) -> None:
        _, stream, _ = self._extract_with_columns(
            ["ID", "Note"],
            baseline_columns=["ID", "Note"],
            key_columns=["ID"],
            hash_columns=["Note"],
        )
        added_sql, removed_sql, changed_sql = [call.args[1] for call in stream.call_args_list]
        assert "s.`ID` = c.`ID`" in added_sql
        assert "SELECT c.`ID` FROM" in removed_sql
        assert "s.`Note` IS NULL" in changed_sql

    def test_no_non_key_columns_produces_no_changed_query(self) -> None:
        _, stream, _ = self._extract_with_columns(["id"], baseline_columns=["id"])
        assert len(stream.call_args_list) == 2

    def test_table_names_are_safe_distinct_and_case_preserving(self) -> None:
        source = BigQuerySource()
        assert source._snapshot_table_names("users") == (
            "_drt_snapshot_users_5b7dcd14",
            "_drt_snapshot_users_5b7dcd14_scratch",
        )
        assert source._snapshot_table_names("users") != source._snapshot_table_names("Users")
        assert source._snapshot_table_names("a.b/c")[0].startswith("_drt_snapshot_a_b_c_")
        base, scratch = source._snapshot_table_names("x" * 400)
        assert len(scratch) < 150
        assert source._snapshot_table_names("x" * 400 + "y")[0] != base

    @pytest.mark.parametrize(
        ("config", "table", "match"),
        [
            (_config(project="bad project"), "t", "project"),
            (_config(managed_schema="bad-dataset"), "t", "dataset"),
            (_config(), "bad-table", "table"),
        ],
    )
    def test_managed_identifiers_are_validated(
        self, config: BigQueryProfile, table: str, match: str
    ) -> None:
        from drt.sources.bigquery import _managed_table_id

        with pytest.raises(ValueError, match=match):
            _managed_table_id(config, table)

    @pytest.mark.parametrize("column", ["", "bad`name", "bad\nname"])
    def test_column_identifiers_reject_unsafe_text(self, column: str) -> None:
        from drt.sources.bigquery import _quote_column

        with pytest.raises(ValueError, match="column identifier"):
            _quote_column(column)

    def test_table_metadata_helpers_use_client_api(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        client.get_table.return_value = _table(["id", "note"], labels={"drt_snapshot_run": "ours"})
        source = BigQuerySource()

        assert source._managed_table_columns(client, _config(), "snap") == ["id", "note"]
        assert source._managed_table_exists_with_client(client, _config(), "snap") is True
        assert source._managed_table_token(client, _config(), "snap") == (True, "ours")
        source._set_snapshot_token(client, _config(), "snap", "new")
        assert client.update_table.call_args.args[0].labels["drt_snapshot_run"] == "new"
        assert client.update_table.call_args.args[1] == ["labels"]

    def test_table_metadata_helpers_handle_absent_and_non_table(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        client.get_table.side_effect = _NotFound("gone")
        assert source._managed_table_exists_with_client(client, _config(), "snap") is False
        assert source._managed_table_token(client, _config(), "snap") == (False, None)
        client.get_table.side_effect = None
        client.get_table.return_value = _table(table_type="VIEW")
        assert source._managed_table_exists_with_client(client, _config(), "snap") is False
        assert source._managed_table_token(client, _config(), "snap") == (False, None)

    def test_assert_snapshot_token_rejects_absent_or_foreign_table(self) -> None:
        source = BigQuerySource()
        with patch.object(source, "_managed_table_token", return_value=(True, "ours")):
            source._assert_snapshot_token(MagicMock(), _config(), "t", "ours", "users")
            with pytest.raises(RuntimeError, match="must not run concurrently"):
                source._assert_snapshot_token(MagicMock(), _config(), "t", "theirs", "users")
        with (
            patch.object(source, "_managed_table_token", return_value=(False, None)),
            pytest.raises(RuntimeError, match="must not run concurrently"),
        ):
            source._assert_snapshot_token(MagicMock(), _config(), "t", "ours", "users")

    def test_stream_checks_token_before_and_after_and_yields_dicts(
        self, mocked_bigquery: Any
    ) -> None:
        client = mocked_bigquery([{"id": 1, "note": "ok"}])
        source = BigQuerySource()
        with patch.object(source, "_assert_snapshot_token") as check:
            rows = list(
                source._stream_query(
                    _config(),
                    "SELECT * FROM scratch",
                    scratch_table="scratch",
                    expected_token="ours",
                    sync_name="users",
                    query_tags={"sync": "Users"},
                )
            )
        assert rows == [{"id": 1, "note": "ok"}]
        assert check.call_count == 2
        assert client.query.call_args.kwargs["job_config"] == {"labels": {"sync": "users"}}

    def test_stream_retries_transient_query_start(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        client.query.side_effect = [_Transient("backend"), client.query.return_value]
        source = BigQuerySource()
        with (
            patch.object(source, "_assert_snapshot_token"),
            patch("drt.destinations.retry.time.sleep"),
        ):
            assert (
                list(
                    source._stream_query(
                        _config(),
                        "SELECT 1",
                        scratch_table="scratch",
                        expected_token="ours",
                        sync_name="users",
                        query_tags=None,
                    )
                )
                == []
            )
        assert client.query.call_count == 2

    def test_commit_uses_atomic_write_truncate_copy_and_retains_recovery_tables(
        self, mocked_bigquery: Any
    ) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        key = source._snapshot_token_key(profile, "users")
        source._snapshot_diff_tokens[key] = "ours"
        with (
            patch.object(source, "_managed_table_token", return_value=(True, "ours")),
            patch.object(source, "_managed_table_exists_with_client", return_value=True),
            patch.object(source, "_assert_snapshot_token") as check,
            patch.object(source, "_set_snapshot_token") as set_token,
        ):
            source.commit_snapshot_diff(profile, "users")

        copies = client.copy_table.call_args_list
        assert len(copies) == 2
        assert copies[0].args[:2] == (
            "my-proj._drt._drt_snapshot_users_5b7dcd14",
            "my-proj._drt._drt_snapshot_users_5b7dcd14_old",
        )
        assert copies[1].args[:2] == (
            "my-proj._drt._drt_snapshot_users_5b7dcd14_scratch",
            "my-proj._drt._drt_snapshot_users_5b7dcd14",
        )
        assert all(
            call.kwargs["job_config"] == {"write_disposition": "WRITE_TRUNCATE"} for call in copies
        )
        assert check.call_count == 3
        set_token.assert_called_once_with(client, profile, "_drt_snapshot_users_5b7dcd14", "ours")
        client.delete_table.assert_not_called()
        assert key not in source._snapshot_diff_tokens

    def test_first_commit_skips_backup_copy(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        source._snapshot_diff_tokens[source._snapshot_token_key(profile, "users")] = "ours"
        with (
            patch.object(source, "_managed_table_token", return_value=(True, "ours")),
            patch.object(source, "_managed_table_exists_with_client", return_value=False),
            patch.object(source, "_assert_snapshot_token"),
            patch.object(source, "_set_snapshot_token"),
        ):
            source.commit_snapshot_diff(profile, "users")
        assert client.copy_table.call_count == 1

    def test_commit_without_extract_is_noop(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        BigQuerySource().commit_snapshot_diff(_config(), "users")
        client.copy_table.assert_not_called()

    def test_commit_after_already_promoted_is_idempotent(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        key = source._snapshot_token_key(profile, "users")
        source._snapshot_diff_tokens[key] = "ours"
        with patch.object(
            source,
            "_managed_table_token",
            side_effect=[(False, None), (True, "ours")],
        ):
            source.commit_snapshot_diff(profile, "users")
        client.copy_table.assert_not_called()
        assert key not in source._snapshot_diff_tokens

    @pytest.mark.parametrize(
        "tokens",
        [[(False, None), (True, "theirs")], [(True, "theirs")]],
    )
    def test_commit_rejects_missing_or_foreign_scratch(
        self, mocked_bigquery: Any, tokens: list[tuple[bool, str | None]]
    ) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        source._snapshot_diff_tokens[source._snapshot_token_key(profile, "users")] = "ours"
        with (
            patch.object(source, "_managed_table_token", side_effect=tokens),
            pytest.raises(RuntimeError, match="must not run concurrently"),
        ):
            source.commit_snapshot_diff(profile, "users")
        client.copy_table.assert_not_called()

    def test_post_copy_race_restores_previous_baseline(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        source._snapshot_diff_tokens[source._snapshot_token_key(profile, "users")] = "ours"
        with (
            patch.object(source, "_managed_table_token", return_value=(True, "ours")),
            patch.object(source, "_managed_table_exists_with_client", return_value=True),
            patch.object(
                source,
                "_assert_snapshot_token",
                side_effect=RuntimeError("must not run concurrently"),
            ),
            pytest.raises(RuntimeError, match="must not run concurrently"),
        ):
            source.commit_snapshot_diff(profile, "users")
        assert client.copy_table.call_args_list[-1].args[:2] == (
            "my-proj._drt._drt_snapshot_users_5b7dcd14_old",
            "my-proj._drt._drt_snapshot_users_5b7dcd14",
        )

    def test_first_commit_race_deletes_untrusted_baseline(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        source = BigQuerySource()
        profile = _config()
        source._snapshot_diff_tokens[source._snapshot_token_key(profile, "users")] = "ours"
        with (
            patch.object(source, "_managed_table_token", return_value=(True, "ours")),
            patch.object(source, "_managed_table_exists_with_client", return_value=False),
            patch.object(
                source,
                "_assert_snapshot_token",
                side_effect=RuntimeError("must not run concurrently"),
            ),
            pytest.raises(RuntimeError, match="must not run concurrently"),
        ):
            source.commit_snapshot_diff(profile, "users")
        client.delete_table.assert_called_once_with(
            "my-proj._drt._drt_snapshot_users_5b7dcd14", not_found_ok=True
        )

    def test_failed_restore_does_not_mask_race(self, mocked_bigquery: Any) -> None:
        client = mocked_bigquery([])
        client.copy_table.side_effect = [MagicMock(), MagicMock(), OSError("copy failed")]
        source = BigQuerySource()
        profile = _config()
        source._snapshot_diff_tokens[source._snapshot_token_key(profile, "users")] = "ours"
        with (
            patch.object(source, "_managed_table_token", return_value=(True, "ours")),
            patch.object(source, "_managed_table_exists_with_client", return_value=True),
            patch.object(
                source,
                "_assert_snapshot_token",
                side_effect=RuntimeError("must not run concurrently"),
            ),
            pytest.raises(RuntimeError, match="must not run concurrently"),
        ):
            source.commit_snapshot_diff(profile, "users")

    def test_snapshot_diff_source_protocol_satisfied(self) -> None:
        from drt.sources.base import SnapshotDiffSource

        assert isinstance(BigQuerySource(), SnapshotDiffSource)
