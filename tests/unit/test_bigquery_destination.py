"""Unit tests for the BigQuery destination.

Uses ``sys.modules`` injection to mock ``google.cloud.bigquery`` /
``google.oauth2.service_account`` — no real GCP project or
``google-cloud-bigquery`` install required (matches the pattern in
test_snowflake_destination.py / test_databricks_destination.py).

The MERGE / auth test shapes are adapted from @PFCAaron12's original #584
contribution.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import ValidationError

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import BigQueryDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import ConnectionTestable
from drt.destinations.bigquery import BigQueryDestination
from drt.engine.sync import run_sync
from drt.sources.base import SnapshotDiffResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _NotFound(Exception):
    """Stand-in for google.api_core.exceptions.NotFound."""


@pytest.fixture(autouse=True)
def _stable_scratch_run_id() -> Iterator[None]:
    """Keep scratch-table assertions deterministic without importing GCP."""
    token = MagicMock()
    token.hex = "a1b2c3d4ffffffffffffffffffffffff"
    with patch("drt.destinations.bigquery.uuid4", return_value=token):
        yield


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> BigQueryDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "bigquery",
        "project": "my-proj",
        "dataset": "analytics",
        "table": "user_scores",
    }
    defaults.update(overrides)
    return BigQueryDestinationConfig.model_validate(defaults)


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.insert_rows_json.return_value = []  # no per-row errors by default
    client.load_table_from_json.return_value = MagicMock()
    client.query.return_value = MagicMock()
    client.copy_table.return_value = MagicMock()
    client.get_table.return_value.schema = []
    return client


def _mocked_bq_modules(
    client: MagicMock | None = None, creds: Any = "fake-creds"
) -> dict[str, Any]:
    """sys.modules entries satisfying `from google.cloud import bigquery` etc."""
    bigquery_mod = MagicMock()
    if client is not None:
        bigquery_mod.Client.return_value = client

    sa_mod = MagicMock()
    sa_mod.Credentials.from_service_account_file.return_value = creds

    cloud = MagicMock()
    cloud.bigquery = bigquery_mod
    oauth2 = MagicMock()
    oauth2.service_account = sa_mod
    api_core_exceptions = MagicMock()
    api_core_exceptions.NotFound = _NotFound
    api_core = MagicMock()
    api_core.exceptions = api_core_exceptions
    google = MagicMock()
    google.cloud = cloud
    google.oauth2 = oauth2
    google.api_core = api_core

    return {
        "google": google,
        "google.api_core": api_core,
        "google.api_core.exceptions": api_core_exceptions,
        "google.cloud": cloud,
        "google.cloud.bigquery": bigquery_mod,
        "google.oauth2": oauth2,
        "google.oauth2.service_account": sa_mod,
    }


def _schema_field(name: str) -> MagicMock:
    field = MagicMock()
    field.name = name
    return field


def _sqls(client: MagicMock) -> list[str]:
    return [(c.args[0] if c.args else "") for c in client.query.call_args_list]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestBigQueryDestinationConfig:
    def test_valid_config(self) -> None:
        c = _config()
        assert c.project == "my-proj"
        assert c.dataset == "analytics"
        assert c.table == "user_scores"
        assert c.mode == "insert"
        assert c.method == "application_default"

    def test_describe(self) -> None:
        assert _config().describe() == "bigquery (my-proj.analytics.user_scores)"

    @pytest.mark.parametrize("mode", ["replace", "mirror"])
    def test_advanced_modes_belong_to_sync_options_not_destination_mode(self, mode: str) -> None:
        with pytest.raises(ValidationError):
            _config(mode=mode)
        assert _options(mode=mode).mode == mode


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


class TestBigQueryDestinationLoad:
    def test_empty_records_short_circuits_before_import(self) -> None:
        # No sys.modules patch; reaching _build_client would raise.
        result = BigQueryDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_import_error_when_extras_missing(self) -> None:
        # Build config/options BEFORE patching __import__ — pydantic may
        # lazily finish a deferred validator on first model_validate, and
        # under a global import patch that surfaces as a bare ImportError
        # instead of the connector-extra message under test.
        config = _config()
        options = _options()
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="drt-core\\[bigquery\\]"):
                BigQueryDestination().load([{"id": 1}], config, options)

    def test_client_init_adc(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load([{"id": 1}], _config(), _options())
        modules["google.cloud.bigquery"].Client.assert_called_once_with(
            project="my-proj", location=None
        )

    def test_client_init_keyfile(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client, creds="sa-creds")
        config = _config(method="keyfile", keyfile="/keys/sa.json")
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load([{"id": 1}], config, _options())
        sa = modules["google.oauth2.service_account"]
        sa.Credentials.from_service_account_file.assert_called_once()
        modules["google.cloud.bigquery"].Client.assert_called_once_with(
            project="my-proj", credentials="sa-creds", location=None
        )

    def test_keyfile_required_when_method_keyfile(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config(method="keyfile")  # no keyfile
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="keyfile is required"):
                BigQueryDestination().load([{"id": 1}], config, _options())

    def test_insert_success(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.80}]
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(records, _config(), _options())
        assert result.success == 2
        assert result.failed == 0
        client.insert_rows_json.assert_called_once_with("my-proj.analytics.user_scores", records)

    def test_insert_preserves_hyphenated_table_and_partition_decorator(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config(project="hyphenated-project", table="daily-events$20261004")
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load([{"id": 1}], config, _options())
        client.insert_rows_json.assert_called_once_with(
            "hyphenated-project.analytics.daily-events$20261004", [{"id": 1}]
        )

    def test_insert_per_row_error_on_error_skip(self) -> None:
        client = _fake_client()
        client.insert_rows_json.return_value = [{"index": 0, "errors": [{"r": "bad"}]}]
        modules = _mocked_bq_modules(client)
        records = [{"id": 1}, {"id": 2}]
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(records, _config(), _options(on_error="skip"))
        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert result.row_errors[0].batch_index == 0

    def test_insert_error_on_error_fail_raises(self) -> None:
        client = _fake_client()
        client.insert_rows_json.return_value = [{"index": 0, "errors": [{"r": "bad"}]}]
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="BigQuery insert failed"):
                BigQueryDestination().load([{"id": 1}], _config(), _options(on_error="fail"))

    def test_insert_error_without_index_fails_whole_batch(self) -> None:
        client = _fake_client()
        client.insert_rows_json.return_value = [{"errors": [{"r": "schema"}]}]  # no index
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1}, {"id": 2}], _config(), _options(on_error="skip")
            )
        assert result.failed == 2
        assert result.success == 0

    def test_merge_query_tags_become_labels_on_both_jobs(self) -> None:
        """#768 — the load-into-tmp-table and the MERGE are both jobs, so
        both get `labels`; `_insert`'s streaming API has no job to label
        (see test_insert_ignores_query_tags_no_job_to_label below)."""
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id"])
        options = _options()
        options._query_tags = {"sync": "s", "run_id": "r"}
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(records, config, options)

        assert result.success == 1
        bq_mod = modules["google.cloud.bigquery"]
        bq_mod.LoadJobConfig.assert_called_once_with(
            labels={"sync": "s", "run_id": "r"},
            write_disposition=bq_mod.WriteDisposition.WRITE_TRUNCATE,
        )
        bq_mod.QueryJobConfig.assert_called_once_with(labels={"sync": "s", "run_id": "r"})

    def test_insert_ignores_query_tags_no_job_to_label(self) -> None:
        """insert_rows_json is a streaming-insert REST call, not a job —
        BigQuery labels are job-scoped, so there's nothing to attach to."""
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        options = _options()
        options._query_tags = {"sync": "s", "run_id": "r"}
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load([{"id": 1}], _config(), options)

        client.insert_rows_json.assert_called_once_with(
            "my-proj.analytics.user_scores", [{"id": 1}]
        )

    def test_merge_success(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(records, config, _options())
        assert result.success == 1
        client.load_table_from_json.assert_called_once()
        assert client.load_table_from_json.call_args.args == (
            records,
            "my-proj.analytics.user_scores_drt_tmp_a1b2c3d4",
        )
        merge = next(s for s in _sqls(client) if "MERGE" in s)
        assert "MERGE `my-proj.analytics.user_scores` T" in merge
        assert "USING `my-proj.analytics.user_scores_drt_tmp_a1b2c3d4` S" in merge
        assert "ON T.`id` = S.`id`" in merge
        assert "WHEN MATCHED THEN UPDATE SET `score` = S.`score`" in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True
        )

    def test_merge_temp_load_uses_target_schema_in_each_run_column_order(self) -> None:
        client = _fake_client()
        id_field = _schema_field("id")
        score_field = _schema_field("score")
        other_field = _schema_field("other")
        client.get_table.return_value.schema = [score_field, other_field, id_field]
        modules = _mocked_bq_modules(client)
        records = [{"id": 1, "score": None}, {"id": 2, "score": None, "other": 3}]

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records,
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )

        assert result.success == 2
        client.get_table.assert_called_once_with("my-proj.analytics.user_scores")
        bq = modules["google.cloud.bigquery"]
        assert bq.LoadJobConfig.call_args_list == [
            call(
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                schema=[id_field, score_field],
                autodetect=False,
            ),
            call(
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                schema=[id_field, score_field, other_field],
                autodetect=False,
            ),
        ]

    def test_sparse_merge_writes_later_field_and_distinguishes_none_from_omitted(self) -> None:
        client = _fake_client()
        fields = {name: _schema_field(name) for name in ("id", "name", "note")}
        client.get_table.return_value.schema = list(fields.values())
        modules = _mocked_bq_modules(client)
        records = [
            {"id": 1, "name": "omitted"},
            {"id": 2, "name": "later", "note": "written"},
            {"id": 3, "name": "explicit-null", "note": None},
        ]
        options = _options()
        options._query_tags = {"sync": "sparse", "run_id": "r"}

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records,
                _config(mode="merge", upsert_key=["id"]),
                options,
            )

        assert result.success == 3
        assert [c.args[0] for c in client.load_table_from_json.call_args_list] == [
            [records[0]],
            records[1:],
        ]
        merge_sqls = [sql for sql in _sqls(client) if sql.startswith("MERGE")]
        assert len(merge_sqls) == 2
        assert "`note`" not in merge_sqls[0]
        assert "`note` = S.`note`" in merge_sqls[1]
        assert "INSERT (`id`, `name`, `note`)" in merge_sqls[1]
        # Explicit None stays present in the staged payload; omission alone
        # starts a different signature run and excludes note from its SQL.
        assert "note" in client.load_table_from_json.call_args_list[1].args[0][1]
        assert client.load_table_from_json.call_args_list[1].args[0][1]["note"] is None
        bq = modules["google.cloud.bigquery"]
        assert all(
            c.kwargs["labels"] == {"sync": "sparse", "run_id": "r"}
            for c in bq.LoadJobConfig.call_args_list
        )
        assert bq.QueryJobConfig.call_args_list == [
            call(labels={"sync": "sparse", "run_id": "r"}),
            call(labels={"sync": "sparse", "run_id": "r"}),
        ]

    def test_sparse_merge_preserves_order_when_signatures_alternate(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [
            {"id": 1, "score": 1},
            {"id": 1, "note": "middle"},
            {"id": 1, "score": 3},
        ]

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records,
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )

        assert result.success == 3
        assert [c.args[0] for c in client.load_table_from_json.call_args_list] == [
            [records[0]],
            [records[1]],
            [records[2]],
        ]
        merge_sqls = [sql for sql in _sqls(client) if sql.startswith("MERGE")]
        assert ["`score`" in sql for sql in merge_sqls] == [True, False, True]
        assert ["`note`" in sql for sql in merge_sqls] == [False, True, False]

    def test_sparse_merge_skip_reports_failed_run_and_continues(self) -> None:
        client = _fake_client()
        failed = MagicMock()
        failed.result.side_effect = RuntimeError("middle run failed")
        client.query.side_effect = [MagicMock(), failed, MagicMock()]
        modules = _mocked_bq_modules(client)
        records = [{"id": 1}, {"id": 2, "note": "bad"}, {"id": 3}]

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records,
                _config(mode="merge", upsert_key=["id"]),
                _options(on_error="skip"),
            )

        assert result.success == 2
        assert result.failed == 1
        assert [error.batch_index for error in result.row_errors] == [1]
        assert result.row_errors[0].error_message == "middle run failed"
        assert client.query.call_count == 3

    def test_sparse_merge_fail_stops_after_failed_run_but_prior_run_is_committed(self) -> None:
        client = _fake_client()
        failed = MagicMock()
        failed.result.side_effect = RuntimeError("middle run failed")
        client.query.side_effect = [MagicMock(), failed]
        modules = _mocked_bq_modules(client)
        records = [{"id": 1}, {"id": 2, "note": "bad"}, {"id": 3}]

        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="middle run failed"):
                BigQueryDestination().load(
                    records,
                    _config(mode="merge", upsert_key=["id"]),
                    _options(on_error="fail"),
                )

        # BigQuery jobs autocommit: the first MERGE cannot be rolled back, and
        # fail policy prevents the third signature run from being attempted.
        assert client.query.call_count == 2
        assert [c.args[0] for c in client.load_table_from_json.call_args_list] == [
            [records[0]],
            [records[1]],
        ]

    def test_merge_temp_load_keeps_autodetect_when_target_column_is_absent(self) -> None:
        client = _fake_client()
        client.get_table.return_value.schema = [_schema_field("id")]
        modules = _mocked_bq_modules(client)

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1, "new_column": "value"}],
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )

        assert result.success == 1
        bq = modules["google.cloud.bigquery"]
        bq.LoadJobConfig.assert_called_once_with(
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE
        )

    def test_merge_temp_load_keeps_autodetect_when_target_does_not_exist(self) -> None:
        client = _fake_client()
        client.get_table.side_effect = _NotFound("absent")
        modules = _mocked_bq_modules(client)

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1}],
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )

        assert result.success == 1
        bq = modules["google.cloud.bigquery"]
        bq.LoadJobConfig.assert_called_once_with(
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE
        )

    def test_merge_target_schema_error_keeps_batch_row_errors(self) -> None:
        client = _fake_client()
        client.get_table.side_effect = RuntimeError("schema lookup failed")
        modules = _mocked_bq_modules(client)
        records = [{"id": 1}, {"id": 2}]

        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records,
                _config(mode="merge", upsert_key=["id"]),
                _options(on_error="skip"),
            )

        assert result.success == 0
        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert {error.error_message for error in result.row_errors} == {"schema lookup failed"}
        client.load_table_from_json.assert_not_called()
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True
        )

    def test_merge_preserves_extended_identifiers(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [{"識別子": 1, "顧客名": "Alice"}]
        config = _config(
            project="hyphenated-project",
            table="daily-events$20261004",
            mode="merge",
            upsert_key=["識別子"],
        )
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load(records, config, _options())
        assert client.load_table_from_json.call_args.args == (
            records,
            "hyphenated-project.analytics.daily-events$20261004_drt_tmp_a1b2c3d4",
        )
        merge = next(sql for sql in _sqls(client) if sql.startswith("MERGE"))
        assert "MERGE `hyphenated-project.analytics.daily-events$20261004` T" in merge
        assert "ON T.`識別子` = S.`識別子`" in merge
        assert "UPDATE SET `顧客名` = S.`顧客名`" in merge

    def test_unsupported_mode_raises(self) -> None:
        # Defensive branch — `mode` is a Literal, so reach it by bypassing validation.
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config()
        config.mode = "bogus"  # type: ignore[assignment]
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="Unsupported mode"):
                BigQueryDestination().load([{"id": 1}], config, _options())

    def test_merge_requires_upsert_key(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config(mode="merge")  # no upsert_key
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="upsert_key is required"):
                BigQueryDestination().load([{"id": 1}], config, _options())

    def test_merge_requires_upsert_key_on_every_record(self) -> None:
        with pytest.raises(ValueError, match="upsert_key columns missing"):
            BigQueryDestination().load(
                [{"id": 1}, {"score": 2}],
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )

    def test_empty_record_and_unsafe_sql_identifier_fail_before_client(self) -> None:
        with pytest.raises(ValueError, match="have no fields"):
            BigQueryDestination().load([{}], _config(), _options())
        with pytest.raises(ValueError, match="column identifier"):
            BigQueryDestination().load(
                [{"id": 1, "bad`name": 2}],
                _config(mode="merge", upsert_key=["id"]),
                _options(),
            )
        with pytest.raises(ValueError, match="project identifier"):
            BigQueryDestination().load([{"id": 1}], _config(project="bad`project"), _options())
        with pytest.raises(ValueError, match="table identifier"):
            BigQueryDestination().load([{"id": 1}], _config(table="bad\\table"), _options())
        with pytest.raises(ValueError, match="dataset identifier"):
            BigQueryDestination().load([{"id": 1}], _config(dataset="bad\ndataset"), _options())

    def test_merge_all_columns_are_key_skips_update(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config(mode="merge", upsert_key=["id", "score"])
        with patch.dict("sys.modules", modules):
            BigQueryDestination().load([{"id": 1, "score": 0.9}], config, _options())
        merge = next(s for s in _sqls(client) if "MERGE" in s)
        assert "WHEN MATCHED THEN UPDATE" not in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge

    def test_merge_error_on_error_fail_still_cleans_up(self) -> None:
        client = _fake_client()
        client.query.return_value.result.side_effect = Exception("merge boom")
        modules = _mocked_bq_modules(client)
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="merge boom"):
                BigQueryDestination().load([{"id": 1}], config, _options(on_error="fail"))
        # temp table dropped even on failure (finally)
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True
        )

    def test_merge_error_on_error_skip_records_failure(self) -> None:
        client = _fake_client()
        client.query.return_value.result.side_effect = Exception("merge boom")
        modules = _mocked_bq_modules(client)
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1}, {"id": 2}], config, _options(on_error="skip")
            )
        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert [error.record_preview for error in result.row_errors] == ["{'id': 1}", "{'id': 2}"]


# ---------------------------------------------------------------------------
# sync.mode: replace (#1055)
# ---------------------------------------------------------------------------


class TestBigQueryReplaceMode:
    def test_replace_ignores_physical_merge_mode_and_does_not_require_key(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1}],
                _config(mode="merge"),
                _options(mode="replace"),
            )
        assert result.success == 1
        client.query.assert_not_called()

    def test_partition_decorator_allows_truncate_but_rejects_swap_before_client(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        config = _config(table="daily-events$20261004")
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                [{"id": 1}],
                config,
                _options(mode="replace", replace_strategy="truncate"),
            )
        assert result.success == 1
        assert client.load_table_from_json.call_args.args[1] == (
            "my-proj.analytics.daily-events$20261004"
        )

        dest = BigQueryDestination()
        opts = _options(mode="replace", replace_strategy="swap")
        with patch.object(dest, "_build_client") as build_client:
            with pytest.raises(ValueError, match=r"swap.*partition-decorated.*truncate"):
                dest.load([{"id": 1}], config, opts)
            with pytest.raises(ValueError, match=r"swap.*partition-decorated.*truncate"):
                dest.finalize_sync(config, opts)
        build_client.assert_not_called()

    def test_truncate_uses_load_job_then_appends_and_resets_at_finalize(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        opts = _options(mode="replace")
        with patch.dict("sys.modules", modules):
            assert dest.load([{"id": 1}], _config(), opts).success == 1
            assert dest.load([{"id": 2}], _config(), opts).success == 1
            assert dest.finalize_sync(_config(), opts) is None
            assert dest.load([{"id": 3}], _config(), opts).success == 1

        bq = modules["google.cloud.bigquery"]
        assert [c.kwargs["write_disposition"] for c in bq.LoadJobConfig.call_args_list] == [
            bq.WriteDisposition.WRITE_TRUNCATE,
            bq.WriteDisposition.WRITE_APPEND,
            bq.WriteDisposition.WRITE_TRUNCATE,
        ]
        assert {c.args[1] for c in client.load_table_from_json.call_args_list} == {
            "my-proj.analytics.user_scores"
        }
        client.query.assert_not_called()

    def test_truncate_load_error_reports_batch_and_fail_resets_state(self) -> None:
        client = _fake_client()
        client.load_table_from_json.return_value.result.side_effect = RuntimeError("load boom")
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        with patch.dict("sys.modules", modules):
            skipped = dest.load(
                [{"id": 1}, {"id": 2}],
                _config(),
                _options(mode="replace", on_error="skip"),
            )
            assert skipped.failed == 2
            assert [error.batch_index for error in skipped.row_errors] == [0, 1]
            assert {error.error_message for error in skipped.row_errors} == {"load boom"}
            with pytest.raises(RuntimeError, match="load boom"):
                dest.load(
                    [{"id": 3}],
                    _config(),
                    _options(mode="replace", on_error="fail"),
                )
        assert dest._replace_started is False

    def test_swap_loads_shadow_appends_then_copies_atomically_and_cleans_up(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        opts = _options(mode="replace", replace_strategy="swap")
        opts._query_tags = {"sync": "replace"}
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), opts)
            dest.load([{"id": 2}], _config(), opts)
            result = dest.finalize_sync(_config(), opts)

        assert result is not None
        assert [c.args[1] for c in client.load_table_from_json.call_args_list] == [
            "my-proj.analytics.user_scores__drt_swap_a1b2c3d4",
            "my-proj.analytics.user_scores__drt_swap_a1b2c3d4",
        ]
        assert [c.args for c in client.copy_table.call_args_list] == [
            (
                "my-proj.analytics.user_scores",
                "my-proj.analytics.user_scores__drt_swap_a1b2c3d4",
            ),
            (
                "my-proj.analytics.user_scores__drt_swap_a1b2c3d4",
                "my-proj.analytics.user_scores",
            ),
        ]
        assert any(
            sql == "TRUNCATE TABLE `my-proj.analytics.user_scores__drt_swap_a1b2c3d4`"
            for sql in _sqls(client)
        )
        bq = modules["google.cloud.bigquery"]
        assert bq.CopyJobConfig.call_args_list == [
            call(
                labels={"sync": "replace"},
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
            ),
            call(
                labels={"sync": "replace"},
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
            ),
        ]
        client.delete_table.assert_called_with(
            "my-proj.analytics.user_scores__drt_swap_a1b2c3d4", not_found_ok=True
        )
        assert dest._swap_shadow_created is False

    def test_swap_copy_failure_still_drops_shadow_and_resets(self) -> None:
        client = _fake_client()
        prepared = MagicMock()
        failed_copy = MagicMock()
        failed_copy.result.side_effect = RuntimeError("copy boom")
        client.copy_table.side_effect = [prepared, failed_copy]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        opts = _options(mode="replace", replace_strategy="swap")
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), opts)
            with pytest.raises(RuntimeError, match="copy boom"):
                dest.finalize_sync(_config(), opts)
        client.delete_table.assert_called_with(
            "my-proj.analytics.user_scores__drt_swap_a1b2c3d4", not_found_ok=True
        )
        assert dest._swap_table_id is None

    def test_swap_load_fail_cleans_shadow_only_for_fail_policy(self) -> None:
        client = _fake_client()
        client.load_table_from_json.return_value.result.side_effect = RuntimeError("load boom")
        modules = _mocked_bq_modules(client)
        opts = _options(mode="replace", replace_strategy="swap", on_error="fail")
        dest = BigQueryDestination()
        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="load boom"):
                dest.load([{"id": 1}], _config(), opts)
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores__drt_swap_a1b2c3d4", not_found_ok=True
        )
        assert dest._swap_shadow_created is False

    def test_finalize_is_noop_without_matching_swap_state(self) -> None:
        dest = BigQueryDestination()
        assert dest.finalize_sync(_config(), _options()) is None
        assert (
            dest.finalize_sync(_config(), _options(mode="replace", replace_strategy="swap")) is None
        )
        dest._swap_shadow_created = True
        assert (
            dest.finalize_sync(_config(), _options(mode="replace", replace_strategy="swap")) is None
        )

    def test_reset_write_state_drops_staging_and_clears_all_state(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        dest._replace_started = True
        dest._swap_shadow_created = True
        dest._swap_table_id = "my-proj.analytics.user_scores"
        dest._mirror_keys_table_id = "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"
        dest._mirror_aborted = True

        with patch.dict("sys.modules", modules):
            dest.reset_write_state(_config(), _options(mode="replace"))

        client.delete_table.assert_has_calls(
            [
                call("my-proj.analytics.user_scores__drt_swap_a1b2c3d4", not_found_ok=True),
                call(
                    "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4",
                    not_found_ok=True,
                ),
            ]
        )
        assert dest._replace_started is False
        assert dest._swap_shadow_created is False
        assert dest._swap_table_id is None
        assert dest._mirror_keys_table_id is None
        assert dest._mirror_aborted is False

    def test_reset_write_state_attempts_all_cleanup_before_reporting_failure(self) -> None:
        client = _fake_client()
        client.delete_table.side_effect = [
            RuntimeError("drop swap"),
            RuntimeError("drop mirror"),
        ]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        dest._swap_shadow_created = True
        dest._swap_table_id = "my-proj.analytics.user_scores"
        dest._mirror_keys_table_id = "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"

        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="drop swap"):
                dest.reset_write_state(_config(), _options())

        assert client.delete_table.call_count == 2
        assert dest._swap_table_id is None
        assert dest._mirror_keys_table_id is None

    def test_reset_write_state_clears_state_when_client_creation_fails(self) -> None:
        dest = BigQueryDestination()
        dest._replace_started = True
        dest._swap_shadow_created = True
        dest._swap_table_id = "my-proj.analytics.user_scores"
        with patch.object(dest, "_build_client", side_effect=RuntimeError("auth boom")):
            with pytest.raises(RuntimeError, match="auth boom"):
                dest.reset_write_state(_config(), _options())
        assert dest._replace_started is False
        assert dest._swap_shadow_created is False
        assert dest._swap_table_id is None

    def test_reset_write_state_is_safe_before_any_batch(self) -> None:
        dest = BigQueryDestination()
        with patch.object(dest, "_build_client") as build:
            dest.reset_write_state(_config(), _options())
        build.assert_not_called()

    def test_scratch_ids_are_unique_per_instance_and_rotate_on_reset(self) -> None:
        tokens = []
        for value in ("11111111", "22222222", "33333333"):
            token = MagicMock()
            token.hex = f"{value}ffffffffffffffffffffffff"
            tokens.append(token)

        with patch("drt.destinations.bigquery.uuid4", side_effect=tokens):
            first = BigQueryDestination()
            second = BigQueryDestination()
            assert first._scratch_table_id("p.d.t", "__drt_swap") == ("p.d.t__drt_swap_11111111")
            assert second._scratch_table_id("p.d.t", "__drt_swap") == ("p.d.t__drt_swap_22222222")
            first.reset_write_state(_config(), _options())
            assert first._scratch_table_id("p.d.t", "__drt_swap") == ("p.d.t__drt_swap_33333333")


# ---------------------------------------------------------------------------
# sync.mode: mirror (#1055)
# ---------------------------------------------------------------------------


class TestBigQueryMirrorMode:
    def test_sparse_batch_stages_all_mirror_keys_once_before_per_run_merges(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(mode="merge", upsert_key=["id"])
        options = _options(mode="mirror")
        records = [
            {"id": 1, "name": "omitted"},
            {"id": 2, "name": "later", "note": "written"},
            {"id": 3, "name": "explicit-null", "note": None},
        ]

        with patch.dict("sys.modules", modules):
            result = dest.load(records, config, options)

        assert result.success == 3
        key_table = "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"
        key_loads = [
            c for c in client.load_table_from_json.call_args_list if c.args[1] == key_table
        ]
        assert len(key_loads) == 1
        assert key_loads[0].args[0] == [{"id": 1}, {"id": 2}, {"id": 3}]
        tmp_table = "my-proj.analytics.user_scores_drt_tmp_a1b2c3d4"
        tmp_loads = [
            c for c in client.load_table_from_json.call_args_list if c.args[1] == tmp_table
        ]
        assert [c.args[0] for c in tmp_loads] == [[records[0]], records[1:]]
        sqls = _sqls(client)
        assert sqls[0].startswith("CREATE OR REPLACE TABLE")
        assert len([sql for sql in sqls if sql.startswith("MERGE")]) == 2

    def test_stages_keys_across_batches_then_deletes_by_anti_join(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(mode="insert", upsert_key=["tenant_id", "id"])
        opts = _options(mode="mirror")
        with patch.dict("sys.modules", modules):
            assert dest.load([{"tenant_id": 1, "id": 1, "name": "a"}], config, opts).success == 1
            assert dest.load([{"tenant_id": 1, "id": 2, "name": "b"}], config, opts).success == 1
            final = dest.finalize_sync(config, opts)

        assert final is not None
        sqls = _sqls(client)
        assert any(
            "CREATE OR REPLACE TABLE `my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4`"
            in sql
            and "SELECT `tenant_id`, `id` FROM `my-proj.analytics.user_scores` WHERE FALSE" in sql
            for sql in sqls
        )
        key_loads = [
            c
            for c in client.load_table_from_json.call_args_list
            if c.args[1] == "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"
        ]
        assert [c.args[0] for c in key_loads] == [
            [{"tenant_id": 1, "id": 1}],
            [{"tenant_id": 1, "id": 2}],
        ]
        delete = next(sql for sql in sqls if sql.startswith("DELETE FROM"))
        assert "NOT EXISTS" in delete
        assert "T.`tenant_id` = K.`tenant_id` AND T.`id` = K.`id`" in delete
        assert " IN (" not in delete
        client.delete_table.assert_has_calls(
            [
                call("my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True),
                call("my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True),
                call(
                    "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4",
                    not_found_ok=True,
                ),
            ]
        )
        assert dest._mirror_keys_table_id is None

    def test_scope_restricts_delete_and_is_staged_once_when_also_a_key(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(mode="merge", upsert_key=["parent_id", "id"])
        opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})
        with patch.dict("sys.modules", modules):
            dest.load([{"parent_id": 1, "id": 2}], config, opts)
            dest.finalize_sync(config, opts)
        stage = next(sql for sql in _sqls(client) if sql.startswith("CREATE OR REPLACE"))
        assert "SELECT `parent_id`, `id` FROM `my-proj.analytics.user_scores` WHERE FALSE" in stage
        delete = next(sql for sql in _sqls(client) if sql.startswith("DELETE FROM"))
        assert "EXISTS" in delete
        assert "TO_JSON_STRING(T.`parent_id`) = TO_JSON_STRING(K.`parent_id`)" in delete

    def test_null_scope_uses_null_safe_match(self) -> None:
        client = _fake_client()
        id_field = _schema_field("id")
        parent_id_field = _schema_field("parent_id")
        client.get_table.return_value.schema = [parent_id_field, id_field]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(mode="merge", upsert_key=["id"])
        opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1, "parent_id": None}], config, opts)
            dest.finalize_sync(config, opts)
        delete = next(sql for sql in _sqls(client) if sql.startswith("DELETE FROM"))
        assert "TO_JSON_STRING(T.`parent_id`) = TO_JSON_STRING(K.`parent_id`)" in delete
        client.get_table.assert_called_once_with("my-proj.analytics.user_scores")
        bq = modules["google.cloud.bigquery"]
        assert bq.LoadJobConfig.call_args_list == [
            call(
                write_disposition=bq.WriteDisposition.WRITE_APPEND,
                schema=[id_field, parent_id_field],
                autodetect=False,
            ),
            call(
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                schema=[id_field, parent_id_field],
                autodetect=False,
            ),
        ]

    def test_nullable_scope_schema_comes_from_target_across_batches(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(mode="merge", upsert_key=["id"])
        opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})

        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1, "parent_id": None}], config, opts)
            dest.load([{"id": 2, "parent_id": 42}], config, opts)

        create = [sql for sql in _sqls(client) if sql.startswith("CREATE OR REPLACE")]
        assert create == [
            "CREATE OR REPLACE TABLE "
            "`my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4` AS "
            "SELECT `id`, `parent_id` FROM `my-proj.analytics.user_scores` WHERE FALSE"
        ]
        key_loads = [
            c.args[0]
            for c in client.load_table_from_json.call_args_list
            if c.args[1] == "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"
        ]
        assert key_loads == [
            [{"id": 1, "parent_id": None}],
            [{"id": 2, "parent_id": 42}],
        ]

    def test_partition_decorated_target_fails_before_any_operation(self) -> None:
        dest = BigQueryDestination()
        config = _config(table="events$20261005", upsert_key=["id"])
        opts = _options(mode="mirror")
        with patch.object(dest, "_build_client") as build_client:
            with pytest.raises(ValueError, match=r"mirror.*partition-decorated.*base table"):
                dest.load([], config, opts)
            with pytest.raises(ValueError, match=r"mirror.*partition-decorated.*base table"):
                dest.finalize_sync(config, opts)
        build_client.assert_not_called()

    def test_interrupted_finalize_leaves_key_cleanup_to_reset(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        opts = _options(mode="mirror")
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], config, opts)
            opts._interrupted = True
            assert dest.finalize_sync(config, opts) is None
            assert not any(sql.startswith("DELETE FROM") for sql in _sqls(client))
            dest.reset_write_state(config, opts)
        assert opts._interrupted is False
        client.delete_table.assert_any_call(
            "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4",
            not_found_ok=True,
        )

    def test_engine_interruption_skips_delete_and_resets_staging(self, tmp_path: Path) -> None:
        class TwoRowSource:
            def extract(
                self,
                query: str,
                config: ProfileConfig,
                *,
                query_tags: dict[str, str] | None = None,
            ) -> Iterator[dict[str, Any]]:
                yield {"id": 1}
                yield {"id": 2}

        client = _fake_client()
        modules = _mocked_bq_modules(client)
        destination = BigQueryDestination()
        stop_event = threading.Event()
        original_load = destination.load

        def load_then_stop(
            records: list[dict[str, Any]],
            config: BigQueryDestinationConfig,
            sync_options: SyncOptions,
        ) -> Any:
            result = original_load(records, config, sync_options)
            stop_event.set()
            return result

        sync = SyncConfig(
            name="interrupted_bigquery_mirror",
            model="SELECT 1",
            destination=_config(upsert_key=["id"]),
            sync=_options(mode="mirror", batch_size=1),
        )
        profile = BigQueryProfile(type="bigquery", project="p", dataset="d")
        with (
            patch.dict("sys.modules", modules),
            patch.object(destination, "load", side_effect=load_then_stop),
        ):
            result = run_sync(
                sync,
                TwoRowSource(),
                destination,
                profile,
                tmp_path,
                stop_event=stop_event,
            )

        assert result.interrupted is True
        assert result.success == 1
        assert not any(sql.startswith("DELETE FROM") for sql in _sqls(client))
        client.delete_table.assert_any_call(
            "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4",
            not_found_ok=True,
        )

    def test_tracked_mirror_strategy_fails_before_client(self) -> None:
        with pytest.raises(ValueError, match="not yet supported on bigquery"):
            BigQueryDestination().load(
                [{"id": 1}],
                _config(upsert_key=["id"]),
                _options(mode="mirror", mirror={"strategy": "tracked"}),
            )

    def test_mirror_requires_key_and_complete_scope(self) -> None:
        with pytest.raises(ValueError, match="sync.mode: mirror requires"):
            BigQueryDestination().load([{"id": 1}], _config(), _options(mode="mirror"))
        with pytest.raises(ValueError, match="upsert_key columns missing"):
            BigQueryDestination().load(
                [{"id": 1}, {"name": "missing"}],
                _config(upsert_key=["id"]),
                _options(mode="mirror"),
            )
        with pytest.raises(ValueError, match="mirror.scope columns missing"):
            BigQueryDestination().load(
                [{"id": 1, "parent_id": 2}, {"id": 2}],
                _config(upsert_key=["id"]),
                _options(mode="mirror", mirror={"scope": ["parent_id"]}),
            )

    def test_mirror_rejects_null_upsert_keys_before_staging(self) -> None:
        with pytest.raises(ValueError, match=r"does not support NULL.*\[1, 2\]"):
            BigQueryDestination().load(
                [
                    {"tenant_id": 1, "id": 1},
                    {"tenant_id": 1, "id": None},
                    {"tenant_id": None, "id": 3},
                ],
                _config(upsert_key=["tenant_id", "id"]),
                _options(mode="mirror"),
            )

    def test_empty_source_finalize_is_safe_noop(self) -> None:
        dest = BigQueryDestination()
        assert dest.finalize_sync(_config(upsert_key=["id"]), _options(mode="mirror")) is None
        assert dest._mirror_aborted is False

    def test_diff_stages_typed_composite_removed_keys_and_deletes_exact_matches(self) -> None:
        client = _fake_client()
        tenant_field = _schema_field("tenant_id")
        id_field = _schema_field("id")
        label_field = _schema_field("label")
        client.get_table.return_value.schema = [tenant_field, id_field, label_field]
        modules = _mocked_bq_modules(client)
        destination = BigQueryDestination()
        config = _config(upsert_key=["tenant_id", "id"])
        options = _options(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        )
        options._diff_removed_keys = [
            {"tenant_id": 1, "id": 2},
            {"tenant_id": 2, "id": 1},
        ]
        options._query_tags = {"sync": "mirror-diff"}

        with patch.dict("sys.modules", modules):
            result = destination.finalize_sync(config, options)

        assert result is not None
        scratch = "my-proj.analytics.user_scores__drt_mirror_keys_diff_a1b2c3d4"
        sqls = _sqls(client)
        assert sqls[0] == (
            f"CREATE OR REPLACE TABLE `{scratch}` AS "
            "SELECT `tenant_id`, `id` FROM `my-proj.analytics.user_scores` WHERE FALSE"
        )
        assert sqls[1] == (
            "DELETE FROM `my-proj.analytics.user_scores` AS T "
            f"WHERE EXISTS (SELECT 1 FROM `{scratch}` K "
            "WHERE T.`tenant_id` = K.`tenant_id` AND T.`id` = K.`id`)"
        )
        assert " IN (" not in sqls[1]
        client.load_table_from_json.assert_called_once()
        assert client.load_table_from_json.call_args.args == (options._diff_removed_keys, scratch)
        bq = modules["google.cloud.bigquery"]
        assert client.load_table_from_json.call_args.kwargs["job_config"] == (
            bq.LoadJobConfig.return_value
        )
        bq.LoadJobConfig.assert_called_once_with(
            labels={"sync": "mirror-diff"},
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
            schema=[tenant_field, id_field],
            autodetect=False,
        )
        client.delete_table.assert_called_once_with(scratch, not_found_ok=True)
        assert destination._mirror_keys_table_id is None

    def test_diff_load_does_not_stage_added_or_changed_keys(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        destination = BigQueryDestination()
        options = _options(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        )
        with patch.dict("sys.modules", modules):
            result = destination.load(
                [{"id": 1, "label": "changed"}],
                _config(upsert_key=["id"]),
                options,
            )

        assert result.success == 1
        assert not any(sql.startswith("CREATE OR REPLACE TABLE") for sql in _sqls(client))
        assert destination._mirror_keys_table_id is None
        assert all(
            call_.args[1] != "my-proj.analytics.user_scores__drt_mirror_keys_diff_a1b2c3d4"
            for call_ in client.load_table_from_json.call_args_list
        )

    @pytest.mark.parametrize("removed_keys", [None, []])
    def test_diff_with_no_removed_keys_is_a_noop(
        self, removed_keys: list[dict[str, Any]] | None
    ) -> None:
        destination = BigQueryDestination()
        options = _options(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        )
        options._diff_removed_keys = removed_keys
        with patch.object(destination, "_build_client") as build_client:
            assert destination.finalize_sync(_config(upsert_key=["id"]), options) is None
        build_client.assert_not_called()

    def test_diff_removed_key_validation_runs_before_client_creation(self) -> None:
        destination = BigQueryDestination()
        options = _options(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        )
        with patch.object(destination, "_build_client") as build_client:
            options._diff_removed_keys = [{"tenant_id": 1}]
            with pytest.raises(ValueError, match="upsert_key columns missing"):
                destination.finalize_sync(_config(upsert_key=["tenant_id", "id"]), options)
            options._diff_removed_keys = [{"tenant_id": 1, "id": None}]
            with pytest.raises(ValueError, match="does not support NULL"):
                destination.finalize_sync(_config(upsert_key=["tenant_id", "id"]), options)
        build_client.assert_not_called()

    def test_diff_delete_failure_still_drops_staged_keys(self) -> None:
        client = _fake_client()
        delete_job = MagicMock()
        delete_job.result.side_effect = RuntimeError("delete boom")
        client.query.side_effect = [MagicMock(), delete_job]
        modules = _mocked_bq_modules(client)
        destination = BigQueryDestination()
        options = _options(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        )
        options._diff_removed_keys = [{"id": 7}]

        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="delete boom"):
                destination.finalize_sync(_config(upsert_key=["id"]), options)

        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores__drt_mirror_keys_diff_a1b2c3d4",
            not_found_ok=True,
        )

    def test_removal_only_diff_deletes_before_baseline_commit(self, tmp_path: Path) -> None:
        class RemovalOnlySource:
            def __init__(self) -> None:
                self.extract_calls = 0
                self.commit_calls = 0

            def extract(
                self,
                query: str,
                config: ProfileConfig,
                *,
                query_tags: dict[str, str] | None = None,
            ) -> Iterator[dict[str, Any]]:
                raise AssertionError("ordinary extraction must not run")

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
                self.extract_calls += 1
                return SnapshotDiffResult(
                    added=iter(()),
                    changed=iter(()),
                    removed_keys=iter(({"id": 7},)),
                    is_first_run=False,
                )

            def commit_snapshot_diff(self, config: ProfileConfig, sync_name: str) -> None:
                self.commit_calls += 1

        source = RemovalOnlySource()
        destination = BigQueryDestination()
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        sync = SyncConfig(
            name="bigquery_removal_only",
            model="ref('users')",
            destination=_config(upsert_key=["id"]),
            sync=_options(
                mode="mirror",
                incremental_strategy="diff",
                mirror={"strategy": "diff"},
            ),
        )
        profile = BigQueryProfile(type="bigquery", project="p", dataset="d")

        with (
            patch.dict("sys.modules", modules),
            patch.object(destination, "load", wraps=destination.load) as load,
        ):
            result = run_sync(sync, source, destination, profile, tmp_path)

        assert result.success == 0
        assert result.failed == 0
        assert result.diff_removed_keys == [{"id": 7}]
        delete = next(sql for sql in _sqls(client) if sql.startswith("DELETE FROM"))
        assert "T.`id` = K.`id`" in delete
        load.assert_not_called()
        assert source.extract_calls == 1
        assert source.commit_calls == 1

    def test_interrupted_diff_does_not_delete_or_commit_baseline(self, tmp_path: Path) -> None:
        class InterruptedDiffSource:
            def __init__(self) -> None:
                self.commit_calls = 0

            def extract(
                self,
                query: str,
                config: ProfileConfig,
                *,
                query_tags: dict[str, str] | None = None,
            ) -> Iterator[dict[str, Any]]:
                raise AssertionError("ordinary extraction must not run")

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
                return SnapshotDiffResult(
                    added=iter(()),
                    changed=iter(()),
                    removed_keys=iter(({"id": 7},)),
                    is_first_run=False,
                )

            def commit_snapshot_diff(self, config: ProfileConfig, sync_name: str) -> None:
                self.commit_calls += 1

        source = InterruptedDiffSource()
        destination = BigQueryDestination()
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        stop_event = threading.Event()
        stop_event.set()

        sync = SyncConfig(
            name="interrupted_bigquery_diff_mirror",
            model="ref('users')",
            destination=_config(upsert_key=["id"]),
            sync=_options(
                mode="mirror",
                incremental_strategy="diff",
                mirror={"strategy": "diff"},
                batch_size=1,
            ),
        )
        profile = BigQueryProfile(type="bigquery", project="p", dataset="d")

        with patch.dict("sys.modules", modules):
            result = run_sync(
                sync,
                source,
                destination,
                profile,
                tmp_path,
                stop_event=stop_event,
            )

        assert result.interrupted is True
        assert sync.sync._interrupted is False  # cleared by reset_write_state()
        assert not any(sql.startswith("DELETE FROM") for sql in _sqls(client))
        assert source.commit_calls == 0

    def test_failed_key_stage_aborts_delete(self) -> None:
        client = _fake_client()
        client.query.return_value.result.side_effect = RuntimeError("stage boom")
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        opts = _options(mode="mirror", on_error="skip")
        with patch.dict("sys.modules", modules):
            result = dest.load([{"id": 1}, {"id": 2}], config, opts)
            final = dest.finalize_sync(config, opts)
        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert final is None
        assert not any(sql.startswith("DELETE FROM") for sql in _sqls(client))

    def test_failed_later_key_stage_drops_existing_keys_without_delete(self) -> None:
        client = _fake_client()
        failed_stage = MagicMock()
        failed_stage.result.side_effect = RuntimeError("stage boom")
        client.load_table_from_json.side_effect = [
            MagicMock(),
            MagicMock(),
            failed_stage,
        ]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        opts = _options(mode="mirror", on_error="skip")
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], config, opts)
            result = dest.load([{"id": 2}], config, opts)
            assert dest.finalize_sync(config, opts) is None
        assert result.failed == 1
        client.delete_table.assert_any_call(
            "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4", not_found_ok=True
        )

    def test_failed_merge_after_key_stage_can_finalize_safely(self) -> None:
        client = _fake_client()
        ok_stage = MagicMock()
        failed_merge = MagicMock()
        failed_merge.result.side_effect = RuntimeError("merge boom")
        ok_delete = MagicMock()
        client.query.side_effect = [ok_stage, failed_merge, ok_delete]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        opts = _options(mode="mirror", on_error="skip")
        with patch.dict("sys.modules", modules):
            result = dest.load([{"id": 1}], config, opts)
            final = dest.finalize_sync(config, opts)
        assert result.failed == 1
        assert final is not None
        assert client.query.call_args_list[-1].args[0].startswith("DELETE FROM")

    def test_fail_policy_merge_error_cleans_complete_key_stage_before_raising(self) -> None:
        client = _fake_client()
        failed_merge = MagicMock()
        failed_merge.result.side_effect = RuntimeError("merge boom")
        client.query.side_effect = [MagicMock(), failed_merge]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        options = _options(mode="mirror", on_error="fail")

        with patch.dict("sys.modules", modules):
            with pytest.raises(RuntimeError, match="merge boom"):
                dest.load([{"id": 1}], config, options)

        client.delete_table.assert_has_calls(
            [
                call(
                    "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4",
                    not_found_ok=True,
                ),
                call("my-proj.analytics.user_scores_drt_tmp_a1b2c3d4", not_found_ok=True),
            ]
        )
        assert dest._mirror_keys_table_id is None

    def test_fail_policy_cleans_existing_key_stage_before_raising(self) -> None:
        client = _fake_client()
        failed_stage = MagicMock()
        failed_stage.result.side_effect = RuntimeError("stage boom")
        client.load_table_from_json.side_effect = [
            MagicMock(),
            MagicMock(),
            failed_stage,
        ]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], config, _options(mode="mirror"))
            with pytest.raises(RuntimeError, match="stage boom"):
                dest.load([{"id": 2}], config, _options(mode="mirror", on_error="fail"))
        client.delete_table.assert_any_call(
            "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4", not_found_ok=True
        )
        assert dest._mirror_keys_table_id is None


class TestBigQueryHelpers:
    def test_contiguous_signature_runs_empty(self) -> None:
        assert BigQueryDestination._contiguous_signature_runs([]) == []

    def test_supported_modes_and_optional_job_configs(self) -> None:
        dest = BigQueryDestination()
        assert dest.supported_modes() == frozenset({"replace", "mirror"})
        assert dest._load_job_config(None) is None

    def test_schema_only_load_config_and_empty_mirror_cleanup(self) -> None:
        modules = _mocked_bq_modules()
        field = _schema_field("id")
        dest = BigQueryDestination()
        client = _fake_client()

        with patch.dict("sys.modules", modules):
            dest._load_job_config(None, schema=[field])
        modules["google.cloud.bigquery"].LoadJobConfig.assert_called_once_with(
            schema=[field], autodetect=False
        )

        dest._cleanup_mirror_staging(client)
        client.delete_table.assert_not_called()
        assert dest._mirror_aborted is False

    def test_copy_job_config_without_labels(self) -> None:
        modules = _mocked_bq_modules()
        with patch.dict("sys.modules", modules):
            BigQueryDestination()._copy_job_config(None)
        bq = modules["google.cloud.bigquery"]
        bq.CopyJobConfig.assert_called_once_with(
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE
        )


class TestBigQueryConnection:
    def test_declares_connection_testable(self) -> None:
        """The destination exposes its least-privilege connectivity probe."""
        assert isinstance(BigQueryDestination(), ConnectionTestable)

    def test_test_connection_gets_configured_table_without_query_job(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            BigQueryDestination().test_connection(_config())
        client.get_table.assert_called_once_with("my-proj.analytics.user_scores")
        client.query.assert_not_called()
