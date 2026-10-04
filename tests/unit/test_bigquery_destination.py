"""Unit tests for the BigQuery destination.

Uses ``sys.modules`` injection to mock ``google.cloud.bigquery`` /
``google.oauth2.service_account`` — no real GCP project or
``google-cloud-bigquery`` install required (matches the pattern in
test_snowflake_destination.py / test_databricks_destination.py).

The MERGE / auth test shapes are adapted from @PFCAaron12's original #584
contribution.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import ValidationError

from drt.config.models import BigQueryDestinationConfig, SyncOptions
from drt.destinations.base import ConnectionTestable
from drt.destinations.bigquery import BigQueryDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    return client


def _mocked_bq_modules(
    client: MagicMock | None = None, creds: Any = "fake-creds"
) -> dict[str, MagicMock]:
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
    google = MagicMock()
    google.cloud = cloud
    google.oauth2 = oauth2

    return {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.bigquery": bigquery_mod,
        "google.oauth2": oauth2,
        "google.oauth2.service_account": sa_mod,
    }


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
            "my-proj.analytics.user_scores_drt_tmp",
        )
        merge = next(s for s in _sqls(client) if "MERGE" in s)
        assert "MERGE `my-proj.analytics.user_scores` T" in merge
        assert "USING `my-proj.analytics.user_scores_drt_tmp` S" in merge
        assert "ON T.`id` = S.`id`" in merge
        assert "WHEN MATCHED THEN UPDATE SET `score` = S.`score`" in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores_drt_tmp", not_found_ok=True
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
            "hyphenated-project.analytics.daily-events$20261004_drt_tmp",
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
            "my-proj.analytics.user_scores_drt_tmp", not_found_ok=True
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
            "my-proj.analytics.user_scores__drt_swap",
            "my-proj.analytics.user_scores__drt_swap",
        ]
        assert [c.args for c in client.copy_table.call_args_list] == [
            (
                "my-proj.analytics.user_scores",
                "my-proj.analytics.user_scores__drt_swap",
            ),
            (
                "my-proj.analytics.user_scores__drt_swap",
                "my-proj.analytics.user_scores",
            ),
        ]
        assert any(
            sql == "TRUNCATE TABLE `my-proj.analytics.user_scores__drt_swap`"
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
            "my-proj.analytics.user_scores__drt_swap", not_found_ok=True
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
            "my-proj.analytics.user_scores__drt_swap", not_found_ok=True
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
            "my-proj.analytics.user_scores__drt_swap", not_found_ok=True
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
        dest._mirror_keys_table_id = "my-proj.analytics.user_scores__drt_mirror_keys"
        dest._mirror_aborted = True

        with patch.dict("sys.modules", modules):
            dest.reset_write_state(_config(), _options(mode="replace"))

        client.delete_table.assert_has_calls(
            [
                call("my-proj.analytics.user_scores__drt_swap", not_found_ok=True),
                call(
                    "my-proj.analytics.user_scores__drt_mirror_keys",
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
        client.delete_table.side_effect = [RuntimeError("drop swap"), None]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        dest._swap_shadow_created = True
        dest._swap_table_id = "my-proj.analytics.user_scores"
        dest._mirror_keys_table_id = "my-proj.analytics.user_scores__drt_mirror_keys"

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


# ---------------------------------------------------------------------------
# sync.mode: mirror (#1055)
# ---------------------------------------------------------------------------


class TestBigQueryMirrorMode:
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
            "CREATE OR REPLACE TABLE `my-proj.analytics.user_scores__drt_mirror_keys`" in sql
            and "SELECT DISTINCT `tenant_id`, `id`" in sql
            for sql in sqls
        )
        assert any(
            "INSERT INTO `my-proj.analytics.user_scores__drt_mirror_keys`" in sql
            and "SELECT DISTINCT `tenant_id`, `id`" in sql
            for sql in sqls
        )
        delete = next(sql for sql in sqls if sql.startswith("DELETE FROM"))
        assert "NOT EXISTS" in delete
        assert "T.`tenant_id` = K.`tenant_id` AND T.`id` = K.`id`" in delete
        assert " IN (" not in delete
        client.delete_table.assert_has_calls(
            [
                call("my-proj.analytics.user_scores_drt_tmp", not_found_ok=True),
                call("my-proj.analytics.user_scores_drt_tmp", not_found_ok=True),
                call(
                    "my-proj.analytics.user_scores__drt_mirror_keys",
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
        assert "SELECT DISTINCT `parent_id`, `id`" in stage
        delete = next(sql for sql in _sqls(client) if sql.startswith("DELETE FROM"))
        assert "EXISTS" in delete
        assert "T.`parent_id` = K.`parent_id`" in delete

    @pytest.mark.parametrize(
        ("mirror", "message"),
        [
            ({"strategy": "tracked"}, "not yet supported on bigquery"),
            ({"strategy": "diff"}, "not yet supported on bigquery"),
        ],
    )
    def test_unsupported_mirror_strategies_fail_before_client(
        self, mirror: dict[str, str], message: str
    ) -> None:
        option_overrides = {"incremental_strategy": "diff"} if mirror["strategy"] == "diff" else {}
        with pytest.raises(ValueError, match=message):
            BigQueryDestination().load(
                [{"id": 1}],
                _config(upsert_key=["id"]),
                _options(mode="mirror", mirror=mirror, **option_overrides),
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
        ok_stage = MagicMock()
        ok_merge = MagicMock()
        failed_stage = MagicMock()
        failed_stage.result.side_effect = RuntimeError("stage boom")
        client.query.side_effect = [ok_stage, ok_merge, failed_stage]
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
            "my-proj.analytics.user_scores__drt_mirror_keys", not_found_ok=True
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

    def test_fail_policy_cleans_existing_key_stage_before_raising(self) -> None:
        client = _fake_client()
        ok_stage = MagicMock()
        ok_merge = MagicMock()
        failed_stage = MagicMock()
        failed_stage.result.side_effect = RuntimeError("stage boom")
        client.query.side_effect = [ok_stage, ok_merge, failed_stage]
        modules = _mocked_bq_modules(client)
        dest = BigQueryDestination()
        config = _config(upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], config, _options(mode="mirror"))
            with pytest.raises(RuntimeError, match="stage boom"):
                dest.load([{"id": 2}], config, _options(mode="mirror", on_error="fail"))
        client.delete_table.assert_any_call(
            "my-proj.analytics.user_scores__drt_mirror_keys", not_found_ok=True
        )
        assert dest._mirror_keys_table_id is None


class TestBigQueryHelpers:
    def test_supported_modes_and_optional_job_configs(self) -> None:
        dest = BigQueryDestination()
        assert dest.supported_modes() == frozenset({"replace", "mirror"})
        assert dest._load_job_config(None) is None

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
