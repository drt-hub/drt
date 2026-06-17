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
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import BigQueryDestinationConfig, SyncOptions
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
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="drt-core\\[bigquery\\]"):
                BigQueryDestination().load([{"id": 1}], _config(), _options())

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
        client.insert_rows_json.assert_called_once_with(
            "my-proj.analytics.user_scores", records
        )

    def test_insert_per_row_error_on_error_skip(self) -> None:
        client = _fake_client()
        client.insert_rows_json.return_value = [{"index": 0, "errors": [{"r": "bad"}]}]
        modules = _mocked_bq_modules(client)
        records = [{"id": 1}, {"id": 2}]
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(
                records, _config(), _options(on_error="skip")
            )
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

    def test_merge_success(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = BigQueryDestination().load(records, config, _options())
        assert result.success == 1
        client.load_table_from_json.assert_called_once_with(
            records, "my-proj.analytics.user_scores_drt_tmp"
        )
        merge = next(s for s in _sqls(client) if "MERGE" in s)
        assert "MERGE `my-proj.analytics.user_scores` T" in merge
        assert "USING `my-proj.analytics.user_scores_drt_tmp` S" in merge
        assert "ON T.id = S.id" in merge
        assert "WHEN MATCHED THEN UPDATE SET score = S.score" in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge
        client.delete_table.assert_called_once_with(
            "my-proj.analytics.user_scores_drt_tmp", not_found_ok=True
        )

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
        assert len(result.row_errors) == 1


class TestBigQueryConnection:
    def test_test_connection_runs_select_1(self) -> None:
        client = _fake_client()
        modules = _mocked_bq_modules(client)
        with patch.dict("sys.modules", modules):
            BigQueryDestination().test_connection(_config())
        assert any("SELECT 1" in s for s in _sqls(client))
