"""Unit tests for the BigQuery source.

Uses ``sys.modules`` injection to mock ``google.cloud.bigquery`` — no real
GCP project or ``google-cloud-bigquery`` install required (matches the
pattern in test_bigquery_destination.py).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from drt.config.credentials import BigQueryProfile
from drt.sources.bigquery import BigQuerySource


def _config(**overrides: Any) -> BigQueryProfile:
    defaults: dict[str, Any] = {"type": "bigquery", "project": "my-proj", "dataset": "analytics"}
    defaults.update(overrides)
    return BigQueryProfile(**defaults)


def _mocked_bq_modules(client: MagicMock) -> dict[str, MagicMock]:
    """sys.modules entries satisfying ``from google.cloud import bigquery``."""
    bigquery_mod = MagicMock()
    bigquery_mod.Client.return_value = client
    # QueryJobConfig(labels=...) needs to round-trip its kwargs for
    # assertions below, not collapse into an opaque MagicMock.
    bigquery_mod.QueryJobConfig.side_effect = lambda **kw: kw

    cloud = MagicMock()
    cloud.bigquery = bigquery_mod
    google = MagicMock()
    google.cloud = cloud

    return {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.bigquery": bigquery_mod,
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
