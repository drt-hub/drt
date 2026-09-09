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


class _NotFound(Exception):
    """Stand-in for google.api_core.exceptions.NotFound — a real Exception
    subclass is required since ``except NotFound:`` can't catch a MagicMock."""


class _Conflict(Exception):
    """Stand-in for google.api_core.exceptions.Conflict (dataset-create race)."""


def _mocked_bq_modules(client: MagicMock) -> dict[str, MagicMock]:
    """sys.modules entries satisfying ``from google.cloud import bigquery``."""
    bigquery_mod = MagicMock()
    bigquery_mod.Client.return_value = client
    # QueryJobConfig(labels=...) needs to round-trip its kwargs for
    # assertions below, not collapse into an opaque MagicMock.
    bigquery_mod.QueryJobConfig.side_effect = lambda **kw: kw
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

    return {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.bigquery": bigquery_mod,
        "google.api_core": api_core,
        "google.api_core.exceptions": api_core_exceptions,
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

        client.delete_table.assert_called_once_with("my-proj._drt._drt_runs")

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
