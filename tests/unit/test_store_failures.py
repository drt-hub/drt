"""Tests for `drt test --store-failures` and `severity` (#779).

Covers: the JSONL failure-sample store, mask reuse (PII must never reach
disk unmasked), the row cap, severity's effect on exit code / JSON, and the
--store-failures CLI wiring end to end (mocking the destination query layer,
same pattern as test_cli_test_command.py::test_drt_test_fail_fast_...).
"""

from __future__ import annotations

import json as json_mod
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.state.test_failures import clear_test_failures, write_test_failures

runner = CliRunner()


def _write_sync(tmp_path: Path, data: dict, filename: str = "sync.yml") -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    with (syncs_dir / filename).open("w") as f:
        yaml.dump(data, f)


_DEST = {
    "type": "postgres",
    "connection_string_env": "DB_CONN",
    "table": "test_table",
    "upsert_key": ["id"],
}


# ---------------------------------------------------------------------------
# drt.state.test_failures — the JSONL writer, in isolation
# ---------------------------------------------------------------------------


def test_write_test_failures_writes_jsonl(tmp_path: Path) -> None:
    path = write_test_failures(tmp_path, "orders_sync", "no-negatives", [{"id": 1, "total": -5}])
    assert path == tmp_path / ".drt" / "test_failures" / "orders_sync" / "no-negatives.jsonl"
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    assert json_mod.loads(lines[0]) == {"id": 1, "total": -5}


def test_write_test_failures_overwrites_previous_run(tmp_path: Path) -> None:
    """A snapshot of THIS run, not an accumulating queue (unlike the DLQ)."""
    write_test_failures(tmp_path, "s", "t", [{"id": 1}, {"id": 2}])
    path = write_test_failures(tmp_path, "s", "t", [{"id": 3}])
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    assert json_mod.loads(lines[0]) == {"id": 3}


def test_write_test_failures_empty_list(tmp_path: Path) -> None:
    path = write_test_failures(tmp_path, "s", "t", [])
    assert path.read_text() == ""


def test_write_test_failures_tolerates_non_json_native_values(tmp_path: Path) -> None:
    """DB rows carry datetimes/Decimals — default=str, mirroring row_errors.py."""
    from datetime import datetime, timezone
    from decimal import Decimal

    row_in = {
        "id": 1,
        "at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "amt": Decimal("1.50"),
    }
    path = write_test_failures(tmp_path, "s", "t", [row_in])
    row = json_mod.loads(path.read_text().splitlines()[0])
    assert row["id"] == 1
    assert "2026-01-01" in row["at"]
    assert row["amt"] == "1.50"


def test_clear_test_failures_removes_file(tmp_path: Path) -> None:
    path = write_test_failures(tmp_path, "s", "t", [{"id": 1}])
    assert path.exists()
    clear_test_failures(tmp_path, "s", "t")
    assert not path.exists()


def test_clear_test_failures_missing_file_is_a_noop(tmp_path: Path) -> None:
    clear_test_failures(tmp_path, "no_such_sync", "no_such_test")  # must not raise


# ---------------------------------------------------------------------------
# fetch_failing_rows — SQL-level LIMIT wrapping
# ---------------------------------------------------------------------------


def test_fetch_failing_rows_wraps_query_with_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    from drt.config.models import PostgresDestinationConfig
    from drt.destinations import query as query_module

    captured: dict[str, str] = {}

    class _FakeCursor:
        description = [("id",), ("email",)]

        def execute(self, q: str) -> None:
            captured["query"] = q

        def fetchall(self):
            return [(1, "a@example.com")]

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

        def close(self):
            pass

    monkeypatch.setattr(
        "drt.destinations.postgres.PostgresDestination._connect",
        staticmethod(lambda config: _FakeConn()),
    )
    cfg = PostgresDestinationConfig(
        type="postgres", host="h", dbname="d", table="t", upsert_key=["id"]
    )
    rows = query_module.fetch_failing_rows(cfg, "SELECT * FROM t WHERE total < 0", limit=5)
    assert rows == [{"id": 1, "email": "a@example.com"}]
    assert captured["query"] == (
        "SELECT * FROM (SELECT * FROM t WHERE total < 0) AS _drt_sample LIMIT 5"
    )


# ---------------------------------------------------------------------------
# --store-failures — CLI end to end
# ---------------------------------------------------------------------------


def _patch_destination_query(
    monkeypatch: pytest.MonkeyPatch, *, count: int, rows: list[dict] | None = None
) -> None:
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "test_table")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: count)
    if rows is not None:
        monkeypatch.setattr(query_module, "fetch_failing_rows", lambda d, q, limit: rows[:limit])


def test_store_failures_off_by_default_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"not_null": {"columns": ["email"]}, "name": "email_nn"}],
        },
    )
    rows = [{"id": i, "email": f"u{i}@x.com"} for i in range(3)]
    _patch_destination_query(monkeypatch, count=3, rows=rows)
    result = runner.invoke(app, ["test"])
    assert result.exit_code == 1
    assert not (tmp_path / ".drt" / "test_failures").exists()


def test_store_failures_writes_up_to_the_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "orders_sync",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"not_null": {"columns": ["email"]}, "name": "email_nn"}],
        },
    )
    all_rows = [{"id": i, "email": f"u{i}@x.com"} for i in range(25)]
    _patch_destination_query(monkeypatch, count=25, rows=all_rows)
    result = runner.invoke(
        app, ["test", "--store-failures", "--store-failures-limit", "5"]
    )
    assert result.exit_code == 1
    path = tmp_path / ".drt" / "test_failures" / "orders_sync" / "email_nn.jsonl"
    lines = path.read_text().splitlines()
    assert len(lines) == 5  # capped at N, not all 25


def test_store_failures_clears_stale_sample_on_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A test that failed last run and now passes must not leave a stale,
    misleading failure file behind."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "orders_sync",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"not_null": {"columns": ["email"]}, "name": "email_nn"}],
        },
    )
    path = tmp_path / ".drt" / "test_failures" / "orders_sync" / "email_nn.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text('{"id": 1}\n')  # stale sample from a previous failing run

    _patch_destination_query(monkeypatch, count=0)  # passes this run
    result = runner.invoke(app, ["test", "--store-failures"])
    assert result.exit_code == 0
    assert not path.exists()


def test_store_failures_row_count_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """row_count has no per-row failure concept — store-failures is a no-op
    for it, not an error."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"row_count": {"min": 100}, "name": "min_rows"}],
        },
    )
    _patch_destination_query(monkeypatch, count=1)  # below min=100 -> fails
    result = runner.invoke(app, ["test", "--store-failures"])
    assert result.exit_code == 1
    assert not (tmp_path / ".drt" / "test_failures").exists()


def test_store_failures_masks_pii_before_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The critical property (#427 reuse): sync.mask is applied BEFORE rows
    reach disk. The mocked fetch returns raw, unmasked PII — the file on disk
    must never contain it."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "users_sync",
            "model": "SELECT 1",
            "destination": _DEST,
            "sync": {"mask": {"email": "redact", "ssn": "hash"}},
            "tests": [{"not_null": {"columns": ["email"]}, "name": "email_present"}],
        },
    )
    raw_ssn = "123-45-6789"
    _patch_destination_query(
        monkeypatch,
        count=1,
        rows=[{"id": 1, "email": "real.person@example.com", "ssn": raw_ssn, "plan": "pro"}],
    )
    result = runner.invoke(app, ["test", "--store-failures"])
    assert result.exit_code == 1

    written = (
        tmp_path / ".drt" / "test_failures" / "users_sync" / "email_present.jsonl"
    ).read_text()
    row = json_mod.loads(written.splitlines()[0])

    assert row["email"] == "[REDACTED]"
    assert row["ssn"] != raw_ssn
    assert raw_ssn not in written  # the raw SSN must not appear anywhere in the file
    assert "real.person@example.com" not in written
    assert row["plan"] == "pro"  # unmasked field passes through untouched
    assert row["id"] == 1


def test_store_failures_json_reports_path_and_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"not_null": {"columns": ["email"]}, "name": "email_nn"}],
        },
    )
    _patch_destination_query(monkeypatch, count=2, rows=[{"id": 1}, {"id": 2}])
    result = runner.invoke(app, ["test", "--store-failures", "--output", "json"])
    payload = json_mod.loads(result.output)
    entry = payload["results"][0]["tests"][0]
    assert entry["failures_stored"]["count"] == 2
    assert entry["failures_stored"]["path"].endswith("email_nn.jsonl")


# ---------------------------------------------------------------------------
# severity — exit code / reporting / JSON
# ---------------------------------------------------------------------------


def test_severity_warn_failure_reported_but_exit_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [
                {
                    "freshness": {"column": "updated_at", "max_age": "1 hour"},
                    "severity": "warn",
                }
            ],
        },
    )
    _patch_destination_query(monkeypatch, count=3)  # 3 stale rows -> fails the check
    result = runner.invoke(app, ["test", "--output", "json"])
    assert result.exit_code == 0  # warn never fails the run
    payload = json_mod.loads(result.output)
    assert payload["status"] == "passed"
    test_entry = payload["results"][0]["tests"][0]
    assert test_entry["passed"] is False  # still reported as failed...
    assert test_entry["severity"] == "warn"
    assert len(payload["warnings"]) == 1
    assert payload["warnings"][0]["sync"] == "s"


def test_severity_error_failure_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            # severity unset -> default "error"
            "tests": [{"freshness": {"column": "updated_at", "max_age": "1 hour"}}],
        },
    )
    _patch_destination_query(monkeypatch, count=3)
    result = runner.invoke(app, ["test", "--output", "json"])
    assert result.exit_code == 1
    payload = json_mod.loads(result.output)
    assert payload["status"] == "failed"
    assert payload["warnings"] == []


def test_mixed_suite_warn_and_error_exits_nonzero_reports_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A warn failure alone must not exit 1, but a mixed suite (one warn +
    one error) must — and BOTH must be visible in the results."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [
                {"not_null": {"columns": ["a"]}, "name": "warn_one", "severity": "warn"},
                {"not_null": {"columns": ["b"]}, "name": "error_one"},
            ],
        },
    )
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "test_table")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 1)  # both fail

    result = runner.invoke(app, ["test", "--output", "json"])
    assert result.exit_code == 1
    payload = json_mod.loads(result.output)
    assert payload["status"] == "failed"
    # An explicit name: takes priority in the display, wrapped as type(name) —
    # not_null(warn_one), not the auto-generated not_null(a).
    names = {t["name"]: t for t in payload["results"][0]["tests"]}
    assert names["not_null(warn_one)"]["passed"] is False
    assert names["not_null(error_one)"]["passed"] is False
    assert len(payload["warnings"]) == 1
    assert payload["warnings"][0]["test"] == "not_null(warn_one)"


def test_severity_warn_visual_mark_is_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Text mode: a warn-severity failure gets a distinct (yellow ⚠) mark,
    not the same ✗ as an error failure — otherwise warn is invisible."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"row_count": {"min": 100}, "severity": "warn"}],
        },
    )
    _patch_destination_query(monkeypatch, count=1)
    result = runner.invoke(app, ["test"])
    assert result.exit_code == 0
    assert "⚠" in result.output
    assert "warning(s)" in result.output


# ---------------------------------------------------------------------------
# back-compat: default severity + no --store-failures = zero behavior change
# ---------------------------------------------------------------------------


def test_default_severity_matches_pre_779_behavior(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sync that never sets severity: behaves exactly as before #779 —
    any failure exits 1, no warnings section entries."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"not_null": {"columns": ["id"]}}],
        },
    )
    _patch_destination_query(monkeypatch, count=5)
    result = runner.invoke(app, ["test", "--output", "json"])
    assert result.exit_code == 1
    payload = json_mod.loads(result.output)
    assert payload["status"] == "failed"
    assert payload["warnings"] == []
    assert payload["results"][0]["tests"][0]["severity"] == "error"
