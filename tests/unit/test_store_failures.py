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


def test_store_failures_freshness_count_and_sample_share_one_predicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#779 CI catch: freshness's condition embeds datetime.now(), read fresh
    on every call. With --store-failures, drt/cli/commands/test.py now
    computes build_failing_rows_query ONCE and threads it into both the count
    query and the stored sample fetch, so this asserts the count query
    literally wraps the exact string handed to fetch_failing_rows — not just
    a same-shaped string, the same object — proving they can never observe a
    different instant.
    """
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "s",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [{"freshness": {"column": "updated_at", "max_age": "1 hour"}}],
        },
    )
    from drt.destinations import query as query_module

    captured: dict[str, str] = {}

    def _fake_execute_test_query(dest: object, q: str) -> int:
        captured["count_query"] = q
        return 1  # one stale row -> fails

    def _fake_fetch_failing_rows(dest: object, q: str, limit: int) -> list[dict]:
        captured["rows_query"] = q
        return [{"id": 1, "updated_at": "2020-01-01"}]

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "test_table")
    monkeypatch.setattr(query_module, "execute_test_query", _fake_execute_test_query)
    monkeypatch.setattr(query_module, "fetch_failing_rows", _fake_fetch_failing_rows)

    result = runner.invoke(app, ["test", "--store-failures"])
    assert result.exit_code == 1
    assert captured["count_query"] == (
        f"SELECT COUNT(*) FROM ({captured['rows_query']}) AS _drt_row_test"
    )


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
    result = runner.invoke(app, ["test", "--store-failures", "--store-failures-limit", "5"])
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
# --store-failures storage errors must never affect the test verdict
# (masukai's PR #830 review, blocking item 2): the count-check result is
# authoritative; --store-failures is a best-effort debugging convenience
# layered on top, not a second way to fail or pass a test.
# ---------------------------------------------------------------------------


def test_store_failures_clear_error_does_not_fail_a_passing_test(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A storage error on a PASSING test (e.g. clear_test_failures can't
    unlink a locked file) must not turn it into a failure — the verdict
    stays clean, the storage error is reported alongside it."""
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
    _patch_destination_query(monkeypatch, count=0)  # 0 nulls -> passes

    from drt.state import test_failures as test_failures_module

    def _raise_clear(*args: object, **kwargs: object) -> None:
        raise PermissionError("file is locked")

    monkeypatch.setattr(test_failures_module, "clear_test_failures", _raise_clear)

    result = runner.invoke(app, ["test", "--store-failures", "--output", "json"])
    assert result.exit_code == 0
    payload = json_mod.loads(result.output)
    entry = payload["results"][0]["tests"][0]
    assert entry["passed"] is True
    assert "error" not in entry  # the verdict itself must stay untouched
    assert entry["failures_stored"] == {"error": "file is locked"}


def test_store_failures_fetch_error_preserves_failing_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same principle for a genuine failure: an error while fetching the
    sample must not discard the real value/verdict in favor of the storage
    exception's text."""
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
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "test_table")
    monkeypatch.setattr(query_module, "execute_test_query", lambda d, q: 3)  # fails

    def _raise_fetch(*args: object, **kwargs: object) -> list[dict]:
        raise ConnectionError("destination unreachable")

    monkeypatch.setattr(query_module, "fetch_failing_rows", _raise_fetch)

    result = runner.invoke(app, ["test", "--store-failures", "--output", "json"])
    assert result.exit_code == 1  # the real failure, not masked by the storage error
    payload = json_mod.loads(result.output)
    entry = payload["results"][0]["tests"][0]
    assert entry["passed"] is False
    assert entry["value"] == "3"  # the real count survives
    assert entry["failures_stored"] == {"error": "destination unreachable"}


# ---------------------------------------------------------------------------
# --store-failures-limit lower bound (masukai's PR #830 review, blocking
# item 3): a non-positive limit produced a SQL syntax error (`LIMIT -5`)
# surfaced as a per-test failure rather than a bad-flag usage error.
# ---------------------------------------------------------------------------


def test_store_failures_limit_rejects_non_positive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exit code 2 is Click's standard UsageError code (a bad --option value
    rejected before any test ran) — asserted rather than matching the error
    text, since Typer/Rich renders it as a colorized, word-wrapped panel
    whose exact line breaks and ANSI codes are platform-dependent (this CI
    caught the previous text-match version failing on Linux)."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["test", "--store-failures", "--store-failures-limit", "0"])
    assert result.exit_code == 2


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


def test_warnings_carry_value_for_a_threshold_breach(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#837: a warn test that ran and failed its check must surface `value`,
    not `error` — the data quality is known (3 stale rows), not unknown."""
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
    _patch_destination_query(monkeypatch, count=3)
    result = runner.invoke(app, ["test", "--output", "json"])
    payload = json_mod.loads(result.output)
    warning = payload["warnings"][0]
    assert warning["value"] == "3"
    assert "error" not in warning


def test_warnings_carry_error_for_an_exception_not_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#837: a warn test whose query raised must surface `error`, not a
    `value` masquerading as one — the exception text is not test data."""
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
    from drt.destinations import query as query_module

    monkeypatch.setattr(query_module, "is_queryable", lambda d: True)
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "test_table")

    def _raise(dest: object, q: str) -> int:
        raise RuntimeError("connection refused")

    monkeypatch.setattr(query_module, "execute_test_query", _raise)
    result = runner.invoke(app, ["test", "--output", "json"])
    payload = json_mod.loads(result.output)
    warning = payload["warnings"][0]
    assert warning["error"] == "connection refused"
    assert "value" not in warning


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


# ---------------------------------------------------------------------------
# --store-failures slug collisions (#835): two differently-named tests whose
# `name:` slugifies to the same string must not overwrite each other's file.
# ---------------------------------------------------------------------------


def test_store_failures_disambiguates_colliding_slugs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`"email nn"` and `"email/nn"` both slugify to `email-nn` — without
    disambiguation the second write silently clobbers the first test's
    sample, which is exactly the failure `--store-failures` exists to let an
    operator see."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "orders_sync",
            "model": "SELECT 1",
            "destination": _DEST,
            "tests": [
                {"not_null": {"columns": ["email"]}, "name": "email nn"},
                {"not_null": {"columns": ["other"]}, "name": "email/nn"},
            ],
        },
    )
    _patch_destination_query(monkeypatch, count=1, rows=[{"id": 1, "email": "a@example.com"}])
    result = runner.invoke(app, ["test", "--store-failures"])

    assert result.exit_code == 1
    failures_dir = tmp_path / ".drt" / "test_failures" / "orders_sync"
    # Both tests failed and both must have left a sample — a collision would
    # leave only one file (the second write clobbering the first).
    written = sorted(p.name for p in failures_dir.glob("*.jsonl"))
    assert len(written) == 2, f"expected 2 sample files, found {written}"


def test_test_id_disambiguates_within_seen_set() -> None:
    from drt.cli.commands.test import _test_id
    from drt.config.sync_options import NotNullTest, SyncTest

    a = SyncTest(name="email nn", not_null=NotNullTest(columns=["email"]))
    b = SyncTest(name="email/nn", not_null=NotNullTest(columns=["other"]))

    seen: set[str] = set()
    id_a = _test_id(a, 0, seen)
    id_b = _test_id(b, 1, seen)

    assert id_a != id_b
    assert id_a == "email-nn"  # first claim keeps the clean slug
    assert id_b == "1-email-nn"  # collision falls back to the index prefix

    # Without a seen set (the old call shape), both still collide — locks the
    # bug this issue reports, not just the fix.
    assert _test_id(a, 0) == _test_id(b, 1) == "email-nn"


def test_test_id_path_traversal_stays_neutralized() -> None:
    """Regression guard requested by #835: the collision fix must not
    reopen the path-traversal case that was already handled correctly."""
    from drt.cli.commands.test import _test_id
    from drt.config.sync_options import NotNullTest, SyncTest

    t = SyncTest(name="../../etc/passwd", not_null=NotNullTest(columns=["id"]))
    assert _test_id(t, 0, set()) == "etc-passwd"
