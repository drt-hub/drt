"""Tests for `drt test --unit` (#780, CLI wiring)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def _write_sync(tmp_path: Path, data: dict, name: str = "sync") -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    with (syncs_dir / f"{name}.yml").open("w") as f:
        yaml.dump(data, f)


def _rest_sync(**overrides: object) -> dict:
    base = {
        "name": "s",
        "model": "SELECT 1",
        "destination": {"type": "rest_api", "url": "http://example.com"},
    }
    base.update(overrides)
    return base


class TestNoUnitTests:
    def test_no_syncs(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["test", "--unit"])
        assert "No syncs found" in result.output
        assert result.exit_code == 0

    def test_no_unit_tests_defined(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(tmp_path, _rest_sync(tests=[{"row_count": {"min": 1}}]))
        result = runner.invoke(app, ["test", "--unit"])
        assert "No unit_tests defined" in result.output
        assert result.exit_code == 0

    def test_no_unit_tests_defined_json_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(tmp_path, _rest_sync())
        result = runner.invoke(app, ["test", "--unit", "--output", "json"])
        assert json.loads(result.output) == {"status": "no_tests", "results": []}
        assert result.exit_code == 0

    def test_ignores_syncs_without_unit_tests_but_runs_the_rest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """sync.tests: alone must not make a sync eligible for --unit."""
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            _rest_sync(name="quality_only", tests=[{"row_count": {"min": 1}}]),
            name="a",
        )
        _write_sync(
            tmp_path,
            _rest_sync(
                name="has_unit",
                unit_tests=[{"name": "t", "given": [{"id": 1}], "expect": [{"id": 1}]}],
            ),
            name="b",
        )
        result = runner.invoke(app, ["test", "--unit", "--output", "json"])
        payload = json.loads(result.output)
        assert [r["sync"] for r in payload["results"]] == ["has_unit"]


class TestPassAndFail:
    def test_a_passing_unit_test(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            _rest_sync(
                sync={"field_mappings": {"first": "given_name"}},
                unit_tests=[
                    {
                        "name": "renames",
                        "given": [{"id": 1, "first": "Alice"}],
                        "expect": [{"id": 1, "given_name": "Alice"}],
                    }
                ],
            ),
        )
        result = runner.invoke(app, ["test", "--unit"])
        assert result.exit_code == 0
        assert "✓" in result.output
        assert "renames" in result.output

    def test_a_failing_unit_test_exits_nonzero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            _rest_sync(
                unit_tests=[{"name": "wrong", "given": [{"id": 1}], "expect": [{"id": 999}]}]
            ),
        )
        result = runner.invoke(app, ["test", "--unit"])
        assert result.exit_code == 1
        assert "✗" in result.output
        assert "wrong" in result.output

    def test_json_output_shape(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            _rest_sync(
                unit_tests=[
                    {"name": "ok", "given": [{"id": 1}], "expect": [{"id": 1}]},
                    {"name": "bad", "given": [{"id": 1}], "expect": [{"id": 2}]},
                ]
            ),
        )
        result = runner.invoke(app, ["test", "--unit", "--output", "json"])
        payload = json.loads(result.output)
        assert payload["status"] == "failed"
        [sync_result] = payload["results"]
        assert sync_result["sync"] == "s"
        by_name = {t["name"]: t for t in sync_result["tests"]}
        assert by_name["ok"]["passed"] is True
        assert by_name["ok"]["mismatches"] == []
        assert by_name["bad"]["passed"] is False
        assert by_name["bad"]["mismatches"]


class TestLookupsRejected:
    def test_reports_as_a_failure_not_a_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            {
                "name": "s",
                "model": "SELECT 1",
                "destination": {
                    "type": "postgres",
                    "host_env": "H",
                    "dbname_env": "D",
                    "user_env": "U",
                    "password_env": "P",
                    "table": "t",
                    "upsert_key": ["id"],
                    "lookups": {
                        "account_id": {
                            "table": "accounts",
                            "match": {"email": "email"},
                            "select": "id",
                        }
                    },
                },
                "unit_tests": [{"name": "t", "given": [{"id": 1}], "expect": [{"id": 1}]}],
            },
        )
        result = runner.invoke(app, ["test", "--unit"])
        assert result.exit_code == 1
        assert "lookups" in result.output


class TestMutualExclusivity:
    @pytest.mark.parametrize("flag", ["--dry-run", "--store-failures"])
    def test_rejected_with_dry_run_or_store_failures(
        self, flag: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(tmp_path, _rest_sync())
        result = runner.invoke(app, ["test", "--unit", flag])
        assert result.exit_code == 2
        assert "--unit" in result.output


class TestFailFast:
    def test_stops_after_first_failing_sync(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        for name, expect_id in [("a", 999), ("b", 1), ("c", 1)]:
            _write_sync(
                tmp_path,
                _rest_sync(
                    name=name,
                    unit_tests=[{"name": "t", "given": [{"id": 1}], "expect": [{"id": expect_id}]}],
                ),
                name=name,
            )
        result = runner.invoke(app, ["test", "--unit", "--fail-fast", "--output", "json"])
        payload = json.loads(result.output)
        statuses = {r["sync"]: r.get("skipped", False) for r in payload["results"]}
        assert statuses["a"] is False  # ran, failed
        # b and c: exactly one order is deterministic (syncs load alphabetically
        # by filename), so both remaining are skipped after "a" fails.
        assert statuses["b"] is True
        assert statuses["c"] is True

    def test_prints_a_skip_notice_in_text_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        for name, expect_id in [("a", 999), ("b", 1)]:
            _write_sync(
                tmp_path,
                _rest_sync(
                    name=name,
                    unit_tests=[{"name": "t", "given": [{"id": 1}], "expect": [{"id": expect_id}]}],
                ),
                name=name,
            )
        result = runner.invoke(app, ["test", "--unit", "--fail-fast"])
        assert "--fail-fast: skipped 1 sync(s)" in result.output


class TestSelectAndExclude:
    def test_select_narrows_to_one_sync(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        _write_sync(
            tmp_path,
            _rest_sync(
                name="a", unit_tests=[{"name": "t", "given": [{"id": 1}], "expect": [{"id": 1}]}]
            ),
            name="a",
        )
        _write_sync(
            tmp_path,
            _rest_sync(
                name="b", unit_tests=[{"name": "t", "given": [{"id": 1}], "expect": [{"id": 1}]}]
            ),
            name="b",
        )
        result = runner.invoke(app, ["test", "--unit", "--select", "a", "--output", "json"])
        payload = json.loads(result.output)
        assert [r["sync"] for r in payload["results"]] == ["a"]
