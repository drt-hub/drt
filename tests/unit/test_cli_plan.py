"""CLI tests for ``drt plan`` (#1216): artifact, unavailable plans, exit codes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.engine.diff import DiffResult

runner = CliRunner()

SYNC_YML: dict[str, Any] = {
    "name": "orders_to_pg",
    "model": "SELECT 1",
    "destination": {
        "type": "postgres",
        "host": "localhost",
        "dbname": "d",
        "user": "u",
        "password": "p",
        "table": "orders",
        "upsert_key": ["id"],
    },
    "sync": {"mode": "upsert"},
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    creds = tmp_path / ".drt"
    creds.mkdir()
    (creds / "credentials.yml").write_text(yaml.dump({"profiles": {"default": {"type": "duckdb"}}}))
    syncs = tmp_path / "syncs"
    syncs.mkdir()
    (syncs / "orders_to_pg.yml").write_text(yaml.dump(SYNC_YML))
    return tmp_path


def _patch_engine(monkeypatch: pytest.MonkeyPatch, diff: DiffResult, calls: list[Any]) -> None:
    from drt.cli.commands import plan as plan_cmd
    from drt.config import credentials as creds
    from drt.engine import sync as sync_module

    class _Result:
        cursor_value_used: str | None = None

        def __init__(self) -> None:
            self.diff = diff

    def fake_run_sync(*args: Any, **kwargs: Any) -> _Result:
        calls.append((args, kwargs))
        return _Result()

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"), raising=False
    )
    monkeypatch.setattr(plan_cmd, "get_source", lambda *_a, **_k: object())
    monkeypatch.setattr(plan_cmd, "get_destination", lambda *_a, **_k: object())


def _changes() -> DiffResult:
    return DiffResult(
        added=[{"id": 1, "email": "a@example.com"}],
        updated=[({"id": 2, "email": "x"}, {"id": 2, "email": "y"})],
        total_source_rows=2,
        total_destination_rows=1,
    )


def test_plan_writes_a_complete_unsampled_plan_file(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []
    _patch_engine(monkeypatch, _changes(), calls)

    result = runner.invoke(app, ["plan", "orders_to_pg", "--out", "plan.json"])

    assert result.exit_code == 0, result.output
    written = json.loads((project / "plan.json").read_text())
    assert written["sync"]["name"] == "orders_to_pg"
    assert written["summary"]["create"] == 1
    assert written["summary"]["update"] == 1
    assert "a@example.com" not in json.dumps(written)  # no row values
    assert '"y"' not in json.dumps(written)
    args, kwargs = calls[0]
    assert args[5] is True  # dry_run: a plan never writes
    assert kwargs["compute_diff"] is True
    assert kwargs["diff_limit"] > 10**9  # not a sample
    assert "plan_id" in result.output


def test_plan_json_output_is_the_plan(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output)["summary"]["create"] == 1


def test_detailed_exitcode_distinguishes_changes_from_none(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_engine(monkeypatch, _changes(), [])
    assert runner.invoke(app, ["plan", "orders_to_pg", "--detailed-exitcode"]).exit_code == 2

    _patch_engine(monkeypatch, DiffResult(total_source_rows=1, total_destination_rows=1), [])
    assert runner.invoke(app, ["plan", "orders_to_pg", "--detailed-exitcode"]).exit_code == 0


def test_unavailable_plan_exits_1_and_writes_no_file(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unsupported = DiffResult(
        sample=[{"id": 1}], supported=False, fallback_reason="rest_api: no comparison available"
    )
    _patch_engine(monkeypatch, unsupported, [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--out", "plan.json"])

    assert result.exit_code == 1
    assert "no comparison available" in result.output
    assert not (project / "plan.json").exists()


def test_unknown_sync_is_an_error(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "nope"])

    assert result.exit_code == 1
    assert "not found" in result.output


def test_redact_keys_hashes_key_values(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "json", "--redact-keys"])

    keys = [e["key"]["id"] for e in json.loads(result.output)["entries"]]
    assert all(isinstance(k, str) and k.startswith("sha256:") for k in keys)
