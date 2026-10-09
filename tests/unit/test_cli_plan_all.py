"""``drt plan --all`` and ``drt apply <dir>`` (#1219): the CI review loop."""

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


def _sync(name: str, table: str) -> dict[str, Any]:
    return {
        "name": name,
        "model": "SELECT 1",
        "destination": {
            "type": "postgres",
            "host": "localhost",
            "dbname": "d",
            "user": "u",
            "password": "p",
            "table": table,
            "upsert_key": ["id"],
        },
        "sync": {"mode": "upsert"},
    }


class _World:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {
            "a_orders": [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}],
            "b_users": [{"id": 10, "v": "z"}],
        }
        self.unsupported: set[str] = set()
        self.writes: list[str] = []
        self.fail_write_for: str | None = None

    def diff(self, name: str) -> DiffResult:
        if name in self.unsupported:
            return DiffResult(supported=False, fallback_reason=f"{name}: not queryable")
        rows = self.rows[name]
        return DiffResult(added=list(rows), total_source_rows=len(rows), total_destination_rows=0)


class _Result:
    success = 1
    skipped = 0
    skipped_no_match = 0
    rows_extracted = 1
    row_errors: list[Any] = []
    errors: list[str] = []
    watermark_source: str | None = None
    watermark_lag: str | None = None
    limit_applied: int | None = None
    duration_seconds = 0.01
    interrupted = False
    run_id: str | None = None
    sync_run_id: str | None = "s"
    cursor_value_used: str | None = None

    def __init__(self, world: _World, name: str, dry_run: bool) -> None:
        self.diff = world.diff(name)
        self.failed = 1 if (not dry_run and world.fail_write_for == name) else 0


@pytest.fixture
def world() -> _World:
    return _World()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, world: _World) -> Path:
    from drt.cli import _helpers
    from drt.cli import _plan_runner as plan_runner
    from drt.cli.commands import run as run_cmd
    from drt.config import credentials as creds
    from drt.engine import sync as sync_module

    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    for name, table in (("a_orders", "orders"), ("b_users", "users")):
        (tmp_path / "syncs" / f"{name}.yml").write_text(yaml.dump(_sync(name, table)))

    def fake_run_sync(*args: Any, **kwargs: Any) -> _Result:
        sync, dry_run = args[0], args[5]
        if not dry_run:
            world.writes.append(sync.name)
        return _Result(world, sync.name, bool(dry_run))

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"), raising=False
    )
    for module in (plan_runner, run_cmd):
        monkeypatch.setattr(module, "get_source", lambda *_a, **_k: object(), raising=False)
        monkeypatch.setattr(module, "get_destination", lambda *_a, **_k: object(), raising=False)
    monkeypatch.setattr(_helpers, "get_source", lambda *_a, **_k: object())
    return tmp_path


def test_plan_all_writes_one_plan_per_sync_and_one_markdown_report(project: Path) -> None:
    result = runner.invoke(app, ["plan", "--all", "--out-dir", "plans", "--output", "markdown"])

    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in (project / "plans").glob("*.json")) == [
        "a_orders.json",
        "b_users.json",
    ]
    assert result.output.startswith("<!-- drt-plan -->")
    assert "2 sync(s); 2 with changes." in result.output
    assert "a_orders" in result.output and "b_users" in result.output


def test_a_sync_that_cannot_be_planned_is_reported_not_fatal(project: Path, world: _World) -> None:
    world.unsupported = {"b_users"}

    result = runner.invoke(app, ["plan", "--all", "--out-dir", "plans", "--output", "markdown"])

    assert result.exit_code == 0, result.output
    assert "Plan unavailable" in result.output and "b_users: not queryable" in result.output
    assert [p.name for p in (project / "plans").glob("*.json")] == ["a_orders.json"]


def test_plan_all_json_and_text_outputs(project: Path, world: _World) -> None:
    world.unsupported = {"b_users"}

    as_json = runner.invoke(app, ["plan", "--all", "--output", "json"])
    as_text = runner.invoke(app, ["plan", "--all"])

    documents = json.loads(as_json.output)["plans"]
    assert [d["sync"]["name"] for d in documents] == [
        "a_orders",
        "b_users",
    ]
    assert documents[1]["status"]["available"] is False
    assert "Wrote 1 plan(s)" in as_text.output


def test_plan_all_detailed_exitcode(project: Path, world: _World) -> None:
    assert runner.invoke(app, ["plan", "--all", "--detailed-exitcode"]).exit_code == 2

    world.rows = {"a_orders": [], "b_users": []}
    assert runner.invoke(app, ["plan", "--all", "--detailed-exitcode"]).exit_code == 0


def test_all_and_a_name_are_exclusive_and_one_is_required(project: Path) -> None:
    assert runner.invoke(app, ["plan", "a_orders", "--all"]).exit_code == 1
    assert runner.invoke(app, ["plan"]).exit_code == 1
    assert runner.invoke(app, ["plan", "--all", "--out", "x.json"]).exit_code == 1


def test_plan_all_errors_outside_a_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    assert runner.invoke(app, ["plan", "--all"]).exit_code == 1

    (tmp_path / "drt_project.yml").write_text("name: t\nversion: '0.1'\nprofile: default\n")
    (tmp_path / "syncs").mkdir()
    empty = runner.invoke(app, ["plan", "--all"])
    assert empty.exit_code == 1 and "No syncs" in empty.output


def test_apply_a_directory_applies_each_plan_in_name_order(project: Path, world: _World) -> None:
    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0

    result = runner.invoke(app, ["apply", "plans", "--auto-approve", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert world.writes == ["a_orders", "b_users"]
    assert [a["sync"] for a in json.loads(result.stdout)["applies"]] == ["a_orders", "b_users"]


def test_apply_a_directory_stops_at_the_first_refusal(project: Path, world: _World) -> None:
    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0
    world.rows["a_orders"].append({"id": 3, "v": "drifted"})  # first plan now drifts

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1
    assert "a_orders.json:" in result.output and "no longer matches" in result.output
    assert world.writes == []  # the second plan is not applied after the first was refused


def test_apply_a_directory_stops_after_a_failed_write(project: Path, world: _World) -> None:
    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0
    world.fail_write_for = "a_orders"

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1
    assert world.writes == ["a_orders"]  # b_users never ran


def test_apply_an_empty_directory_is_an_error(project: Path) -> None:
    (project / "plans").mkdir()

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1 and "no *.json plans" in result.output


def test_plan_all_rejects_bad_vars(project: Path) -> None:
    result = runner.invoke(app, ["plan", "--all", "--vars", "::"])

    assert result.exit_code == 1
