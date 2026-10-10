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
        "001-a_orders.json",
        "002-b_users.json",
        "manifest.json",
    ]
    assert result.output.startswith("<!-- drt-plan -->")
    assert "2 sync(s); 2 with changes." in result.output
    assert "a_orders" in result.output and "b_users" in result.output


def test_a_sync_that_cannot_be_planned_is_reported_not_fatal(project: Path, world: _World) -> None:
    world.unsupported = {"b_users"}

    result = runner.invoke(app, ["plan", "--all", "--out-dir", "plans", "--output", "markdown"])

    assert result.exit_code == 0, result.output
    assert "Plan unavailable" in result.output and "b_users: not queryable" in result.output
    assert sorted(p.name for p in (project / "plans").glob("*.json")) == [
        "001-a_orders.json",
        "manifest.json",
    ]


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


def test_apply_a_directory_without_a_manifest_is_refused(project: Path, world: _World) -> None:
    (project / "plans").mkdir()
    (project / "plans" / "001-a_orders.json").write_text("{}")

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1 and "no manifest.json" in result.output
    assert world.writes == []


def _plans(project: Path) -> Path:
    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0
    return project / "plans"


def test_a_run_where_a_sync_failed_to_plan_is_incomplete_and_cannot_be_applied(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _plan_runner as plan_runner

    real = plan_runner.compute_plan

    def flaky(name: str, **kwargs: Any) -> Any:
        if name == "b_users":
            raise plan_runner.PlanCliError("warehouse exploded: password=hunter2")
        return real(name, **kwargs)

    monkeypatch.setattr(plan_runner, "compute_plan", flaky)

    planned = runner.invoke(app, ["plan", "--all", "--out-dir", "plans", "--output", "markdown"])

    assert planned.exit_code == 1  # the job goes red, but the report and plans are still written
    assert "hunter2" not in planned.stdout  # connector error text never reaches the PR comment
    assert "could not be planned" in planned.stdout and "b_users" in planned.stdout
    assert "hunter2" in planned.stderr  # the detail stays in the job log
    manifest = json.loads((project / "plans" / "manifest.json").read_text())
    assert [e["status"] for e in manifest["syncs"]] == ["planned", "error"]

    applied = runner.invoke(app, ["apply", "plans", "--auto-approve"])
    assert applied.exit_code == 1 and "incomplete" in applied.output
    assert world.writes == []  # not even the sync that did plan


def test_an_edited_manifest_is_refused(project: Path, world: _World) -> None:
    plans = _plans(project)
    manifest = json.loads((plans / "manifest.json").read_text())
    manifest["syncs"][1]["status"] = "unavailable"  # drop b_users from the run
    (plans / "manifest.json").write_text(json.dumps(manifest))

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1 and "manifest seal" in result.output
    assert world.writes == []


def test_a_stale_or_missing_plan_file_is_refused(project: Path, world: _World) -> None:
    plans = _plans(project)
    (plans / "003-old_sync.json").write_text(
        (plans / "001-a_orders.json").read_text()
    )  # left over from an earlier run

    stale = runner.invoke(app, ["apply", "plans", "--auto-approve"])
    (plans / "003-old_sync.json").unlink()
    (plans / "002-b_users.json").unlink()
    missing = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    flat = lambda r: " ".join(r.output.split())  # noqa: E731 - rich wraps long lines
    assert stale.exit_code == 1 and "not in the manifest: 003-old_sync.json" in flat(stale)
    assert missing.exit_code == 1 and "missing: 002-b_users.json" in flat(missing)
    assert world.writes == []


def test_swapping_one_plan_file_for_another_is_refused(project: Path, world: _World) -> None:
    plans = _plans(project)
    (plans / "001-a_orders.json").write_text((plans / "002-b_users.json").read_text())

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1 and "not the plan the manifest recorded" in result.output
    assert world.writes == []


def test_every_plan_is_checked_before_the_first_write(project: Path, world: _World) -> None:
    """A later plan that is already refusable must not leave the first one applied."""
    plans = _plans(project)
    from drt.cli._plan_runner import load_plan_key
    from drt.engine.plan import plan_id_of, seal_of

    doc = json.loads((plans / "002-b_users.json").read_text())
    doc["created_at"] = "2020-01-01T00:00:00+00:00"  # stale, and correctly re-sealed
    doc["plan_id"] = plan_id_of(
        doc["digest"], doc["fingerprints"]["cursor_hash"], doc["created_at"]
    )
    doc["seal"] = seal_of(doc, load_plan_key(project))
    (plans / "002-b_users.json").write_text(json.dumps(doc))
    manifest = json.loads((plans / "manifest.json").read_text())
    manifest["syncs"][1]["plan_id"] = doc["plan_id"]
    from drt.engine.plan import build_manifest

    (plans / "manifest.json").write_text(
        json.dumps(build_manifest(manifest["syncs"], load_plan_key(project)))
    )

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 1 and "older than" in result.output
    assert world.writes == []  # a_orders was not applied before b_users was found stale


def test_unplannable_syncs_are_not_applied_and_an_all_unavailable_run_is_a_noop(
    project: Path, world: _World
) -> None:
    world.unsupported = {"a_orders", "b_users"}
    plans = _plans(project)

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    assert result.exit_code == 0 and "nothing to apply" in result.output
    assert sorted(p.name for p in plans.glob("*.json")) == ["manifest.json"]


def test_a_second_plan_run_replaces_the_first_runs_files(project: Path, world: _World) -> None:
    plans = _plans(project)
    assert (plans / "002-b_users.json").exists()
    world.unsupported = {"b_users"}

    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0

    assert not (plans / "002-b_users.json").exists()  # no stale plan survives
    assert runner.invoke(app, ["apply", "plans", "--auto-approve"]).exit_code == 0


def test_a_hostile_sync_name_cannot_escape_the_output_directory(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli.commands import plan as plan_cmd

    for hostile in ("../../etc/passwd", "a/b\\c", "x\ny", "..", "/abs"):
        slug = plan_cmd._slug(hostile)
        assert "/" not in slug and "\\" not in slug and "\n" not in slug
        assert not slug.startswith(".") and slug
    assert plan_cmd._slug("...") == "sync"


def test_the_markdown_report_cannot_be_forged_by_a_sync_name_or_a_reason(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli.commands.plan import _markdown_report

    report = _markdown_report(
        [("evil\n## injected <b>x</b>", None, "unavailable", "reason\n### also injected `x`")]
    )

    assert "\n## injected" not in report and "\n### also injected" not in report


def test_plan_all_rejects_bad_vars(project: Path) -> None:
    result = runner.invoke(app, ["plan", "--all", "--vars", "::"])

    assert result.exit_code == 1


def test_duplicate_sync_names_are_rejected_before_planning(project: Path, world: _World) -> None:
    (project / "syncs" / "z_copy.yml").write_text(yaml.dump(_sync("a_orders", "other")))

    result = runner.invoke(app, ["plan", "--all", "--out-dir", "plans"])
    single = runner.invoke(app, ["plan", "a_orders"])

    assert result.exit_code == 1 and "more than once" in result.output
    assert single.exit_code == 1 and "more than once" in single.output
    assert not (project / "plans").exists()


def test_plan_all_never_writes_through_a_symlink_left_in_the_output_directory(
    project: Path, world: _World
) -> None:
    victim = project / "victim.txt"
    victim.write_text("precious")
    plans = project / "plans"
    plans.mkdir()
    (plans / "001-a_orders.json").symlink_to(victim)
    (plans / "manifest.json").symlink_to(project / "dangling-target")

    result = runner.invoke(app, ["plan", "--all", "--out-dir", "plans"])

    assert result.exit_code == 0, result.output
    assert victim.read_text() == "precious"  # untouched
    assert not (project / "dangling-target").exists()
    assert not (plans / "001-a_orders.json").is_symlink()
    assert not (plans / "manifest.json").is_symlink()
    assert runner.invoke(app, ["apply", "plans", "--auto-approve"]).exit_code == 0


def test_file_names_widen_past_999_syncs_and_apply_accepts_them() -> None:
    from drt.cli._apply_flow import _PLAN_FILE
    from drt.cli.commands.plan import _file_name

    assert _file_name(7, 12, "orders") == "007-orders.json"
    assert _file_name(1000, 1200, "orders") == "1000-orders.json"
    assert _PLAN_FILE.fullmatch("1000-orders.json") and _PLAN_FILE.fullmatch("007-orders.json")
    assert not _PLAN_FILE.fullmatch("1-orders.json")


def test_a_directory_planned_with_another_key_says_how_to_share_the_key(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DRT_PLAN_KEY", "key-from-the-plan-runner")
    assert runner.invoke(app, ["plan", "--all", "--out-dir", "plans"]).exit_code == 0
    monkeypatch.setenv("DRT_PLAN_KEY", "a-different-runner-key")

    result = runner.invoke(app, ["apply", "plans", "--auto-approve"])

    flat = " ".join(result.output.split())
    assert result.exit_code == 1 and "same DRT_PLAN_KEY secret" in flat
    assert "empty secret makes each runner invent its own key" in flat.replace("an unset or ", "")
    assert world.writes == []
