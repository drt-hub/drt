"""CLI tests for ``drt apply`` (#1217): verify-by-replan, drift, single use."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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


class _World:
    """What the faked source/destination currently look like."""

    def __init__(self) -> None:
        self.added: list[dict[str, Any]] = [{"id": 1}, {"id": 2}]
        self.cursor: str | None = None
        self.calls: list[dict[str, Any]] = []
        self.fail_writes = False

    def diff(self) -> DiffResult:
        return DiffResult(
            added=list(self.added), total_source_rows=len(self.added), total_destination_rows=0
        )


class _Result:
    success = 2
    skipped = 0
    skipped_no_match = 0
    rows_extracted = 2
    row_errors: list[Any] = []
    errors: list[str] = []
    watermark_source: str | None = None
    watermark_lag: str | None = None
    limit_applied: int | None = None
    duration_seconds = 0.01
    interrupted = False
    run_id: str | None = None
    sync_run_id: str | None = "s"

    def __init__(self, world: _World, dry_run: bool) -> None:
        self.failed = 1 if world.fail_writes and not dry_run else 0
        self.diff = world.diff()
        self.cursor_value_used = world.cursor


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
    (tmp_path / ".drt").mkdir()
    (tmp_path / ".drt" / "credentials.yml").write_text(
        yaml.dump({"profiles": {"default": {"type": "duckdb"}}})
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(SYNC_YML))

    def fake_run_sync(*args: Any, **kwargs: Any) -> _Result:
        world.calls.append({"dry_run": args[5], **kwargs})
        return _Result(world, bool(args[5]))

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"), raising=False
    )
    for module in (plan_runner, run_cmd):
        monkeypatch.setattr(module, "get_source", lambda *_a, **_k: object(), raising=False)
        monkeypatch.setattr(module, "get_destination", lambda *_a, **_k: object(), raising=False)
    monkeypatch.setattr(_helpers, "get_source", lambda *_a, **_k: object())
    return tmp_path


def _plan(project: Path) -> dict[str, Any]:
    result = runner.invoke(app, ["plan", "orders_to_pg", "--out", "plan.json"])
    assert result.exit_code == 0, result.output
    return json.loads((project / "plan.json").read_text())


def _rewrite(project: Path, edit: Any) -> None:
    path = project / "plan.json"
    doc = json.loads(path.read_text())
    edit(doc)
    path.write_text(json.dumps(doc))


def _writes(world: _World) -> list[dict[str, Any]]:
    return [c for c in world.calls if not c["dry_run"]]


def test_apply_writes_through_the_normal_run_path_with_plan_id_as_run_id(
    project: Path, world: _World
) -> None:
    plan = _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output
    writes = _writes(world)
    assert len(writes) == 1
    assert writes[0]["run_id"] == plan["plan_id"]
    results = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert results["invocation"]["run_id"] == plan["plan_id"]
    assert results["results"][0]["plan_id"] == plan["plan_id"]


def test_drift_aborts_before_any_write_and_reports_the_keys(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3})

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "no longer matches" in result.output
    assert '"id": 3' in result.output
    assert _writes(world) == []


def test_allow_drift_pct_permits_small_drift(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3})  # 1 of 2 planned keys => 50%

    refused = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--allow-drift-pct", "40"]
    )
    allowed = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--allow-drift-pct", "60"]
    )

    assert refused.exit_code == 1
    assert allowed.exit_code == 0, allowed.output
    assert "Proceeding despite drift" in allowed.output
    assert len(_writes(world)) == 1


def test_a_plan_is_single_use(project: Path, world: _World) -> None:
    from drt.config.parser import load_project
    from drt.state.factory import build_state_bundle
    from drt.state.history import HistoryEntry

    plan = _plan(project)
    bundle = build_state_bundle(load_project(Path(".")), Path("."))
    bundle.history.append(
        HistoryEntry(
            sync_name="orders_to_pg",
            started_at="2026-10-09T00:00:00+00:00",
            completed_at="2026-10-09T00:01:00+00:00",
            duration_seconds=1.0,
            status="success",
            records_synced=2,
            records_failed=0,
            run_id=plan["plan_id"],
        )
    )

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "already applied" in result.output
    assert _writes(world) == []


def test_history_disabled_refuses_because_single_use_cannot_be_enforced(
    project: Path, world: _World
) -> None:
    _plan(project)
    (project / "drt_project.yml").write_text(
        yaml.dump(
            {"name": "t", "version": "0.1", "profile": "default", "history": {"enabled": False}}
        )
    )

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "history" in result.output
    assert _writes(world) == []


def test_edited_plan_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _rewrite(project, lambda d: d["entries"].append({"key": {"id": 99}, "action": "delete"}))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "modified" in result.output
    assert _writes(world) == []


def test_stale_plan_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    old = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
    _rewrite(project, lambda d: d.update(created_at=old))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "--max-age" in result.output
    assert (
        runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--max-age", "48h"]).exit_code
        == 0
    )


def test_different_major_version_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _rewrite(project, lambda d: d.update(drt_version="9.0.0"))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "major version" in result.output


def test_changed_sync_definition_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "batch_size": 7}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "sync definition" in result.output
    assert _writes(world) == []


def test_moved_watermark_is_rejected(project: Path, world: _World) -> None:
    world.cursor = "2026-10-01"
    _plan(project)
    world.cursor = "2026-10-08"

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "watermark" in result.output
    assert _writes(world) == []


def test_requires_auto_approve_without_a_terminal(project: Path, world: _World) -> None:
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json"])

    assert result.exit_code == 1
    assert "--auto-approve" in result.output
    assert _writes(world) == []


def test_plan_without_changes_applies_nothing(project: Path, world: _World) -> None:
    world.added.clear()
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0
    assert "nothing to apply" in result.output
    assert _writes(world) == []


def test_bad_options_and_missing_file(project: Path) -> None:
    assert runner.invoke(app, ["apply", "nope.json"]).exit_code == 1
    _plan(project)
    assert runner.invoke(app, ["apply", "plan.json", "--max-age", "soon"]).exit_code == 1
    assert runner.invoke(app, ["apply", "plan.json", "--allow-drift-pct", "150"]).exit_code == 1
    assert runner.invoke(app, ["apply", "plan.json", "--output", "yaml"]).exit_code == 1


def test_invalid_created_at_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _rewrite(project, lambda d: d.update(created_at="yesterday"))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "created_at" in result.output


def test_naive_created_at_is_treated_as_utc(project: Path, world: _World) -> None:
    _plan(project)
    naive = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    _rewrite(project, lambda d: d.update(created_at=naive))

    assert runner.invoke(app, ["apply", "plan.json", "--auto-approve"]).exit_code == 0


def test_destination_that_became_unplannable_is_not_applied(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    _plan(project)
    monkeypatch.setattr(
        _World,
        "diff",
        lambda self: DiffResult(supported=False, fallback_reason="not queryable now"),
    )

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "Cannot verify the plan" in result.output
    assert _writes(world) == []


def test_large_drift_report_is_truncated(project: Path, world: _World) -> None:
    _plan(project)
    world.added.extend({"id": i} for i in range(100, 125))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "and 15 more" in result.output


@pytest.mark.parametrize(("answer", "code", "writes"), [(True, 0, 1), (False, 1, 0)])
def test_interactive_confirmation(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
    answer: bool,
    code: int,
    writes: int,
) -> None:
    import typer

    class _Tty:
        def isatty(self) -> bool:
            return True

    _plan(project)
    monkeypatch.setattr(typer, "get_text_stream", lambda *_a, **_k: _Tty())
    monkeypatch.setattr(typer, "confirm", lambda *_a, **_k: answer)

    result = runner.invoke(app, ["apply", "plan.json"])

    assert result.exit_code == code
    assert len(_writes(world)) == writes
    if not answer:
        assert "Nothing was written" in result.output


def test_json_output_and_failed_write(project: Path, world: _World) -> None:
    plan = _plan(project)
    world.fail_writes = True

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--output", "json"])

    assert result.exit_code == 1
    assert plan["plan_id"] in result.output
