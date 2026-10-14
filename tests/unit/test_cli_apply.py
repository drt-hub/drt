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
        self.added: list[dict[str, Any]] = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        self.cursor: str | None = None
        self.calls: list[dict[str, Any]] = []
        self.fail_writes = False
        self.raise_write: Exception | None = None
        self.rows_extracted = 2
        self.snapshot_rows_override: int | None = None
        self.success = 2
        self.row_errors: list[Any] = []
        self.deleted: list[dict[str, Any]] = []
        self.baseline: int | None = None
        self.written: list[dict[str, Any]] = []

    def diff(self) -> DiffResult:
        return DiffResult(
            added=list(self.added),
            deleted=list(self.deleted),
            delete_reason="mirror" if self.deleted else None,
            delete_baseline=self.baseline,
            total_source_rows=len(self.added),
            total_destination_rows=0,
        )


class _FakeDestination:
    def supports_keyed_upsert(self, config: Any) -> bool:
        return True


class _Result:
    skipped = 0
    skipped_no_match = 0
    errors: list[str] = []
    watermark_source: str | None = None
    watermark_lag: str | None = None
    limit_applied: int | None = None
    duration_seconds = 0.01
    interrupted = False
    run_id: str | None = None
    sync_run_id: str | None = "s"

    def __init__(self, world: _World, dry_run: bool) -> None:
        self.success = world.success
        self.failed = 1 if world.fail_writes and not dry_run else 0
        self.rows_extracted = world.rows_extracted
        self.row_errors = list(world.row_errors)
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
        predicate = kwargs.get("row_filter")
        selected = world.added
        if predicate is not None:
            records = (
                world.added if args[5] else list(args[1].extract("", args[3], query_tags=None))
            )
            selected = [row for row in records if predicate(row)]
        if world.raise_write is not None and not args[5]:
            raise world.raise_write
        if not args[5]:
            world.written.extend(dict(row) for row in selected)
        result = _Result(world, bool(args[5]))
        if predicate is not None:
            result.rows_extracted = (
                world.snapshot_rows_override
                if args[5] and world.snapshot_rows_override is not None
                else len(selected)
            )
            result.success = min(world.success, len(selected))
            result.failed = int(bool(world.fail_writes and not args[5] and selected))
        return result

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"), raising=False
    )
    for module in (plan_runner, run_cmd):
        monkeypatch.setattr(module, "get_source", lambda *_a, **_k: object(), raising=False)
        monkeypatch.setattr(
            module, "get_destination", lambda *_a, **_k: _FakeDestination(), raising=False
        )
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


def _forge(project: Path, edit: Any) -> None:
    """Edit plan.json like a determined editor: recompute plan_id and the seal."""
    from drt.cli._plan_runner import load_plan_key
    from drt.engine.plan import plan_id_of, seal_of

    path = project / "plan.json"
    doc = json.loads(path.read_text())
    edit(doc)
    doc["plan_id"] = plan_id_of(
        doc["digest"], doc["fingerprints"]["cursor_hash"], doc["created_at"]
    )
    doc["seal"] = seal_of(doc, load_plan_key(project))
    path.write_text(json.dumps(doc))


def _writes(world: _World) -> list[dict[str, Any]]:
    return [c for c in world.calls if not c["dry_run"]]


def test_apply_writes_through_the_normal_run_path(project: Path, world: _World) -> None:
    plan = _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output
    writes = _writes(world)
    assert len(writes) == 1
    # A normal UUID run id (metadata_columns.run_id may be a UUID column); the plan
    # is linked through the claim record and run_results.json instead.
    assert writes[0]["run_id"] != plan["plan_id"]
    assert len(writes[0]["run_id"]) == 36
    results = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert results["invocation"]["run_id"] == writes[0]["run_id"]
    assert results["results"][0]["plan_id"] == plan["plan_id"]
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "success"
    assert claim["run_id"] == writes[0]["run_id"]


def test_drift_aborts_before_any_write_and_reports_the_keys(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3, "name": "c"})

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "no longer matches" in result.output
    assert '"id": 3' in result.output
    assert _writes(world) == []


def test_allow_drift_pct_permits_small_drift(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3, "name": "c"})  # 1 of 2 planned keys => 50%

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


def _claim_file(project: Path, plan_id: str, state: str) -> None:
    folder = project / ".drt" / "applied_plans"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{plan_id}.json").write_text(
        json.dumps({"plan_id": plan_id, "state": state, "claimed_at": "2026-10-09T00:00:00+00:00"})
    )


@pytest.mark.parametrize(
    ("state", "message"),
    [
        ("success", "already applied"),
        ("pending", "being applied"),
        ("running", "being applied"),
        ("failed", "already attempted"),
        ("halted", "already attempted"),
    ],
)
def test_a_claimed_plan_is_refused_in_every_state(
    project: Path, world: _World, state: str, message: str
) -> None:
    plan = _plan(project)
    _claim_file(project, plan["plan_id"], state)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert message in result.output
    assert _writes(world) == []


def test_applying_twice_is_refused_the_second_time(project: Path, world: _World) -> None:
    _plan(project)

    assert runner.invoke(app, ["apply", "plan.json", "--auto-approve"]).exit_code == 0
    second = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert second.exit_code == 1
    assert "already applied" in second.output
    assert len(_writes(world)) == 1


def test_canary_halt_is_terminal_and_same_plan_is_refused(project: Path, world: _World) -> None:
    plan = _plan(project)
    world.fail_writes = True

    halted = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert halted.exit_code == 1
    assert "wave 1/3" in halted.output and "HALTED" in halted.output
    claim_path = project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json"
    claim = json.loads(claim_path.read_text())
    assert claim["status"] == "halted"
    assert claim["waves"] == [1.0, 10.0, 100.0]
    assert claim["wave_results"][-1]["failed"] == 1
    assert len(claim["run_ids"]) == 3
    assert "completed_wave_indexes" not in claim
    assert "pinned_cursor" not in claim
    assert "apply_started_at" not in claim

    ordinary_retry = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])
    assert ordinary_retry.exit_code == 1
    assert "already attempted" in ordinary_retry.output
    assert "--resume" not in runner.invoke(app, ["apply", "--help"]).output


def test_canary_json_output_is_one_per_wave_document(project: Path, world: _World) -> None:
    _plan(project)

    result = runner.invoke(
        app,
        ["apply", "plan.json", "--auto-approve", "--canary", "1,10", "--output", "json"],
    )

    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["result"]["canary"]["status"] == "success"
    assert [wave["cumulative_pct"] for wave in document["result"]["canary"]["results"]] == [
        1.0,
        10.0,
        100.0,
    ]


def test_canary_text_output_reports_every_passing_wave(project: Path) -> None:
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 0, result.output
    assert "wave 1/3 (1% cumulative): 0 rows, 0 ok, 0 failed — PASS" in result.output
    assert "wave 2/3 (10% cumulative): 0 rows, 0 ok, 0 failed — PASS" in result.output
    assert "wave 3/3 (100% cumulative): 2 rows, 2 ok, 0 failed — PASS" in result.output


def test_canary_json_halt_reports_the_terminal_verdict(project: Path, world: _World) -> None:
    _plan(project)
    world.fail_writes = True

    result = runner.invoke(
        app,
        ["apply", "plan.json", "--auto-approve", "--canary", "1,10", "--output", "json"],
    )

    assert result.exit_code == 1
    document = json.loads(result.stdout)
    assert document["result"]["status"] == "halted"
    assert document["result"]["canary"]["status"] == "halted"
    assert document["result"]["canary"]["results"][-1]["verdict"] == "halt"
    assert "destination failure under on_error: fail" in document["result"]["canary"]["reason"]


def test_canary_skip_policy_halts_on_the_error_rate(project: Path, world: _World) -> None:
    config = json.loads(json.dumps(SYNC_YML))
    config["sync"] = {"mode": "upsert", "on_error": "skip"}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(config))
    _plan(project)
    world.success = 1
    world.row_errors = ["transform failed"]

    result = runner.invoke(
        app,
        [
            "apply",
            "plan.json",
            "--auto-approve",
            "--canary",
            "1,10",
            "--canary-max-error-pct",
            "40",
        ],
    )

    assert result.exit_code == 1
    assert "wave error rate 50.00% exceeded 40.00%" in result.output


def test_canary_exception_halt_records_only_the_exception_class(
    project: Path, world: _World
) -> None:
    _plan(project)
    world.raise_write = RuntimeError("connector rejected the canary")

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 1
    assert "wave raised an exception" in result.output
    assert "connector rejected the canary" not in result.output


def test_canary_replays_one_snapshot_when_live_source_changes_between_waves(
    project: Path, world: _World
) -> None:
    from drt.cli._apply_flow import apply_plan

    _plan(project)
    approved = [dict(row) for row in world.added]

    def mutate_live_source(_seconds: float) -> None:
        world.added[:] = [{"id": 999, "name": "late"}]

    outcome = apply_plan(
        (project / "plan.json").read_text(),
        project_dir=project,
        approved=True,
        canary_waves=[1, 10],
        canary_soak=timedelta(seconds=1),
        sleep=mutate_live_source,
    )

    assert outcome.canary is not None and outcome.canary["status"] == "success"
    assert sorted(world.written, key=lambda row: row["id"]) == approved
    assert not list((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))
    claim = json.loads((project / ".drt" / "applied_plans" / f"{outcome.plan_id}.json").read_text())
    assert claim["spool"]["rows"] == len(approved)
    assert len(claim["spool"]["sha256"]) == 64


def test_canary_null_key_and_spool_limit_fail_before_any_write(
    project: Path, world: _World
) -> None:
    world.added = [{"id": None, "name": "bad"}]
    _plan(project)
    null_result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "100"])
    assert null_result.exit_code == 1
    assert "Canary key components" in null_result.output and "null." in null_result.output
    assert _writes(world) == []
    assert not list((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))


def test_canary_spool_row_limit_is_configurable_and_fails_closed(
    project: Path, world: _World
) -> None:
    config = json.loads(json.dumps(SYNC_YML))
    config["sync"] = {
        "mode": "upsert",
        "rollout": {"waves": [100], "max_spool_rows": 1},
    }
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(config))
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "max_spool_rows" in result.output
    assert _writes(world) == []
    assert not list((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))


def test_canary_detects_spool_tampering_before_the_next_wave(project: Path, world: _World) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    plan = _plan(project)

    def tamper(_seconds: float) -> None:
        spool = next((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))
        spool.write_bytes(spool.read_bytes() + b" ")

    with pytest.raises(ApplyRefused, match="snapshot hash changed"):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
            canary_soak=timedelta(seconds=1),
            sleep=tamper,
        )

    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "halted" and claim["reason"] == "interrupted"
    assert not list((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))


def test_canary_interruption_during_soak_leaves_terminal_halted_claim(
    project: Path, world: _World
) -> None:
    from drt.cli._apply_flow import apply_plan

    plan = _plan(project)

    def interrupt(_seconds: float) -> None:
        raise SystemExit(143)

    with pytest.raises(SystemExit):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
            canary_soak=timedelta(seconds=1),
            sleep=interrupt,
        )

    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["status"] == "halted"
    assert claim["reason"] == "interrupted"
    assert "finished_at" in claim
    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])
    assert refused.exit_code == 1
    assert "already attempted" in refused.output


def test_canary_interruption_during_wave_is_attributed_and_audited(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli._apply_flow import apply_plan
    from drt.cli.commands import run as run_command

    plan = _plan(project)
    real_run_one = run_command._run_one

    def interrupt_wave(sync: Any, ctx: Any, profile: Any) -> Any:
        if not ctx.dry_run:
            raise KeyboardInterrupt
        return real_run_one(sync, ctx, profile)

    monkeypatch.setattr(run_command, "_run_one", interrupt_wave)
    with pytest.raises(KeyboardInterrupt):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
        )

    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "halted" and claim["reason"] == "interrupted"
    assert claim["current_run_id"] == claim["run_ids"][0]
    assert claim["wave_results"][0]["outcome"] == "error"
    assert claim["wave_results"][0]["exception"] == "KeyboardInterrupt"
    assert claim["wave_results"][0]["failed"] is None
    artifact = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert artifact["results"][0]["canary"]["results"][0]["outcome"] == "error"
    assert not list((project / ".drt" / "applied_plans").glob("*.spool.jsonl"))


def test_canary_claim_update_failure_aborts_before_next_wave(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _apply_flow

    plan = _plan(project)
    real_finish = _apply_flow._finish_claim

    def fail_between_waves(project_dir: Path, plan_id: str, state: str, **updates: Any) -> bool:
        if state == "running" and updates.get("wave_results"):
            return False
        return real_finish(project_dir, plan_id, state, **updates)

    monkeypatch.setattr(_apply_flow, "_finish_claim", fail_between_waves)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 1
    assert "aborting before the next wave" in result.output
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "halted" and claim["reason"] == "interrupted"
    assert len(_writes(world)) == 1


@pytest.mark.parametrize(
    ("failure_point", "message"),
    [
        ("initialize", "initialize the canary claim"),
        ("snapshot", "Could not create the canary snapshot"),
        ("before_wave", "record the current canary wave"),
        ("finalize", "claim could not be finalized"),
    ],
)
def test_canary_claim_write_failures_are_fail_closed(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    message: str,
) -> None:
    from drt.cli import _apply_flow

    _plan(project)
    real_finish = _apply_flow._finish_claim

    def fail_selected(project_dir: Path, plan_id: str, state: str, **updates: Any) -> bool:
        selected = (
            failure_point == "initialize"
            and state == "pending"
            and "max_error_pct" in updates
            or failure_point == "snapshot"
            and state == "pending"
            and "spool" in updates
            or failure_point == "before_wave"
            and state == "running"
            and updates.get("current_run_id") is not None
            or failure_point == "finalize"
            and state == "success"
        )
        return False if selected else real_finish(project_dir, plan_id, state, **updates)

    monkeypatch.setattr(_apply_flow, "_finish_claim", fail_selected)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "100"])

    assert result.exit_code == 1 and message in result.output
    if failure_point != "finalize":
        assert _writes(world) == []


def test_canary_snapshot_row_count_mismatch_fails_before_write(
    project: Path, world: _World
) -> None:
    _plan(project)
    world.snapshot_rows_override = 3

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "100"])

    assert result.exit_code == 1
    assert "row count did not" in result.output and "match completed" in result.output
    assert _writes(world) == []


def test_canary_spool_open_failure_has_no_partial_writer(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _canary_spool

    _plan(project)

    def fail_open(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("spool directory is read-only")

    monkeypatch.setattr(_canary_spool.SnapshotSpoolWriter, "__init__", fail_open)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "100"])
    assert result.exit_code == 1
    assert "spool directory is read-only" in result.output
    assert _writes(world) == []


def test_canary_error_outcome_claim_failure_is_still_terminal(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _apply_flow

    plan = _plan(project)
    world.raise_write = RuntimeError("private destination value")
    real_finish = _apply_flow._finish_claim

    def fail_error_outcome(project_dir: Path, plan_id: str, state: str, **updates: Any) -> bool:
        if state == "halted" and updates.get("reason") == "wave raised an exception":
            return False
        return real_finish(project_dir, plan_id, state, **updates)

    monkeypatch.setattr(_apply_flow, "_finish_claim", fail_error_outcome)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "100"])

    assert result.exit_code == 1 and "terminal canary outcome" in result.output
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "halted"


def test_canary_interruption_keeps_priority_over_artifact_failure(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli._apply_flow import apply_plan
    from drt.cli.commands import run as run_command

    _plan(project)
    real_write = run_command._write_run_results
    calls = 0

    def fail_second_artifact(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("artifact unavailable")
        real_write(*args, **kwargs)

    monkeypatch.setattr(run_command, "_write_run_results", fail_second_artifact)

    def interrupt(_seconds: float) -> None:
        raise SystemExit(143)

    with pytest.raises(SystemExit):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
            canary_soak=timedelta(seconds=1),
            sleep=interrupt,
        )


def test_canary_spool_delete_is_best_effort(
    project: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.cli._apply_flow import apply_plan

    _plan(project)
    real_unlink = Path.unlink

    def fail_spool_unlink(path: Path, *args: Any, **kwargs: Any) -> None:
        if path.name.endswith(".spool.jsonl"):
            raise OSError("busy")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_spool_unlink)
    outcome = apply_plan(
        (project / "plan.json").read_text(),
        project_dir=project,
        approved=True,
        canary_waves=[100],
    )
    assert outcome.applied
    assert "Could not delete canary snapshot" in caplog.text


@pytest.mark.parametrize("soak", ["1s", "1 second"])
def test_configured_canary_soak_uses_the_injected_sleep(project: Path, soak: str) -> None:
    from drt.cli._apply_flow import apply_plan

    config = json.loads(json.dumps(SYNC_YML))
    config["sync"] = {"mode": "upsert", "rollout": {"waves": [1, 10], "soak": soak}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(config))
    _plan(project)
    sleeps: list[float] = []

    outcome = apply_plan(
        (project / "plan.json").read_text(),
        project_dir=project,
        approved=True,
        sleep=sleeps.append,
    )

    assert outcome.canary is not None and outcome.canary["status"] == "success"
    assert sleeps == [1.0, 1.0]


def test_canary_only_controls_require_a_rollout(project: Path) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)

    with pytest.raises(ApplyRefused, match="require --canary or sync.rollout"):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_soak=timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"allow_drift_pct": float("nan")}, "allow_drift_pct must be between 0 and 100"),
        ({"canary_max_error_pct": -1}, "canary_max_error_pct must be between 0 and 100"),
    ],
)
def test_canary_library_rejects_invalid_percentages(
    project: Path, kwargs: dict[str, Any], message: str
) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)

    with pytest.raises(ApplyRefused, match=message):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
            **kwargs,
        )


def test_canary_library_rejects_a_different_expected_plan_id(project: Path) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)

    with pytest.raises(ApplyRefused, match="not the requested wrong-plan-id"):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            expect_plan_id="wrong-plan-id",
            canary_waves=[1, 10],
        )


def test_canary_turns_plan_recomputation_errors_into_refusals(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _plan_runner
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)

    def unavailable(*_args: Any, **_kwargs: Any) -> None:
        raise _plan_runner.PlanCliError("source could not be verified")

    monkeypatch.setattr(_plan_runner, "compute_plan", unavailable)

    with pytest.raises(ApplyRefused, match="source could not be verified"):
        apply_plan(
            (project / "plan.json").read_text(),
            project_dir=project,
            approved=True,
            canary_waves=[1, 10],
        )


def test_canary_refuses_an_unavailable_recomputed_plan(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    _plan(project)

    def unsupported(_self: _World) -> DiffResult:
        return DiffResult(supported=False, fallback_reason="not queryable now")

    monkeypatch.setattr(_World, "diff", unsupported)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 1
    assert "Cannot verify the plan: not queryable now" in result.output


def test_canary_refuses_changed_fingerprints(project: Path, world: _World) -> None:
    _plan(project)
    config = {**SYNC_YML, "sync": {"mode": "upsert", "batch_size": 7}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(config))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 1
    assert "sync definition changed" in result.output
    assert _writes(world) == []


def test_canary_drift_is_refused_or_explicitly_allowed(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3, "name": "c"})

    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])
    allowed = runner.invoke(
        app,
        [
            "apply",
            "plan.json",
            "--auto-approve",
            "--canary",
            "1,10",
            "--allow-drift-pct",
            "60",
        ],
    )

    assert refused.exit_code == 1 and "no longer matches" in refused.output
    assert allowed.exit_code == 0, allowed.output
    assert "Proceeding despite drift: 1 entries (50.0% <= 60.0%)" in allowed.output


def test_canary_plan_without_changes_is_a_noop(project: Path, world: _World) -> None:
    world.added.clear()
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 0
    assert "nothing to apply" in result.output
    assert _writes(world) == []


def test_canary_guards_are_refused_or_forced_and_recorded(project: Path, world: _World) -> None:
    _guarded_sync(project, {"max_creates": 1})
    _plan(project)

    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])
    forced = runner.invoke(
        app,
        ["apply", "plan.json", "--auto-approve", "--canary", "1,10", "--force-guards"],
    )

    assert refused.exit_code == 1 and "Change guards tripped" in refused.output
    assert forced.exit_code == 0, forced.output
    artifact = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert artifact["results"][0]["guards_forced"][0]["guard"] == "max_creates"


@pytest.mark.parametrize(
    ("confirm", "exception", "writes"),
    [
        (None, "Approval is required", 0),
        (lambda _question: False, "Aborted", 0),
        (lambda _question: True, None, 3),
    ],
)
def test_canary_library_confirmation_paths(
    project: Path,
    world: _World,
    confirm: Any,
    exception: str | None,
    writes: int,
) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)
    kwargs = {
        "project_dir": project,
        "canary_waves": [1, 10],
        "confirm": confirm,
    }

    if exception is not None:
        with pytest.raises(ApplyRefused, match=exception):
            apply_plan((project / "plan.json").read_text(), **kwargs)
    else:
        outcome = apply_plan((project / "plan.json").read_text(), **kwargs)
        assert outcome.applied and not outcome.halted
    assert len(_writes(world)) == writes


def test_canary_losing_the_claim_race_writes_nothing(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _apply_flow

    _plan(project)

    def lost(*_args: Any, **_kwargs: Any) -> None:
        raise FileExistsError

    monkeypatch.setattr(_apply_flow, "_claim", lost)
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert result.exit_code == 1
    assert "just taken" in result.output
    assert _writes(world) == []


@pytest.mark.parametrize(
    ("sync_options", "message"),
    [
        ({"mode": "incremental", "cursor_field": "id"}, "non-incremental"),
        ({"mode": "upsert", "incremental_strategy": "diff"}, "snapshot baseline"),
        ({"mode": "mirror"}, "non-incremental"),
        ({"mode": "replace"}, "non-incremental"),
    ],
)
def test_cli_canary_override_rejects_unsupported_sync_before_writes(
    project: Path,
    world: _World,
    sync_options: dict[str, Any],
    message: str,
) -> None:
    config = json.loads(json.dumps(SYNC_YML))
    config["sync"] = sync_options
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(config))
    _plan(project)

    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--canary", "1,10"])

    assert refused.exit_code == 1
    assert message in refused.output
    assert _writes(world) == []


def test_apply_time_canary_guard_rejects_staged_destination() -> None:
    from drt.cli._apply_flow import ApplyRefused, _validate_canary_compatibility
    from drt.config.models import SyncConfig
    from drt.destinations.base import SyncResult

    sync = SyncConfig.model_validate(
        {
            "name": "orders",
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
    )

    class _Staged:
        def stage(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def finalize(self, *_args: Any, **_kwargs: Any) -> SyncResult:
            return SyncResult()

    with pytest.raises(ApplyRefused, match="non-staged"):
        _validate_canary_compatibility(sync, _Staged())


def test_apply_time_canary_guard_requires_keyed_upsert_capability() -> None:
    from drt.cli._apply_flow import ApplyRefused, _validate_canary_compatibility
    from drt.config.models import SyncConfig

    sync = SyncConfig.model_validate(SYNC_YML)

    class _AppendOnly:
        def supports_keyed_upsert(self, config: Any) -> bool:
            return False

    with pytest.raises(ApplyRefused, match="keyed-upsert destination.*PostgreSQL/MySQL"):
        _validate_canary_compatibility(sync, _AppendOnly())


def test_losing_the_claim_race_writes_nothing(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _apply_flow as apply_cmd

    def lost(*_a: Any, **_k: Any) -> None:
        raise FileExistsError

    monkeypatch.setattr(apply_cmd, "_claim", lost)
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "just taken" in result.output
    assert _writes(world) == []


def test_a_crashed_write_leaves_a_failed_claim(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli.commands import run as run_cmd

    plan = _plan(project)

    def boom(*_a: Any, **_k: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(run_cmd, "_run_one", boom)

    runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "failed"


def test_edited_plan_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _rewrite(project, lambda d: d["entries"].append({"key": {"id": 99}, "action": "delete"}))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "modified" in result.output
    assert _writes(world) == []


def test_hand_editing_created_at_or_version_is_caught_by_the_seal(
    project: Path, world: _World
) -> None:
    _plan(project)
    _rewrite(project, lambda d: d.update(created_at=datetime.now(timezone.utc).isoformat()))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "modified" in result.output


def test_stale_plan_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    old = (datetime.now(timezone.utc) - timedelta(hours=30)).isoformat()
    _forge(project, lambda d: d.update(created_at=old))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "--max-age" in result.output
    assert (
        runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--max-age", "48h"]).exit_code
        == 0
    )


def test_future_created_at_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    future = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    _forge(project, lambda d: d.update(created_at=future))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "future" in result.output


def test_different_major_version_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _forge(project, lambda d: d.update(drt_version="9.0.0"))

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


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--canary", "1,,10", "comma-separated list"),
        ("--canary", "one,ten", "only numbers"),
        ("--canary-soak", "eventually", "use a number and a unit"),
    ],
)
def test_bad_canary_cli_options_are_clean_errors(
    project: Path, option: str, value: str, message: str
) -> None:
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", option, value])

    assert result.exit_code == 1
    assert "--canary:" in result.output and message in result.output


def test_malformed_plan_still_reaches_normal_preflight(project: Path, world: _World) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    with pytest.raises(ApplyRefused, match="Cannot apply the plan"):
        apply_plan("not JSON", project_dir=project, approved=True)
    assert _writes(world) == []


def test_invalid_created_at_is_rejected(project: Path, world: _World) -> None:
    _plan(project)
    _forge(project, lambda d: d.update(created_at="yesterday"))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "created_at" in result.output


def test_naive_created_at_is_treated_as_utc(project: Path, world: _World) -> None:
    _plan(project)
    naive = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    _forge(project, lambda d: d.update(created_at=naive))

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
    world.added.extend({"id": i, "name": "x"} for i in range(100, 125))

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


def test_a_changed_value_is_drift_even_though_key_and_action_match(
    project: Path, world: _World
) -> None:
    _plan(project)
    world.added[0] = {"id": 1, "name": "CHANGED"}

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "no longer matches" in result.output
    assert "CHANGED" not in result.output  # values never reach the report
    assert _writes(world) == []


def test_duplicate_planned_keys_cannot_hide_drift(project: Path, world: _World) -> None:
    world.added.append({"id": 1, "name": "a"})  # same key and row twice
    _plan(project)
    world.added.pop()

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert _writes(world) == []


def test_a_different_environment_is_refused(project: Path, world: _World) -> None:
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--vars", "region: eu"])

    assert result.exit_code == 1
    assert "environment" in result.output
    assert _writes(world) == []


def test_cursor_override_round_trips_and_pins_the_real_run_to_the_verified_window(
    project: Path, world: _World
) -> None:
    cfg = {**SYNC_YML, "sync": {"mode": "incremental", "cursor_field": "updated_at"}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    world.cursor = "2026-10-01"
    result = runner.invoke(
        app, ["plan", "orders_to_pg", "--out", "plan.json", "--cursor-value", "2026-10-01"]
    )
    assert result.exit_code == 0, result.output
    world.cursor = "2026-10-08"  # the stored watermark moved after the plan

    refused = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])
    world.cursor = "2026-10-01"
    applied = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--cursor-value", "2026-10-01"]
    )

    assert refused.exit_code == 1
    assert applied.exit_code == 0, applied.output
    assert _writes(world)[0]["cursor_value_override"] == "2026-10-01"


def test_metadata_columns_do_not_break_planning_or_apply(project: Path, world: _World) -> None:
    cfg = {
        **SYNC_YML,
        "sync": {"mode": "upsert", "metadata_columns": {"synced_at": "synced_at"}},
    }
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    _plan(project)

    assert runner.invoke(app, ["apply", "plan.json", "--auto-approve"]).exit_code == 0


@pytest.mark.parametrize("value", ["nan", "inf", "-1", "101"])
def test_drift_percentage_must_be_a_finite_number_in_range(project: Path, value: str) -> None:
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--allow-drift-pct", value])

    assert result.exit_code == 1


def test_nan_cannot_switch_drift_enforcement_off(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3, "name": "c"})

    result = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--allow-drift-pct", "nan"]
    )

    assert result.exit_code == 1
    assert _writes(world) == []


def test_a_plan_from_another_plan_key_is_refused(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DRT_PLAN_KEY", "ci-secret-one")
    _plan(project)
    monkeypatch.setenv("DRT_PLAN_KEY", "ci-secret-two")

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "plan key" in result.output
    assert _writes(world) == []


def test_a_shared_plan_key_lets_another_workspace_apply(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DRT_PLAN_KEY", "ci-secret")
    _plan(project)

    assert runner.invoke(app, ["apply", "plan.json", "--auto-approve"]).exit_code == 0
    assert "ci-secret" not in (project / "plan.json").read_text()


def test_plan_key_file_is_created_private_and_reused(project: Path) -> None:
    import stat

    from drt.cli._plan_runner import load_plan_key

    first = load_plan_key()
    key_file = project / ".drt" / "plan.key"

    assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert load_plan_key() == first


def test_allowed_drift_keeps_json_output_parseable(project: Path, world: _World) -> None:
    _plan(project)
    world.added.append({"id": 3, "name": "c"})

    result = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--allow-drift-pct", "60", "--output", "json"]
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["plan_id"]  # stdout is a single JSON document
    assert "Proceeding despite drift" not in result.stdout


def test_a_claim_finalization_failure_does_not_mask_the_write(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    import tempfile

    def broken(*_a: Any, **_k: Any) -> None:
        raise OSError("disk full")

    plan = _plan(project)
    monkeypatch.setattr(tempfile, "mkstemp", broken)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output  # the write happened and is reported
    assert len(_writes(world)) == 1
    assert (project / "target" / "drt" / "run_results.json").exists()
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["state"] == "pending"  # still protects against a second apply


def test_claim_finalization_does_not_swallow_base_exceptions(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import logging

    from drt.cli import _apply_flow

    class _BrokenLogger:
        def warning(self, *_args: Any, **_kwargs: Any) -> None:
            raise KeyboardInterrupt

    def interrupted_read(*_args: Any, **_kwargs: Any) -> None:
        raise SystemExit(143)

    real_get_logger = logging.getLogger
    monkeypatch.setattr(_apply_flow, "_read_claim", interrupted_read)
    monkeypatch.setattr(
        logging,
        "getLogger",
        lambda name=None: (
            _BrokenLogger() if name == _apply_flow.__name__ else real_get_logger(name)
        ),
    )

    with pytest.raises(SystemExit, match="143"):
        _apply_flow._finish_claim(project, "a" * 64, "halted", reason="interrupted")


def test_claim_finalization_swallows_ordinary_logging_failure(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import logging

    from drt.cli import _apply_flow

    class _BrokenLogger:
        def warning(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("logging unavailable")

    def broken_read(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("claim unavailable")

    real_get_logger = logging.getLogger
    monkeypatch.setattr(_apply_flow, "_read_claim", broken_read)
    monkeypatch.setattr(
        logging,
        "getLogger",
        lambda name=None: (
            _BrokenLogger() if name == _apply_flow.__name__ else real_get_logger(name)
        ),
    )
    assert not _apply_flow._finish_claim(project, "a" * 64, "halted")


def test_finalized_claim_stays_private(project: Path, world: _World) -> None:
    import stat

    plan = _plan(project)
    runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    claim = project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json"
    assert stat.S_IMODE(claim.stat().st_mode) == 0o600
    assert not list(claim.parent.glob("*.tmp"))


def test_profiles_are_fingerprinted_canonically_or_refused() -> None:
    import dataclasses

    from drt.cli._plan_runner import PlanCliError, _canonical_profile

    @dataclasses.dataclass
    class _Profile:
        host: str

    class _Plain:
        def __init__(self) -> None:
            self.host = "h"

    class _Slotted:
        __slots__ = ()

    from pydantic import BaseModel

    class _Model(BaseModel):
        host: str

    assert _canonical_profile(_Model(host="h")) == {"host": "h"}
    assert _canonical_profile(_Profile("h")) == {"host": "h"}
    assert _canonical_profile(_Plain()) == {"host": "h"}
    with pytest.raises(PlanCliError, match="cannot be fingerprinted"):
        _canonical_profile(_Slotted())


def test_two_processes_creating_the_plan_key_agree(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _plan_runner

    key_file = project / ".drt" / "plan.key"
    key_file.write_bytes(b"created-by-the-other-process")
    real_read = Path.read_bytes
    calls = {"n": 0}

    def first_read_misses(self: Path) -> bytes:
        calls["n"] += 1
        if calls["n"] == 1:
            raise FileNotFoundError
        return real_read(self)

    monkeypatch.setattr(Path, "read_bytes", first_read_misses)

    assert _plan_runner.load_plan_key() == b"created-by-the-other-process"


def test_a_failed_claim_rename_cleans_up_its_temp_file(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    def broken_replace(*_a: Any, **_k: Any) -> None:
        raise OSError("read-only filesystem")

    plan = _plan(project)
    real_replace = os.replace

    def only_claims(src: Any, dst: Any) -> None:
        if str(dst).endswith(f"{plan['plan_id']}.json"):
            broken_replace()
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", only_claims)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output
    assert not list((project / ".drt" / "applied_plans").glob("*.tmp"))


def test_an_unremovable_temp_file_does_not_change_the_outcome(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    plan = _plan(project)
    real_replace = os.replace

    def deny(*_a: Any, **_k: Any) -> None:
        raise OSError("denied")

    def only_claims(src: Any, dst: Any) -> None:
        if str(dst).endswith(f"{plan['plan_id']}.json"):
            deny()
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", only_claims)
    monkeypatch.setattr(os, "unlink", deny)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output


def _guarded_sync(project: Path, guards: dict[str, Any]) -> None:
    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "guards": guards}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))


def test_apply_refuses_when_a_guard_trips_and_names_it(project: Path, world: _World) -> None:
    world.deleted = [{"id": i} for i in range(100, 120)]
    world.baseline = 40
    _guarded_sync(project, {"max_deletes": 5, "max_delete_pct": 90})
    plan = _plan(project)
    assert [t["guard"] for t in plan["guards"]["tripped"]] == ["max_deletes"]

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "Change guards tripped" in result.output
    assert "max_deletes" in result.output
    assert "--force-guards" in result.output
    assert _writes(world) == []


def test_force_guards_applies_and_records_the_override(project: Path, world: _World) -> None:
    world.deleted = [{"id": i} for i in range(100, 120)]
    world.baseline = 40
    _guarded_sync(project, {"max_deletes": 5})
    plan = _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--force-guards"])

    assert result.exit_code == 0, result.output
    assert len(_writes(world)) == 1
    results = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert results["results"][0]["guards_forced"][0]["guard"] == "max_deletes"
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["guards_forced"] == ["max_deletes"]


def test_guards_judge_the_recomputed_plan_not_the_file(project: Path, world: _World) -> None:
    _guarded_sync(project, {"max_creates": 1})
    plan = _plan(project)
    assert [t["guard"] for t in plan["guards"]["tripped"]] == ["max_creates"]

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1  # two creates against a limit of 1
    assert _writes(world) == []


def test_within_limits_applies_normally(project: Path, world: _World) -> None:
    _guarded_sync(project, {"max_creates": 5, "max_deletes": 0})
    _plan(project)

    assert runner.invoke(app, ["apply", "plan.json", "--auto-approve"]).exit_code == 0


def test_unevaluable_percentage_guard_blocks_the_apply(project: Path, world: _World) -> None:
    world.deleted = [{"id": 100}]
    world.baseline = None
    _guarded_sync(project, {"max_delete_pct": 50})
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    assert "cannot be evaluated" in result.output


def test_plan_command_reports_guards_without_failing(project: Path, world: _World) -> None:
    world.deleted = [{"id": i} for i in range(100, 110)]
    world.baseline = 20
    _guarded_sync(project, {"max_deletes": 1})

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 0
    assert "GUARD TRIPPED" in result.output


def test_bad_vars_and_unreadable_plan_file_are_clean_errors(project: Path) -> None:
    _plan(project)

    bad_vars = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--vars", "::"])
    unreadable = runner.invoke(app, ["apply", str(project / "syncs"), "--auto-approve"])

    assert bad_vars.exit_code == 1
    assert unreadable.exit_code == 1 and "manifest" in unreadable.output


def test_the_library_refuses_when_nobody_can_approve(project: Path, world: _World) -> None:
    from drt.cli._apply_flow import ApplyRefused, apply_plan

    _plan(project)

    with pytest.raises(ApplyRefused, match="Approval is required"):
        apply_plan((project / "plan.json").read_text(), project_dir=project)
    assert _writes(world) == []


def test_approved_by_is_recorded_from_the_cli(project: Path, world: _World) -> None:
    plan = _plan(project)

    result = runner.invoke(
        app, ["apply", "plan.json", "--auto-approve", "--approved-by", "masukai"]
    )

    assert result.exit_code == 0, result.output
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["approved_by"] == "masukai"
    results = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert results["results"][0]["approved_by"] == "masukai"


def test_declining_the_prompt_is_aborted_not_an_error(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    import typer

    class _Tty:
        def isatty(self) -> bool:
            return True

    _plan(project)
    monkeypatch.setattr(typer, "get_text_stream", lambda *_a, **_k: _Tty())
    monkeypatch.setattr(typer, "confirm", lambda *_a, **_k: False)

    result = runner.invoke(app, ["apply", "plan.json"])

    assert result.exit_code == 1
    assert "Aborted. Nothing was written." in result.output
    assert "Error" not in result.output
