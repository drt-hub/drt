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


def _forge(project: Path, edit: Any) -> None:
    """Edit plan.json like a determined editor: recompute plan_id and the seal."""
    from drt.engine.plan import plan_id_of, seal_of

    path = project / "plan.json"
    doc = json.loads(path.read_text())
    edit(doc)
    doc["plan_id"] = plan_id_of(
        doc["digest"], doc["fingerprints"]["cursor_hash"], doc["created_at"]
    )
    doc["seal"] = seal_of(doc)
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
        ("failed", "already attempted"),
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


def test_losing_the_claim_race_writes_nothing(
    project: Path, world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli.commands import apply as apply_cmd

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
