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
        self.deleted: list[dict[str, Any]] = []
        self.baseline: int | None = None
        self.real_run_sync: Any = None

    def diff(self) -> DiffResult:
        return DiffResult(
            added=list(self.added),
            deleted=list(self.deleted),
            delete_reason="mirror" if self.deleted else None,
            delete_baseline=self.baseline,
            total_source_rows=len(self.added),
            total_destination_rows=0,
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
        self._plan_payload_records = list(world.added)


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
        world.calls.append({"dry_run": args[5], "source": args[1], **kwargs})
        return _Result(world, bool(args[5]))

    world.real_run_sync = sync_module.run_sync
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


def _exact_plan(project: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    from pyrage import x25519

    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))
    result = runner.invoke(
        app,
        [
            "plan",
            "orders_to_pg",
            "--out",
            "plan.json",
            "--include-values",
            "--recipient",
            str(identity.to_public()),
        ],
    )
    assert result.exit_code == 0, result.output
    return json.loads((project / "plan.json").read_text())


def _patch_exact_verification(
    project: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    destination_drift: bool = False,
) -> None:
    import dataclasses

    from drt.cli import _plan_runner
    from drt.engine.plan import build_plan

    base = _plan_runner.compute_plan("orders_to_pg", project_dir=project)

    def from_records(_sync_name: str, records: list[dict[str, Any]], **_kwargs: Any) -> Any:
        replayed = list(records)
        if destination_drift:
            replayed.append({"id": 99, "name": "destination changed"})
        diff = DiffResult(
            added=replayed,
            total_source_rows=len(records),
            total_destination_rows=0,
        )
        current = build_plan(
            diff,
            sync_name=base.sync.name,
            sync_mode=base.sync.sync.mode,
            match_policy=base.sync.sync.match_policy,
            destination=base.plan.destination,
            config_fingerprint=base.plan.config_hash,
            environment_fingerprint=base.plan.environment_hash,
            plan_key=_plan_runner.load_plan_key(project),
            drt_version=base.plan.drt_version,
            key_columns=list(base.sync.destination.upsert_key or []),
            mask_columns=set(base.sync.sync.mask or {}),
            exclude_columns=_plan_runner._engine_written_columns(base.sync),
            guards=base.sync.sync.guards,
        )
        return dataclasses.replace(base, plan=current, payload_records=records)

    monkeypatch.setattr(_plan_runner, "compute_plan_from_records", from_records)


def _use_real_engine_with_io(
    monkeypatch: pytest.MonkeyPatch,
    world: _World,
    destination: Any,
) -> None:
    from drt.cli import _helpers, _plan_runner
    from drt.cli.commands import run as run_command
    from drt.engine import diff as diff_module
    from drt.engine import sync as sync_module

    class Source:
        def extract(
            self,
            _query: str,
            _config: Any,
            *,
            query_tags: dict[str, str] | None = None,
        ) -> Any:
            del query_tags
            yield from (dict(row) for row in world.added)

        def test_connection(self, _config: Any) -> bool:
            return True

    source = Source()
    monkeypatch.setattr(sync_module, "run_sync", world.real_run_sync)
    monkeypatch.setattr(diff_module, "fetch_rows_by_keys", lambda *_a, **_k: [])
    for module in (_plan_runner, run_command):
        monkeypatch.setattr(module, "get_source", lambda *_a, **_k: source, raising=False)
        monkeypatch.setattr(module, "get_destination", lambda *_a, **_k: destination, raising=False)
    monkeypatch.setattr(_helpers, "get_source", lambda *_a, **_k: source)


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


def test_non_exact_apply_still_installs_configured_dlq_observer(
    project: Path, world: _World
) -> None:
    from drt.engine.observer import DlqObserver

    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "dlq": {"enabled": True}}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 0, result.output
    [write] = _writes(world)
    observers = write["observer"]._observers
    assert len([observer for observer in observers if isinstance(observer, DlqObserver)]) == 1


def test_exact_apply_ignores_source_drift_and_deletes_payload_after_success(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.engine import diff as diff_module

    _exact_plan(project, monkeypatch)
    # Exercise the real compute_plan_from_records path; only the destination
    # read is faked, like a queryable empty PostgreSQL table.
    monkeypatch.setattr(diff_module, "fetch_rows_by_keys", lambda *_a, **_k: [])
    world.added[0] = {"id": 1, "name": "source moved on"}

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    [write] = _writes(world)
    replayed = list(write["source"].extract("", object()))
    assert replayed[0] == {"id": 1, "name": "a"}
    assert write["exact"] is True
    assert not (project / "plan.json.payload").exists()
    artifact = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert artifact["results"][0]["exact"] is True


def test_noop_exact_apply_deletes_payload_after_success(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.engine import diff as diff_module

    world.added.clear()
    _exact_plan(project, monkeypatch)
    monkeypatch.setattr(diff_module, "fetch_rows", lambda *_a, **_k: [])

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    assert "nothing to apply" in result.output
    assert not (project / "plan.json.payload").exists()


def test_exact_apply_does_not_pass_configured_ledger_to_delivery(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dataclasses

    from drt.engine import diff as diff_module
    from drt.state import factory as state_factory

    _exact_plan(project, monkeypatch)
    monkeypatch.setattr(diff_module, "fetch_rows_by_keys", lambda *_a, **_k: [])
    real_build = state_factory.build_state_bundle
    configured_ledger = object()

    def build_with_ledger(*args: Any, **kwargs: Any) -> Any:
        return dataclasses.replace(real_build(*args, **kwargs), ledger=configured_ledger)

    monkeypatch.setattr(state_factory, "build_state_bundle", build_with_ledger)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    [write] = _writes(world)
    assert write["idempotency_ledger"] is None


def test_exact_verification_matches_existing_row_with_metadata_columns(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.cli._apply_flow import load_exact_payload
    from drt.destinations.base import SyncResult

    cfg = {
        **SYNC_YML,
        "sync": {
            "mode": "upsert",
            "metadata_columns": {
                "synced_at": "_synced_at",
                "run_id": "_run_id",
                "sync_name": "_sync_name",
            },
        },
    }
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    world.added = [{"id": 1, "name": "a"}]
    loaded_rows: list[dict[str, Any]] = []

    class Destination:
        def load(self, records: list[dict[str, Any]], *_args: Any) -> SyncResult:
            loaded_rows.extend(dict(record) for record in records)
            return SyncResult(success=len(records))

    _use_real_engine_with_io(monkeypatch, world, Destination())
    _exact_plan(project, monkeypatch)
    planned_row = load_exact_payload(
        (project / "plan.json").read_text(),
        project / "plan.json.payload",
        project_dir=project,
    ).records[0]

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    [applied_row] = loaded_rows
    artifact = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert planned_row["_run_id"] is None
    assert applied_row["_run_id"] == artifact["invocation"]["run_id"]
    assert applied_row["_synced_at"] != planned_row["_synced_at"]
    assert applied_row["_sync_name"] == "orders_to_pg"


def test_successful_exact_apply_updates_state_but_preserves_cursor(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.destinations.base import SyncResult
    from drt.state.manager import StateManager, SyncState

    cfg = {
        **SYNC_YML,
        "sync": {"mode": "incremental", "cursor_field": "updated_at"},
    }
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    world.added = [{"id": 1, "name": "a", "updated_at": "2026-10-29T00:00:00Z"}]

    class Destination:
        def load(self, records: list[dict[str, Any]], *_args: Any) -> SyncResult:
            return SyncResult(success=len(records))

    _use_real_engine_with_io(monkeypatch, world, Destination())
    state = StateManager(project)
    state.save_sync(
        SyncState(
            "orders_to_pg",
            "2026-10-28T00:00:00Z",
            0,
            "failed",
            last_cursor_value="cursor-before-exact",
        )
    )
    _exact_plan(project, monkeypatch)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    saved = state.get_last_sync("orders_to_pg")
    assert saved is not None
    assert saved.status == "success"
    assert saved.records_synced == 1
    assert saved.last_run_at != "2026-10-28T00:00:00Z"
    assert saved.last_cursor_value == "cursor-before-exact"


@pytest.mark.parametrize("remote_backend", [False, True], ids=["local", "remote"])
def test_exact_row_failure_never_writes_decrypted_values_to_dlq(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
    remote_backend: bool,
) -> None:
    import dataclasses
    from unittest.mock import MagicMock

    from drt.destinations.base import SyncResult
    from drt.destinations.row_errors import RowError
    from drt.state import factory as state_factory

    secret = "decrypted-row-secret"
    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "dlq": {"enabled": True}}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))
    world.added = [{"id": 1, "name": secret}]

    class FailingDestination:
        def load(self, records: list[dict[str, Any]], *_args: Any) -> SyncResult:
            return SyncResult(
                failed=1,
                errors=["rejected"],
                row_errors=[
                    RowError(
                        batch_index=0,
                        record_preview="redacted",
                        http_status=400,
                        error_message="rejected",
                    )
                ],
            )

    _use_real_engine_with_io(monkeypatch, world, FailingDestination())
    remote_dlq = None
    if remote_backend:
        remote_dlq = MagicMock()
        real_build = state_factory.build_state_bundle

        def build_with_remote_dlq(*args: Any, **kwargs: Any) -> Any:
            return dataclasses.replace(real_build(*args, **kwargs), dlq=remote_dlq)

        monkeypatch.setattr(state_factory, "build_state_bundle", build_with_remote_dlq)
    _exact_plan(project, monkeypatch)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 1
    if remote_dlq is not None:
        remote_dlq.append.assert_not_called()
    dlq_dir = project / ".drt" / "dlq"
    assert not dlq_dir.exists()
    assert secret not in "".join(path.read_text() for path in dlq_dir.glob("*") if path.is_file())


def test_compute_plan_from_records_reports_missing_and_duplicate_syncs(
    project: Path,
) -> None:
    from drt.cli._plan_runner import PlanCliError, compute_plan_from_records

    with pytest.raises(PlanCliError, match="not found"):
        compute_plan_from_records("missing", [], project_dir=project)

    duplicate = {**SYNC_YML, "model": "SELECT 2"}
    (project / "syncs" / "duplicate.yml").write_text(yaml.dump(duplicate))
    with pytest.raises(PlanCliError, match="defined more than once"):
        compute_plan_from_records("orders_to_pg", [], project_dir=project)


def test_compute_plan_from_records_wraps_destination_diff_failure(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli._plan_runner import PlanCliError, compute_plan_from_records
    from drt.engine import diff as diff_module

    def fail_diff(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("destination read failed")

    monkeypatch.setattr(diff_module, "compute_diff", fail_diff)

    with pytest.raises(PlanCliError, match="Could not verify exact replay"):
        compute_plan_from_records("orders_to_pg", [{"id": 1}], project_dir=project)


def test_compute_plan_from_records_returns_known_unavailable_result(project: Path) -> None:
    from drt.cli._plan_runner import compute_plan_from_records

    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "match_policy": "update_only"}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))

    context = compute_plan_from_records("orders_to_pg", [{"id": 1}], project_dir=project)

    assert not context.plan.available
    assert "match_policy" in str(context.plan.unavailable_reason)


def test_apply_artifact_drops_raw_exception_and_is_private(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import stat

    from drt.cli.commands import run as run_module

    _plan(project)
    secret = "postgres://user:private-password@db.internal"

    def fail_destination(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError(secret)

    monkeypatch.setattr(run_module, "get_destination", fail_destination)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve"])

    assert result.exit_code == 1
    artifact_path = project / "target" / "drt" / "run_results.json"
    artifact = json.loads(artifact_path.read_text())
    assert "error" not in artifact["results"][0]
    assert secret not in artifact_path.read_text()
    assert stat.S_IMODE(artifact_path.stat().st_mode) == 0o600


def test_exact_apply_catches_destination_drift_and_leaves_payload(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exact_plan(project, monkeypatch)
    _patch_exact_verification(project, monkeypatch, destination_drift=True)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 1
    assert "no longer matches" in result.output
    assert _writes(world) == []
    assert (project / "plan.json.payload").exists()


def test_failed_exact_apply_keeps_payload(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exact_plan(project, monkeypatch)
    _patch_exact_verification(project, monkeypatch)
    world.fail_writes = True

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 1
    assert (project / "plan.json.payload").exists()


def test_keep_payload_retains_successful_exact_sidecar(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _exact_plan(project, monkeypatch)
    _patch_exact_verification(project, monkeypatch)

    result = runner.invoke(
        app, ["apply", "plan.json", "--exact", "--keep-payload", "--auto-approve"]
    )

    assert result.exit_code == 0, result.output
    assert (project / "plan.json.payload").exists()


def test_successful_exact_apply_warns_if_payload_deletion_fails(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.engine import diff as diff_module

    _exact_plan(project, monkeypatch)
    monkeypatch.setattr(diff_module, "fetch_rows_by_keys", lambda *_a, **_k: [])
    real_unlink = Path.unlink

    def fail_payload_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        if self.name == "plan.json.payload":
            raise OSError("read-only artifact store")
        real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_payload_unlink)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    assert "could not delete payload" in result.output
    assert (project / "plan.json.payload").exists()


def test_successful_exact_apply_does_not_delete_replaced_payload(
    project: Path,
    world: _World,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    from drt.cli import _apply_flow
    from drt.engine import diff as diff_module

    _exact_plan(project, monkeypatch)
    monkeypatch.setattr(diff_module, "fetch_rows_by_keys", lambda *_a, **_k: [])
    real_apply = _apply_flow.apply_plan
    replacement = b"replacement-sidecar"

    def apply_then_replace(*args: Any, **kwargs: Any) -> Any:
        outcome = real_apply(*args, **kwargs)
        staged = project / "replacement.payload"
        staged.write_bytes(replacement)
        os.replace(staged, project / "plan.json.payload")
        return outcome

    monkeypatch.setattr(_apply_flow, "apply_plan", apply_then_replace)

    result = runner.invoke(app, ["apply", "plan.json", "--exact", "--auto-approve"])

    assert result.exit_code == 0, result.output
    assert "was replaced after it was read" in result.output
    assert (project / "plan.json.payload").read_bytes() == replacement


def test_apply_hands_the_plan_and_approver_to_the_run_for_its_history_entry(
    project: Path, world: _World
) -> None:
    plan = _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--approved-by", "alice"])

    assert result.exit_code == 0, result.output
    [write] = _writes(world)
    assert write["plan_id"] == plan["plan_id"]
    assert write["approved_by"] == "alice"


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
    assert runner.invoke(app, ["apply", "plan.json", "--keep-payload"]).exit_code == 1
    assert runner.invoke(app, ["apply", "plan.json", "--exact"]).exit_code == 1
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
    assert "guards" not in results["results"][0]
    claim = json.loads((project / ".drt" / "applied_plans" / f"{plan['plan_id']}.json").read_text())
    assert claim["guards_forced"] == ["max_deletes"]


def test_force_percentage_is_bounded_by_the_reviewed_baseline(project: Path, world: _World) -> None:
    world.deleted = [{"id": 100}, {"id": 101}]
    world.baseline = 10
    _guarded_sync(project, {"max_delete_pct": 10})
    _plan(project)

    # Entries/digest are unchanged, but the percentage denominator shrank
    # after review: 2/5 is larger than the reviewed 2/10.
    world.baseline = 5
    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--force-guards"])

    assert result.exit_code == 1
    assert "larger than" in result.output and "Re-run `drt plan`" in result.output
    assert _writes(world) == []


def test_force_percentage_refuses_plan_without_baseline(project: Path, world: _World) -> None:
    world.deleted = [{"id": 100}, {"id": 101}]
    world.baseline = 10
    _guarded_sync(project, {"max_delete_pct": 10})
    _plan(project)
    _forge(project, lambda doc: doc["summary"].pop("delete_baseline"))

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--force-guards"])

    assert result.exit_code == 1
    assert "omits delete_baseline" in result.output
    assert "Re-run `drt plan`" in result.output
    assert _writes(world) == []


def test_force_unevaluable_percentage_is_bounded_by_delete_count(
    project: Path, world: _World
) -> None:
    world.deleted = [{"id": 100}]
    world.baseline = None
    _guarded_sync(project, {"max_delete_pct": 10})
    _plan(project)

    result = runner.invoke(app, ["apply", "plan.json", "--auto-approve", "--force-guards"])

    assert result.exit_code == 0, result.output
    artifact = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert artifact["results"][0]["guards_forced"][0]["observed"] is None


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
