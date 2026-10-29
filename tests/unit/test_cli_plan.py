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
    from drt.cli import _plan_runner as plan_cmd
    from drt.config import credentials as creds
    from drt.engine import sync as sync_module

    class _Result:
        cursor_value_used: str | None = None
        failed = 0
        interrupted = False

        def __init__(self) -> None:
            self.diff = diff
            self._plan_payload_records = [
                *diff.added,
                *(new for _old, new in diff.updated),
                *diff.inserted,
                *(new for _old, new in diff.replaced),
            ]

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


def test_include_values_writes_only_an_encrypted_private_sidecar(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import stat

    from pyrage import x25519

    _patch_engine(monkeypatch, _changes(), [])
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
    document = json.loads((project / "plan.json").read_text())
    sidecar = project / "plan.json.payload"
    assert document["schema_version"] == 2
    assert document["payload"]["rows"] == 2
    assert document["payload"]["bytes"] == len(sidecar.read_bytes())
    assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
    assert b"a@example.com" not in sidecar.read_bytes()
    assert "a@example.com" not in (project / "plan.json").read_text()


def test_exact_sidecar_is_atomically_replaced_with_private_mode(tmp_path: Path) -> None:
    import os
    import stat

    from drt.cli.commands.plan import _write_new_bytes

    path = tmp_path / "plan.json.payload"
    path.write_bytes(b"old")
    replaced_from: list[Path] = []
    real_replace = os.replace

    def tracked_replace(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
        source_path = Path(source)
        replaced_from.append(source_path)
        assert source_path != path
        assert stat.S_IMODE(source_path.stat().st_mode) == 0o600
        real_replace(source, target)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "replace", tracked_replace)
        _write_new_bytes(path, b"new")

    assert replaced_from
    assert path.read_bytes() == b"new"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_failed_atomic_sidecar_replace_removes_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    from drt.cli.commands.plan import _write_new_bytes

    path = tmp_path / "plan.json.payload"

    def fail_replace(_source: str | os.PathLike[str], _target: str | os.PathLike[str]) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        _write_new_bytes(path, b"new")

    assert not path.exists()
    assert list(tmp_path.iterdir()) == []


def test_failed_plan_write_removes_new_exact_sidecar(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    from drt.cli.commands import plan as plan_command

    _patch_engine(monkeypatch, _changes(), [])
    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))

    def fail_plan_write(_path: Path, _text: str) -> None:
        raise OSError("plan write failed")

    monkeypatch.setattr(plan_command, "_write_new_file", fail_plan_write)
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

    assert result.exit_code == 1
    assert not (project / "plan.json.payload").exists()


def test_sidecar_cleanup_failure_does_not_mask_plan_write_error(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    from drt.cli.commands import plan as plan_command

    _patch_engine(monkeypatch, _changes(), [])
    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_SECRETS_KEY", str(identity))

    def fail_plan_write(_path: Path, _text: str) -> None:
        raise OSError("plan write failed")

    monkeypatch.setattr(plan_command, "_write_new_file", fail_plan_write)
    real_unlink = Path.unlink

    def fail_sidecar_unlink(self: Path, *args: Any, **kwargs: Any) -> None:
        if self.name == "plan.json.payload":
            raise OSError("cleanup failed")
        real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_sidecar_unlink)

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

    assert result.exit_code == 1
    assert isinstance(result.exception, OSError)
    assert str(result.exception) == "plan write failed"


def test_private_age_identity_is_rejected_without_echoing_it(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    _patch_engine(monkeypatch, _changes(), [])
    private_identity = str(x25519.Identity.generate())

    result = runner.invoke(
        app,
        [
            "plan",
            "orders_to_pg",
            "--out",
            "plan.json",
            "--include-values",
            "--recipient",
            private_identity,
        ],
    )

    assert result.exit_code == 1
    assert private_identity not in result.output
    assert "that is a private identity" in result.output
    assert "public key (age1...)" in result.output.replace("\n", " ")


def test_invalid_age_recipient_is_never_echoed(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_engine(monkeypatch, _changes(), [])
    invalid = "not-a-recipient-sensitive-input"

    result = runner.invoke(
        app,
        [
            "plan",
            "orders_to_pg",
            "--out",
            "plan.json",
            "--include-values",
            "--recipient",
            invalid,
        ],
    )

    assert result.exit_code == 1
    assert invalid not in result.output
    assert "Invalid age X25519 recipient." in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["orders_to_pg", "--recipient", "age1notused"],
        ["orders_to_pg", "--include-values", "--recipient", "age1missingout"],
        ["orders_to_pg", "--max-payload-rows", "0"],
        ["orders_to_pg", "--out", "plan.json", "--include-values"],
    ],
)
def test_exact_plan_option_validation(project: Path, args: list[str]) -> None:
    result = runner.invoke(app, ["plan", *args])

    assert result.exit_code == 1


def test_plan_recipients_can_come_from_environment(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    _patch_engine(monkeypatch, _changes(), [])
    identity = x25519.Identity.generate()
    monkeypatch.setenv("DRT_PLAN_RECIPIENTS", str(identity.to_public()))

    result = runner.invoke(app, ["plan", "orders_to_pg", "--out", "plan.json", "--include-values"])

    assert result.exit_code == 0, result.output
    assert (project / "plan.json.payload").exists()


def test_include_values_over_row_cap_is_unavailable(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    _patch_engine(monkeypatch, _changes(), [])
    identity = x25519.Identity.generate()

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
            "--max-payload-rows",
            "1",
        ],
    )

    assert result.exit_code == 1
    assert "--max-payload-rows is 1" in result.output
    assert not (project / "plan.json").exists()
    assert not (project / "plan.json.payload").exists()


def test_include_values_over_byte_cap_is_unavailable(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyrage import x25519

    from drt.cli import _plan_payload

    _patch_engine(monkeypatch, _changes(), [])
    monkeypatch.setattr(_plan_payload, "MAX_PAYLOAD_BYTES", 20)
    identity = x25519.Identity.generate()

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

    assert result.exit_code == 1
    assert "byte cap is 20 bytes" in result.output
    assert not (project / "plan.json").exists()


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


def test_partial_extraction_is_never_an_available_plan(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.cli import _plan_runner as plan_cmd
    from drt.engine import sync as sync_module

    _patch_engine(monkeypatch, DiffResult(total_source_rows=0), [])

    class _Failed:
        failed = 1
        interrupted = False
        cursor_value_used: str | None = None
        diff = DiffResult(total_source_rows=0)

    monkeypatch.setattr(sync_module, "run_sync", lambda *_a, **_k: _Failed())
    monkeypatch.setattr(plan_cmd, "get_source", lambda *_a, **_k: object())

    result = runner.invoke(app, ["plan", "orders_to_pg", "--out", "plan.json"])

    assert result.exit_code == 1
    assert "did not complete" in result.output
    assert not (project / "plan.json").exists()


def test_unsupported_sync_is_refused_before_the_engine_runs(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []
    _patch_engine(monkeypatch, _changes(), calls)
    cfg = {**SYNC_YML, "sync": {"mode": "upsert", "match_policy": "update_only"}}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1
    assert "match_policy" in result.output
    assert calls == []  # nothing was extracted, nothing could have been written


def test_insert_fill_empty_is_invalid_before_plan_can_become_available(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []
    _patch_engine(monkeypatch, _changes(), calls)
    cfg = {
        "name": "orders_to_bigquery",
        "model": "SELECT 1",
        "destination": {
            "type": "bigquery",
            "project": "analytics",
            "dataset": "warehouse",
            "table": "orders",
            "mode": "insert",
            "upsert_key": ["id"],
        },
        "sync": {"mode": "upsert", "write_policy": "fill_empty"},
    }
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))

    result = runner.invoke(app, ["plan", "orders_to_bigquery"])

    assert result.exit_code == 1
    assert "fill_empty needs destination.mode: merge" in result.output
    assert calls == []


def test_unknown_sync_is_an_error(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "nope"])

    assert result.exit_code == 1
    assert "not found" in result.output


def test_redact_keys_hashes_key_values(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "json", "--redact-keys"])

    keys = [e["key"]["id"] for e in json.loads(result.output)["entries"]]
    assert all(isinstance(k, str) and k.startswith("hmac-sha256:") for k in keys)


def test_markdown_output(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "markdown"])

    assert result.exit_code == 0
    assert "### drt plan" in result.output
    assert "| create | 1 |" in result.output


def test_invalid_output_format_is_rejected(project: Path) -> None:
    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "yaml"])

    assert result.exit_code == 1
    assert "--output must be" in result.output


def test_outside_a_project_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1


def test_missing_profile_is_an_error(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from drt.config import credentials as creds

    def boom(*_a: Any, **_k: Any) -> None:
        raise FileNotFoundError("no such profile")

    monkeypatch.setattr(creds, "load_profile", boom)

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1
    assert "no such profile" in result.output


def test_undefined_var_is_an_error(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])
    cfg = {**SYNC_YML, "description": "{{ var('missing') }}"}
    (project / "syncs" / "orders_to_pg.yml").write_text(yaml.dump(cfg))

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1


def test_engine_failure_is_reported_not_raised(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.engine import sync as sync_module

    _patch_engine(monkeypatch, _changes(), [])

    def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("warehouse unreachable")

    monkeypatch.setattr(sync_module, "run_sync", boom)

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1
    assert "Could not compute a plan" in result.output


def test_missing_diff_is_an_error(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from drt.engine import sync as sync_module

    _patch_engine(monkeypatch, _changes(), [])

    class _NoDiff:
        failed = 0
        interrupted = False
        cursor_value_used: str | None = None
        diff = None

    monkeypatch.setattr(sync_module, "run_sync", lambda *_a, **_k: _NoDiff())

    result = runner.invoke(app, ["plan", "orders_to_pg"])

    assert result.exit_code == 1
    assert "No diff was produced" in result.output


def test_unavailable_plan_renders_markdown_too(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unsupported = DiffResult(supported=False, fallback_reason="not queryable")
    _patch_engine(monkeypatch, unsupported, [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "markdown"])

    assert result.exit_code == 1
    assert "Plan unavailable" in result.output


def test_markdown_with_detailed_exitcode_still_prints_and_exits_2(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(
        app, ["plan", "orders_to_pg", "--output", "markdown", "--detailed-exitcode"]
    )

    assert result.exit_code == 2
    assert "### drt plan" in result.output


def test_markdown_honours_redact_keys(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_engine(monkeypatch, _changes(), [])

    result = runner.invoke(app, ["plan", "orders_to_pg", "--output", "markdown", "--redact-keys"])

    assert "sha256:" in result.output
    assert "id=1" not in result.output


def test_plan_text_does_not_highlight_the_plan_id_or_digest_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fragment of the digest (e.g. 'a256:f777') must not be coloured on its own."""
    import io

    from rich.console import Console

    from drt.cli.commands import plan as plan_command

    stream = io.StringIO()
    forced = Console(file=stream, force_terminal=True, color_system="truecolor", width=200)
    monkeypatch.setattr(plan_command, "console", forced)

    plan_command._print_plan_text(
        "Plan for sync 'x' -> postgres (t)\n"
        "  450 to update\n"
        "  plan_id: plan-d8cc67abba2d345b\n"
        "  digest:  sha256:f7773e25977523165da61d252e7a5a606f703e8bfbb34f8ef25b231b9ddacb26\n"
    )
    lines = stream.getvalue().splitlines()

    assert "\x1b[" in lines[1], "numbers in the summary keep their highlight"
    assert all("\x1b[" not in line for line in lines[2:4]), lines[2:4]
