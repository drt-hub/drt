"""CLI smoke tests for ``drt run --diff`` (#413).

The diff engine itself is tested in test_diff.py. This module covers
the CLI plumbing: flag validation, JSON-mode embedding, text-mode rendering.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()

PROFILE_YML = {"profiles": {"default": {"type": "duckdb"}}}

SYNC_YML: dict[str, Any] = {
    "name": "sync_a",
    "model": "SELECT 1",
    "destination": {
        "type": "rest_api",
        "url": "https://example.com",
        "method": "POST",
    },
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    creds_dir = tmp_path / ".drt"
    creds_dir.mkdir()
    (creds_dir / "credentials.yml").write_text(yaml.dump(PROFILE_YML))
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "sync_a.yml").write_text(yaml.dump(SYNC_YML))
    return tmp_path


def test_diff_requires_dry_run(project: Path) -> None:
    """--diff without --dry-run must error out before any sync runs."""
    result = runner.invoke(app, ["run", "--diff"])
    assert result.exit_code == 1
    assert "--diff requires --dry-run" in result.output


def test_diff_with_dry_run_runs(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--dry-run --diff completes successfully on a non-queryable destination."""
    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.engine import diff as diff_mod
    from drt.engine import sync as sync_module

    class _FakeResult:
        success = 1
        failed = 0
        skipped = 0
        rows_extracted = 1
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        duration_seconds = 0.01
        interrupted = False
        diff: Any = diff_mod.DiffResult(
            sample=[{"id": 1, "name": "Alice"}],
            total_source_rows=1,
            supported=False,
            fallback_reason="rest_api: no comparison available",
        )

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
        return _FakeResult()

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
        raising=False,
    )
    monkeypatch.setattr(
        cli_main, "_get_source", lambda *_a, **_k: object(), raising=False
    )
    monkeypatch.setattr(
        cli_main, "_get_destination", lambda *_a, **_k: object(), raising=False
    )

    result = runner.invoke(app, ["run", "--dry-run", "--diff"])
    assert result.exit_code == 0
    # The fallback reason should appear in the rendered preview
    assert "no comparison available" in result.output


def test_diff_json_mode_embeds_diff(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--output json --dry-run --diff embeds diff dict in the per-sync entry."""
    import json as _json

    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.engine import diff as diff_mod
    from drt.engine import sync as sync_module

    sample_diff = diff_mod.DiffResult(
        sample=[{"id": 1, "msg": "ping"}],
        total_source_rows=5,
        supported=False,
        fallback_reason="rest_api: no comparison available",
    )

    class _FakeResult:
        success = 5
        failed = 0
        skipped = 0
        rows_extracted = 5
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        duration_seconds = 0.01
        interrupted = False
        diff: Any = sample_diff

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
        return _FakeResult()

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(
        creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
        raising=False,
    )
    monkeypatch.setattr(
        cli_main, "_get_source", lambda *_a, **_k: object(), raising=False
    )
    monkeypatch.setattr(
        cli_main, "_get_destination", lambda *_a, **_k: object(), raising=False
    )

    result = runner.invoke(app, ["run", "--dry-run", "--diff", "--output", "json"])
    assert result.exit_code == 0
    payload = _json.loads(result.output)
    sync_entry = payload["syncs"][0]
    assert "diff" in sync_entry
    assert sync_entry["diff"]["supported"] is False
    assert sync_entry["diff"]["fallback_reason"]
    assert sync_entry["diff"]["sample"] == [{"id": 1, "msg": "ping"}]
