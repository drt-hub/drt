"""`drt run` flags that must refuse a sync.backfill (ADR 0021 slice 3).

Each of these would let a run claim a window complete when it only sampled or reset
it, so the CLI refuses them before touching a source or destination.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()

_SYNC = textwrap.dedent(
    """\
    name: b
    model: "SELECT id FROM t"
    destination:
      type: postgres
      host_env: H
      dbname_env: D
      user_env: U
      password_env: P
      table: t
      upsert_key: [id]
    sync:
      mode: incremental
      cursor_field: id
      backfill:
        window: 10
        start: 0
        end: 100
    """
)


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # Never read or write the developer's real ~/.drt: point HOME at the sandbox.
    home = tmp_path / "home"
    (home / ".drt").mkdir(parents=True)
    (home / ".drt" / "profiles.yml").write_text("dev:\n  type: duckdb\n  database: ':memory:'\n")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text('name: p\nversion: "0.1"\nprofile: dev\n')
    (tmp_path / "syncs" / "b.yml").write_text(_SYNC)
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--limit", "5"], "--limit is not allowed with sync.backfill"),
        (["--full-refresh"], "--full-refresh is not allowed with sync.backfill"),
        (["--cursor-value", "3"], "--cursor-value is not allowed with sync.backfill"),
    ],
)
def test_flags_that_would_fake_window_completion_are_refused(
    project: Path, flags: list[str], message: str
) -> None:
    result = runner.invoke(app, ["run", "--select", "b", *flags])

    assert result.exit_code == 1
    assert message in " ".join(result.output.split())
    assert "b" in result.output


def test_conflicting_full_refresh_and_cursor_value_are_refused_first(project: Path) -> None:
    result = runner.invoke(app, ["run", "--select", "b", "--full-refresh", "--cursor-value", "3"])

    assert result.exit_code == 1
    assert "mutually exclusive" in " ".join(result.output.split())
