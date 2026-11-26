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


_PLAIN_SYNC = _SYNC.replace("name: b", "name: p").replace(
    "  backfill:\n    window: 10\n    start: 0\n    end: 100\n", ""
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
    (tmp_path / "syncs" / "p.yml").write_text(_PLAIN_SYNC)
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


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--backfill-start", "5", "--cursor-value", "3"], "--cursor-value"),
        (["--backfill-end", "50", "--full-refresh"], "--full-refresh"),
        (["--backfill-start", "5", "--limit", "5"], "--limit"),
    ],
)
def test_range_flags_are_exclusive_with_the_other_overrides(
    project: Path, flags: list[str], message: str
) -> None:
    result = runner.invoke(app, ["run", "--select", "b", *flags])

    text = " ".join(result.output.split())
    assert result.exit_code == 1
    assert message in text
    # Either the range-flag exclusion or the older per-flag refusal fires first; both
    # stop the run before anything is extracted.
    assert "mutually exclusive" in text or "not allowed with sync.backfill" in text


@pytest.mark.parametrize(
    "other",
    [["--cursor-value", "3"], ["--full-refresh"], ["--limit", "5"]],
    ids=["cursor-value", "full-refresh", "limit"],
)
def test_range_flags_exclusion_applies_even_when_the_sync_has_no_backfill(
    project: Path, other: list[str]
) -> None:
    """The ordinary overrides stay valid for a plain sync, so the combination must still fail."""
    result = runner.invoke(app, ["run", "--select", "p", "--backfill-start", "5", *other])

    assert result.exit_code == 1
    assert "mutually exclusive" in " ".join(result.output.split())


@pytest.mark.parametrize(
    "selection",
    [["--select", "p"], ["--select", "b", "--select", "p"], []],
    ids=["plain-sync-only", "backfill-plus-plain", "all-syncs"],
)
def test_range_flags_need_exactly_one_selected_backfill_sync(
    project: Path, selection: list[str]
) -> None:
    result = runner.invoke(app, ["run", *selection, "--backfill-start", "5"])

    assert result.exit_code == 1
    assert "need exactly one selected sync" in " ".join(result.output.split())


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--backfill-start", "yesterday"], "must be an integer"),
        (["--backfill-end", "2026-01-01T00:00:00Z"], "must be an integer"),
        (["--backfill-start", "50", "--backfill-end", "50"], "must be before"),
        (["--backfill-start", "60", "--backfill-end", "10"], "must be before"),
    ],
)
def test_malformed_range_values_are_refused_before_any_extraction(
    project: Path, flags: list[str], message: str
) -> None:
    result = runner.invoke(app, ["run", "--select", "b", *flags])

    assert result.exit_code == 1
    assert message in " ".join(result.output.split())


def test_cursor_value_message_points_at_the_new_flags(project: Path) -> None:
    result = runner.invoke(app, ["run", "--select", "b", "--cursor-value", "3"])

    assert result.exit_code == 1
    assert "--backfill-start" in " ".join(result.output.split())


def test_conflicting_full_refresh_and_cursor_value_are_refused_first(project: Path) -> None:
    result = runner.invoke(app, ["run", "--select", "b", "--full-refresh", "--cursor-value", "3"])

    assert result.exit_code == 1
    assert "mutually exclusive" in " ".join(result.output.split())
