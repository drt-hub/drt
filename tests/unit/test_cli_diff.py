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


def test_diff_with_dry_run_runs(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--dry-run --diff completes successfully on a non-queryable destination."""
    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.engine import diff as diff_mod
    from drt.engine import sync as sync_module

    class _FakeResult:
        success = 1
        failed = 0
        skipped = 0
        skipped_no_match = 0
        rows_extracted = 1
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        watermark_lag: str | None = None
        limit_applied: int | None = None
        duration_seconds = 0.01
        interrupted = False
        run_id: str | None = None
        sync_run_id: str | None = "fake-sync-run-id"
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
        creds,
        "load_profile",
        lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
        raising=False,
    )
    monkeypatch.setattr(cli_main, "_get_source", lambda *_a, **_k: object(), raising=False)
    monkeypatch.setattr(cli_main, "_get_destination", lambda *_a, **_k: object(), raising=False)

    result = runner.invoke(app, ["run", "--dry-run", "--diff"])
    assert result.exit_code == 0
    # The fallback reason should appear in the rendered preview
    assert "no comparison available" in result.output


def test_diff_json_mode_embeds_diff(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
        skipped_no_match = 0
        rows_extracted = 5
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        watermark_lag: str | None = None
        limit_applied: int | None = None
        duration_seconds = 0.01
        interrupted = False
        run_id: str | None = None
        sync_run_id: str | None = "fake-sync-run-id"
        diff: Any = sample_diff

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
        return _FakeResult()

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(
        creds,
        "load_profile",
        lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
        raising=False,
    )
    monkeypatch.setattr(cli_main, "_get_source", lambda *_a, **_k: object(), raising=False)
    monkeypatch.setattr(cli_main, "_get_destination", lambda *_a, **_k: object(), raising=False)

    result = runner.invoke(app, ["run", "--dry-run", "--diff", "--output", "json"])
    assert result.exit_code == 0
    payload = _json.loads(result.output)
    sync_entry = payload["syncs"][0]
    assert "diff" in sync_entry
    assert sync_entry["diff"]["supported"] is False
    assert sync_entry["diff"]["fallback_reason"]
    assert sync_entry["diff"]["sample"] == [{"id": 1, "msg": "ping"}]


# ---------------------------------------------------------------------------
# Delete-provenance rendering (#693, Task B2)
#
# ``replace`` drops rows because the table is rebuilt; tracked ``mirror`` issues
# real DELETE statements against rows the destination keeps otherwise. Same
# ``deleted`` list, very different blast radius — the renderers must say which.
# The replace-mode output is the pre-existing contract and stays byte-identical.
# ---------------------------------------------------------------------------


def _rendered(diff: Any) -> str:
    """Render *diff* through ``print_diff_table`` and return the plain text."""
    from drt.cli.output import console, print_diff_table

    with console.capture() as cap:
        print_diff_table(diff, "sync_a")
    return cap.get()


def _deleting_diff(delete_reason: str | None) -> Any:
    from drt.engine import diff as diff_mod

    return diff_mod.DiffResult(
        added=[{"id": 1, "name": "Alice"}],
        deleted=[{"id": 9}],
        total_source_rows=1,
        total_destination_rows=2,
        supported=True,
        delete_reason=delete_reason,
    )


def test_print_diff_table_replace_delete_label_unchanged() -> None:
    """Replace-mode deletes keep the original ``- Deleted (N):`` label.

    This is the byte-level regression guard: the pre-#693 renderer emitted
    exactly this line, and existing users' output must not shift.
    """
    out = _rendered(_deleting_diff("replace"))

    assert "- Deleted (1):" in out
    assert "- id=9" in out
    assert "mirror" not in out.lower()


def test_print_diff_table_mirror_delete_is_labelled() -> None:
    """Mirror deletes are labelled as DELETE statements, not a rebuild."""
    out = _rendered(_deleting_diff("mirror"))

    assert "- Deleted (1, mirror DELETE):" in out
    assert "- id=9" in out
    # The state table stores keys only, so the preview rows are key-only —
    # say so, or the missing columns read as data loss.
    assert "key columns only" in out


def test_print_diff_table_destination_mirror_delete_names_the_extra_read() -> None:
    """The destination strategy's preview cost a destination read — say so.

    Same blast radius as tracked mirror, but the user paid a round trip to learn
    it. A label identical to ``"mirror"`` would hide that; the note makes the
    cost legible next to the numbers it bought.
    """
    out = _rendered(_deleting_diff("mirror_scan"))

    assert "- Deleted (1, mirror DELETE):" in out
    assert "- id=9" in out
    assert "key columns only" in out
    # The distinguishing part: where the delete set came from.
    assert "destination" in out.lower()


def test_print_diff_table_tracked_mirror_does_not_claim_a_destination_read() -> None:
    """The tracked label must stay free of the destination-read note (#833)."""
    out = _rendered(_deleting_diff("mirror"))

    assert "- Deleted (1, mirror DELETE):" in out
    assert "read from destination" not in out.lower()


def test_print_diff_table_unlabelled_delete_falls_back_to_plain() -> None:
    """A DiffResult with deletes but no reason renders the legacy label.

    Guards the dataclass default: any caller constructing a ``DiffResult``
    without the new field keeps the pre-#693 rendering.
    """
    out = _rendered(_deleting_diff(None))

    assert "- Deleted (1):" in out
    assert "mirror" not in out.lower()


def test_diff_to_dict_exposes_delete_reason() -> None:
    """``delete_reason`` rides alongside ``deleted`` without reshaping it."""
    from drt.cli.output import diff_to_dict

    replace_payload = diff_to_dict(_deleting_diff("replace"))
    mirror_payload = diff_to_dict(_deleting_diff("mirror"))

    # The existing public keys keep their exact shape...
    assert replace_payload["deleted"] == [{"id": 9}]
    assert mirror_payload["deleted"] == [{"id": 9}]
    # ...and provenance is a new sibling key.
    assert replace_payload["delete_reason"] == "replace"
    assert mirror_payload["delete_reason"] == "mirror"


def test_diff_to_dict_distinguishes_destination_strategy_mirror() -> None:
    """The two mirror strategies are separable in JSON, not collapsed.

    A consumer budgeting dry-run cost needs to tell the state-table preview from
    the one that reads the destination.
    """
    from drt.cli.output import diff_to_dict

    payload = diff_to_dict(_deleting_diff("mirror_scan"))

    assert payload["deleted"] == [{"id": 9}]
    assert payload["delete_reason"] == "mirror_scan"


def test_diff_to_dict_delete_reason_none_when_nothing_deleted() -> None:
    """No deletes → ``delete_reason`` is null rather than absent."""
    from drt.cli.output import diff_to_dict
    from drt.engine import diff as diff_mod

    payload = diff_to_dict(
        diff_mod.DiffResult(added=[{"id": 1}], total_source_rows=1, supported=True)
    )

    assert payload["deleted"] == []
    assert payload["delete_reason"] is None
    assert payload["delete_preview_unavailable_reason"] is None


def test_delete_preview_unavailable_is_distinct_from_zero_deletes() -> None:
    """Text and JSON must not present an unknown delete set as zero deletes."""
    from drt.cli.output import diff_to_dict
    from drt.engine import diff as diff_mod

    zero = diff_mod.DiffResult(
        added=[{"id": 1}],
        total_source_rows=1,
        supported=True,
    )
    unavailable = diff_mod.DiffResult(
        added=[{"id": 1}],
        total_source_rows=1,
        supported=True,
        delete_preview_unavailable_reason="PermissionError: SELECT denied",
    )

    zero_payload = diff_to_dict(zero)
    unavailable_payload = diff_to_dict(unavailable)
    assert zero_payload["deleted"] == unavailable_payload["deleted"] == []
    assert zero_payload["delete_preview_unavailable_reason"] is None
    assert unavailable_payload["delete_preview_unavailable_reason"] == (
        "PermissionError: SELECT denied"
    )

    zero_text = _rendered(zero)
    unavailable_text = _rendered(unavailable)
    assert "preview unavailable" not in zero_text
    assert "Deleted (mirror DELETE): preview unavailable" in unavailable_text
    assert "PermissionError: SELECT denied" in unavailable_text


def test_diff_to_dict_unsupported_shape_has_no_delete_reason() -> None:
    """The fallback (sample-mode) payload is untouched — no diff, no deletes."""
    from drt.cli.output import diff_to_dict
    from drt.engine import diff as diff_mod

    payload = diff_to_dict(
        diff_mod.DiffResult(
            sample=[{"id": 1}],
            total_source_rows=1,
            supported=False,
            fallback_reason="rest_api: no comparison available",
        )
    )

    assert payload["supported"] is False
    assert "delete_reason" not in payload
