"""``drt retry`` must not delete dead letters a destination never attempted.

A destination that stops after a failure (``on_error: fail``) reports the rest
of the batch as ``skipped`` without naming those rows. Treating every
non-failed row as delivered silently dropped records that were never sent.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

import drt.cli._helpers as helpers
from drt.cli.main import app
from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.state.dlq import DeadLetter, DlqStore

runner = CliRunner()


class _StopsAfterFirstFailure:
    """Fails record 0, reports the remaining records as skipped (never attempted)."""

    def __init__(self, *, skipped_no_match: int = 0) -> None:
        self.skipped_no_match = skipped_no_match

    def load(
        self, records: list[dict[str, Any]], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        result = SyncResult(failed=1, skipped=len(records) - 1)
        result.skipped_no_match = self.skipped_no_match
        result.row_errors.append(
            RowError(batch_index=0, record_preview="{}", error_message="boom", http_status=500)
        )
        result.errors.append("boom")
        return result


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 10, "dlq": {"enabled": True}},
            }
        )
    )
    DlqStore(tmp_path).append(
        "post_users", [DeadLetter(record={"id": i}, error_message="old") for i in (1, 2, 3)]
    )
    return tmp_path


def test_records_the_destination_never_attempted_stay_queued(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(helpers, "get_destination", lambda sync: _StopsAfterFirstFailure())

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    remaining = DlqStore(project).read("post_users")
    assert [e.record["id"] for e in remaining] == [1, 2, 3]
    unattempted = [e for e in remaining if e.record["id"] != 1]
    assert all("not attempted" in e.error_message for e in unattempted)


def test_a_policy_skip_that_is_explained_is_still_removed(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Skips accounted for by match_policy are deliberate; they leave the queue as before."""
    monkeypatch.setattr(
        helpers, "get_destination", lambda sync: _StopsAfterFirstFailure(skipped_no_match=2)
    )

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    remaining = DlqStore(project).read("post_users")
    assert [e.record["id"] for e in remaining] == [1]
