"""``drt retry`` releases the destination's write state like ``run_sync`` does.

A destination that keeps a client across ``load()`` calls (Kafka's producer) is
closed by ``reset_write_state``. ``drt retry`` drives ``load()`` itself, so it
has to call that hook too — but never ``finalize_sync``, which performs the
end-of-run work (mirror deletes, swap cutover, managed-table DDL).
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
from drt.state.dlq import DeadLetter, DlqStore

runner = CliRunner()


class _Destination:
    def __init__(self, *, load_raises: bool = False, reset_raises: bool = False) -> None:
        self.load_raises = load_raises
        self.reset_raises = reset_raises
        self.events: list[str] = []

    def load(
        self, records: list[dict[str, Any]], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        self.events.append("load")
        if self.load_raises:
            raise RuntimeError("broker down")
        return SyncResult(success=len(records))

    def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> None:
        self.events.append("finalize_sync")

    def reset_write_state(self, config: DestinationConfig, sync_options: SyncOptions) -> None:
        self.events.append("reset_write_state")
        if self.reset_raises:
            raise RuntimeError("close failed")


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
        "post_users", [DeadLetter(record={"id": i}, error_message="boom") for i in (1, 2)]
    )
    return tmp_path


def _use(monkeypatch: pytest.MonkeyPatch, dest: _Destination) -> None:
    monkeypatch.setattr(helpers, "get_destination", lambda sync: dest)


def test_retry_resets_write_state_but_never_finalizes(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = _Destination()
    _use(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert dest.events == ["load", "reset_write_state"]


def test_retry_resets_write_state_when_load_raises(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = _Destination(load_raises=True)
    _use(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code != 0
    assert dest.events == ["load", "reset_write_state"]


def test_a_failing_reset_does_not_fail_the_retry(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = _Destination(reset_raises=True)
    _use(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded" in result.output


def test_dry_run_never_builds_or_resets_a_destination(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = _Destination()
    _use(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert dest.events == []
