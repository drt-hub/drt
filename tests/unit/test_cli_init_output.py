"""Tests drt init success output."""

from __future__ import annotations

import pytest

from drt.cli.output import print_init_success


def test_print_init_success_shows_next_steps(capsys: pytest.CaptureFixture[str]) -> None:
    paths = ["drt_project.yml", "syncs/example_sync.yml"]

    print_init_success(paths)

    out = capsys.readouterr().out
    assert "Next steps:" in out
    for p in paths:
        assert p in out
    assert "drt validate" in out
    assert "drt run --dry-run" in out
    assert "Docs" in out
