"""Tests for ``_print_watermark_summary`` (post-run watermark notes).

Covers the two branches that emit Rich console notes after a ``drt run``
finishes — ``default_value`` (first-run / fallback) and ``cli_override``
(``--cursor-value`` was used). Both branches were silent in coverage on
the original move (#573 Phase 2b PR (a)) because no end-to-end CLI test
exercised an actual successful sync with a watermark_source field set.
Calling the helper directly is the cheapest way to lock the contract
without rebuilding a full project fixture.
"""

from __future__ import annotations

from drt.cli.commands.run import _print_watermark_summary
from drt.cli.output import console


def test_print_watermark_summary_empty_results(capsys) -> None:  # type: ignore[no-untyped-def]
    """Empty results list emits nothing — both branches are skipped."""
    _print_watermark_summary([])
    out = capsys.readouterr().out
    assert out == ""


def test_print_watermark_summary_default_value_branch(capsys) -> None:  # type: ignore[no-untyped-def]
    """``watermark_source=default_value`` triggers the first-run yellow note."""
    with console.capture() as cap:
        _print_watermark_summary(
            [
                {"name": "post_users", "watermark_source": "default_value"},
                {"name": "post_orders", "watermark_source": "default_value"},
            ]
        )
    out = cap.get()
    assert "2 sync(s) used watermark.default_value" in out
    assert "post_users" in out
    assert "post_orders" in out


def test_print_watermark_summary_cli_override_branch(capsys) -> None:  # type: ignore[no-untyped-def]
    """``watermark_source=cli_override`` triggers the --cursor-value cyan note."""
    with console.capture() as cap:
        _print_watermark_summary([{"name": "post_users", "watermark_source": "cli_override"}])
    out = cap.get()
    assert "1 sync(s) used --cursor-value" in out
    assert "post_users" in out


def test_print_watermark_summary_both_branches(capsys) -> None:  # type: ignore[no-untyped-def]
    """Results mixing both watermark sources emit both notes."""
    with console.capture() as cap:
        _print_watermark_summary(
            [
                {"name": "post_users", "watermark_source": "default_value"},
                {"name": "post_orders", "watermark_source": "cli_override"},
            ]
        )
    out = cap.get()
    assert "1 sync(s) used watermark.default_value" in out
    assert "1 sync(s) used --cursor-value" in out


def test_print_watermark_summary_ignores_unknown_sources(capsys) -> None:  # type: ignore[no-untyped-def]
    """Results without a recognised watermark_source produce no output."""
    with console.capture() as cap:
        _print_watermark_summary(
            [
                {"name": "post_users"},  # no watermark_source key
                {"name": "post_orders", "watermark_source": "engine_state"},
            ]
        )
    assert cap.get() == ""
