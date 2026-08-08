"""Tests for ``scripts/check_cli_mcp_parity.py`` (check 9 of the drift audit, #871)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load the script as a module — it lives outside ``drt/`` so an import path
# rewrite is the simplest way to make it testable.
_SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "check_cli_mcp_parity.py"
_spec = importlib.util.spec_from_file_location("check_cli_mcp_parity", _SCRIPT)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


# ---------------------------------------------------------------------------
# Enumeration
#
# These assert against the real CLI and MCP surfaces rather than fixtures: the
# whole value of this check is that it tracks reality, and a fixture would go
# stale in exactly the way the check exists to prevent.
# ---------------------------------------------------------------------------


def test_cli_enumeration_finds_options_on_a_known_command() -> None:
    """The vendored-Click trap: an isinstance-based filter yields zero here."""
    cli = mod.collect_cli_options()
    assert "--dry-run" in cli["run"]
    assert "--limit" in cli["run"]
    assert len(cli["run"]) > 10


def test_cli_enumeration_descends_into_subcommand_groups() -> None:
    cli = mod.collect_cli_options()
    assert "profile test" in cli
    assert "docs generate" in cli
    # The group itself is not a leaf command and must not be reported as one.
    assert "profile" not in cli


def test_cli_enumeration_excludes_help_and_positional_arguments() -> None:
    cli = mod.collect_cli_options()
    assert "--help" not in cli["run"]
    # `drt profile test NAME` takes a positional argument and no options.
    assert cli["profile test"] == set()


def test_mcp_enumeration_finds_tool_parameters() -> None:
    mcp = mod.collect_mcp_parameters()
    assert "run_sync" in mcp
    assert {"sync_name", "dry_run", "compute_diff"} <= mcp["run_sync"]
    # ``ctx`` is plumbing, not a user-facing parameter.
    assert "ctx" not in mcp["run_sync"]


def test_mcp_enumeration_finds_every_function_in_a_multi_tool_module() -> None:
    """``state.py`` is the first module holding more than one tool function
    (#776) — ``getattr(module, module_name)`` finds neither, since the
    functions are ``state_show`` / ``state_reset``, not ``state``. Regression
    guard for the near-miss this shape caused: the sanity check exited 2 with
    ``mapped MCP tool not found: state_reset`` until collect_mcp_parameters()
    was widened from an exact-name match to "the module name or
    ``f'{name}_'``-prefixed".

    Skips until #776 lands ``drt/mcp/tools/state.py`` — this branch adds the
    generalisation and its COMMAND_TO_TOOLS/COMMAND_EXCLUSIONS entries ahead
    of the module existing, so whichever of #776/#877 merges first does not
    red-build waiting on the other. Once merged, this stops skipping and
    starts asserting for real; nothing else needs to change."""
    pytest.importorskip("drt.mcp.tools.state")
    mcp = mod.collect_mcp_parameters()
    assert "state_show" in mcp
    assert "state_reset" in mcp
    assert {"watermark", "runs", "tracked_mirror", "dry_run"} <= mcp["state_reset"]


# ---------------------------------------------------------------------------
# The false-green guard
# ---------------------------------------------------------------------------


def test_guard_rejects_an_empty_cli_enumeration() -> None:
    """A check that finds nothing must fail, not report a clean audit."""
    empty = {command: set() for command in mod.COMMAND_TO_TOOLS}
    with pytest.raises(SystemExit) as excinfo:
        mod._assert_enumeration_sane(empty, mod.collect_mcp_parameters())
    assert excinfo.value.code == 2


def test_guard_rejects_a_renamed_mcp_tool() -> None:
    mcp = mod.collect_mcp_parameters()
    del mcp["run_sync"]
    with pytest.raises(SystemExit) as excinfo:
        mod._assert_enumeration_sane(mod.collect_cli_options(), mcp)
    assert excinfo.value.code == 2


def test_guard_passes_on_the_real_surfaces() -> None:
    mod._assert_enumeration_sane(mod.collect_cli_options(), mod.collect_mcp_parameters())


# ---------------------------------------------------------------------------
# Gap detection
# ---------------------------------------------------------------------------


def test_every_cli_command_is_classified() -> None:
    """A new command must force a decision rather than being silently skipped.

    This is #870's failure mode one level up: an unclassified command would
    contribute no gaps at all, so the check would pass while ignoring it.
    """
    assert mod._unclassified_commands(mod.collect_cli_options()) == []


def test_a_new_unclassified_command_is_reported() -> None:
    cli = dict(mod.collect_cli_options())
    cli["frobnicate"] = {"--wibble"}
    gaps = dict(mod.find_gaps(cli, mod.collect_mcp_parameters()))
    assert "frobnicate:*" in gaps
    assert "COMMAND_TO_TOOLS" in gaps["frobnicate:*"]


def test_renamed_option_is_not_reported_as_missing() -> None:
    """``--diff`` reaches MCP as ``compute_diff``; a name-only check false-positives."""
    assert mod._option_is_satisfied("run", "--diff", {"compute_diff"})
    assert not mod._option_is_satisfied("run", "--diff", {"diff"})


def test_inverted_alias_counts_as_reachable() -> None:
    """``docs generate --no-state`` is ``include_state`` with the polarity flipped."""
    assert mod._option_is_satisfied("docs generate", "--no-state", {"include_state"})


def test_plain_option_maps_by_the_obvious_transform() -> None:
    assert mod._option_is_satisfied("run", "--cursor-value", {"cursor_value"})
    assert not mod._option_is_satisfied("run", "--threads", {"cursor_value"})


def test_presentation_and_selection_options_are_never_reported() -> None:
    gaps = dict(mod.find_gaps(mod.collect_cli_options(), mod.collect_mcp_parameters()))
    for excluded in (
        "--output",
        "--verbose",
        "--quiet",
        "--log-format",
        "--select",
        "--exclude",
        "--state",
    ):
        assert f"run:{excluded}" not in gaps


def test_known_870_gaps_are_detected() -> None:
    """The flags #870 documents must actually be found, or the check is useless."""
    gaps = dict(mod.find_gaps(mod.collect_cli_options(), mod.collect_mcp_parameters()))
    for item in (
        "run:--limit",
        "run:--fail-fast",
        "run:--failed",
        "run:--vars",
        "run:--threads",
        "validate:--check-connection",
        "validate:--strict",
        "test:--store-failures",
    ):
        assert item in gaps, f"{item} should be reported until #870 lands"


def test_gaps_are_all_baselined_so_the_audit_is_green() -> None:
    """Every gap reported today must be recorded in the ratchet.

    Keeps the two files honest with each other: fixing a flag in #870 without
    deleting its baseline line, or vice versa, fails here.
    """
    baseline = (_SCRIPT.parent / "drift_baseline.txt").read_text().splitlines()
    recorded = {line for line in baseline if line.startswith("cli-mcp:")}
    gaps = mod.find_gaps(mod.collect_cli_options(), mod.collect_mcp_parameters())
    assert {f"cli-mcp:{item}" for item, _ in gaps} == recorded
