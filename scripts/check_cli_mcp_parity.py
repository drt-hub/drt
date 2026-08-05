#!/usr/bin/env python3
"""Verify that behaviour-changing CLI options reach the matching MCP tool.

Usage:
    python scripts/check_cli_mcp_parity.py [--porcelain]

Exit codes:
    0  Enumeration succeeded. Gaps (if any) are printed; the caller decides
       whether they are new drift or baselined.
    2  Script error — the CLI or MCP surface could not be introspected.
       This is deliberately NOT "no gaps": see `Failing loudly` below.

Called as check 9 of ``scripts/check_drift.sh``, which owns the
``scripts/drift_baseline.txt`` ratchet and the DRIFT/KNOWN reporting.
Run standalone for a human-readable listing.

Why this exists (#871)
----------------------
``check_drift.sh`` audits *connectors* and *MCP tool existence*, but nothing
checked that a CLI **option** reached MCP. Adding a connector was caught;
adding a flag was not. The v0.8.0 wave (``--limit``, ``--fail-fast``,
``--failed``, ``--vars``, ``--threads``) shipped without MCP parity and went
unnoticed for three releases (#870).

The release checklist already asks for this by hand
(``.claude/commands/drt-release-check.md``, Phase 0 item 12). It asks a human
to enumerate "new CLI capabilities" from memory at release time — exactly the
task that degrades when a release contains thirty PRs. This script does the
enumeration instead.

Failing loudly
--------------
A parity check that silently finds nothing is worse than no check: it reports
success forever while the gap it was written to catch grows. There is a real
way for that to happen here, and it was hit while writing this script.

Typer 0.27 vendors its own copy of Click, so ``TyperOption`` inherits
``typer._click.core.Parameter`` — **not** the installed ``click.Option``. The
obvious ``isinstance(param, click.Option)`` filter therefore matches nothing,
every command enumerates zero options, and the check passes with a clean bill
of health. So options are identified by duck-typing on ``param_type_name``,
and :func:`_assert_enumeration_sane` refuses to report success unless the
enumeration still finds known-present options. If Typer or Click changes shape
again, this exits 2 rather than quietly going green.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import sys

# --- Scope -----------------------------------------------------------------
#
# A CLI command is in scope when an MCP tool covers the same task. Mapping to
# a *tuple* of tools because the split is not always 1:1 — `drt status`
# corresponds to both `drt_get_status` and `drt_get_history`, and an option is
# satisfied by whichever tool exposes it.
COMMAND_TO_TOOLS: dict[str, tuple[str, ...]] = {
    "run": ("run_sync",),
    "test": ("run_test",),
    "validate": ("validate",),
    "status": ("get_status", "get_history"),
    "list": ("list_syncs",),
    "doctor": ("doctor",),
    "retry": ("retry", "dlq"),
    "sources": ("list_connectors",),
    "destinations": ("list_connectors",),
    "docs generate": ("get_manifest",),
    "profile list": ("list_profiles",),
    "profile test": ("test_profile",),
    # #776, landed alongside this check.
    "state show": ("state_show",),
    "state reset": ("state_reset",),
}

# Commands deliberately absent from MCP, with the reason. Kept explicit rather
# than implied by omission so that a NEW command fails this check until someone
# classifies it — that is the same "silently skipped" failure mode as #870, one
# level up. See _unclassified_commands().
UNMAPPED_COMMANDS: dict[str, str] = {
    "init": "scaffolds files on disk; an agent should not silently create a project",
    "serve": "runs a long-lived HTTP daemon — not a request/response tool",
    "mcp run": "starts the MCP server itself",
    "clean": "deletes local artifacts",
    "deploy github-actions": "writes workflow files into the repo",
    "cloud push": "drt Cloud stub, not yet a shipped capability",
    "cloud status": "drt Cloud stub, not yet a shipped capability",
    "config set": "mutates local telemetry config",
    "config unset": "mutates local telemetry config",
    "config show-telemetry": "prints local telemetry config",
    "docs serve": "runs a local web server",
    "build": "run + test in one pass; composed by calling drt_run_sync then drt_run_test",
    "profile add": "interactive prompt flow; writes credentials to disk",
    "profile remove": "destructive credential edit",
    "profile show": "prints a stored credential (masked) — deliberately not agent-reachable",
}

# --- Structural exclusions -------------------------------------------------
#
# Options that legitimately do not belong in MCP, per #870. These are NOT
# debt: they are correct absences, so they live here rather than in the
# baseline ratchet.
#
# Presentation — MCP returns structured data, so rendering flags are
# meaningless across the boundary. ``--format`` belongs here in every current
# use: `table|json` on the connector listings, and `html|mermaid|json` on
# `docs generate`, whose JSON artifact is precisely what drt_get_manifest
# returns.
PRESENTATION_OPTIONS = frozenset({"--output", "--log-format", "--verbose", "--quiet", "--format"})
# Selection — expressed by each tool's `sync_name` argument instead.
SELECTION_OPTIONS = frozenset({"--select", "--exclude"})

# Per-command structural exclusions, with the reason recorded inline.
COMMAND_EXCLUSIONS: dict[str, dict[str, str]] = {
    "status": {
        "--history": "the CLI flag switches mode; MCP splits it into drt_get_history",
    },
    "sources": {"--detailed": "drt_list_connectors always returns full detail"},
    "destinations": {"--detailed": "drt_list_connectors always returns full detail"},
    "validate": {
        "--emit-schema": "writes a schema file to disk; drt_get_schema returns it instead",
    },
    "docs generate": {
        "--inline": "HTML packaging (single-object bundling); MCP returns the manifest",
    },
    "state reset": {
        "--yes": (
            "skips a TTY confirmation prompt (#776); MCP has no TTY and "
            "drt_state_reset never prompts, so there is nothing to skip"
        ),
    },
}

# --- Naming ----------------------------------------------------------------
#
# Most options map to a tool parameter by the obvious transform
# (`--dry-run` -> `dry_run`). These do not, and a name-only check would report
# them as missing when they are in fact present under another name.
# ``--no-state`` is an *inverted* alias of ``include_state``: the capability is
# reachable, which is what parity means here, so the polarity flip is fine.
OPTION_ALIASES: dict[str, dict[str, str]] = {
    "run": {"--diff": "compute_diff", "--profile": "profile_name"},
    "status": {"--sync": "sync_name"},
    "docs generate": {"--no-state": "include_state"},
}

# Canary: options known to exist right now. If the enumeration stops finding
# these, introspection has broken and the run is not trustworthy.
ENUMERATION_CANARY: dict[str, str] = {
    "run": "--dry-run",
    "validate": "--strict",
}


def _is_option(param: object) -> bool:
    """True for a Click/Typer *option* (not a positional argument).

    Duck-typed on ``param_type_name`` rather than ``isinstance`` — see the
    module docstring for why an isinstance check silently matches nothing.
    """
    return getattr(param, "param_type_name", None) == "option"


def _long_name(param: object) -> str | None:
    """Return the longest declaration for a param, e.g. ``--select`` for ``-s/--select``."""
    opts = [o for o in getattr(param, "opts", []) if o.startswith("--")]
    return max(opts, key=len) if opts else None


def collect_cli_options() -> dict[str, set[str]]:
    """Return ``{command path: {--option, ...}}`` for every leaf CLI command.

    Deliberately imports no ``click``. Typer 0.27 vendors its own copy, so
    standalone Click is not a runtime dependency of a core ``pip install drt``
    — importing it here would make this script work locally (where dev extras
    pull Click in) and fail in the drift workflow. Typer's ``list_commands``
    and ``get_command`` ignore the context argument, so ``None`` is passed
    rather than constructing one.
    """
    import typer.main

    import drt.cli.main  # noqa: F401 — importing registers every command on the app
    from drt.cli._app import app

    def walk(command: object, prefix: str = "") -> dict[str, set[str]]:
        found: dict[str, set[str]] = {}
        for name in command.list_commands(None):  # type: ignore[attr-defined]
            sub = command.get_command(None, name)  # type: ignore[attr-defined]
            if sub is None:
                continue
            path = f"{prefix}{name}"
            if hasattr(sub, "list_commands"):
                found.update(walk(sub, f"{path} "))
                continue
            options = set()
            for param in sub.params:
                if not _is_option(param) or getattr(param, "hidden", False):
                    continue
                long = _long_name(param)
                if long and long != "--help":
                    options.add(long)
            found[path] = options
        return found

    return walk(typer.main.get_command(app))


def collect_mcp_parameters() -> dict[str, set[str]]:
    """Return ``{tool name: {parameter, ...}}`` for every MCP tool.

    Most modules hold exactly one tool function named after the module
    (``run_sync.py`` -> ``run_sync``), which a plain ``getattr(module, name)``
    finds. ``state.py`` is the first exception (#776): one module, two
    commands, two functions (``state_show``, ``state_reset``) — matching how
    ``server.py`` imports them. So every top-level function whose name is the
    module name or starts with ``f"{name}_"`` is collected, not just the exact
    match. Keyed by function name (== the MCP tool's ``drt_`` suffix), not
    module name, so both of ``state.py``'s tools get their own entry rather
    than one clobbering the other.
    """
    import drt.mcp.tools as tools_pkg

    found: dict[str, set[str]] = {}
    for module_info in pkgutil.iter_modules(tools_pkg.__path__):
        name = module_info.name
        module = importlib.import_module(f"drt.mcp.tools.{name}")
        for attr_name, obj in vars(module).items():
            if not inspect.isfunction(obj) or obj.__module__ != module.__name__:
                continue
            if attr_name != name and not attr_name.startswith(f"{name}_"):
                continue
            found[attr_name] = {p for p in inspect.signature(obj).parameters if p != "ctx"}
    return found


def _assert_enumeration_sane(cli: dict[str, set[str]], mcp: dict[str, set[str]]) -> None:
    """Exit 2 unless both surfaces enumerated plausibly. See module docstring."""
    problems = []
    if not cli:
        problems.append("no CLI commands found")
    if not mcp:
        problems.append("no MCP tools found")
    for command, option in ENUMERATION_CANARY.items():
        if option not in cli.get(command, set()):
            problems.append(f"canary missing: `drt {command} {option}` did not enumerate")
    # Only for commands the CLI actually has today — mirrors find_gaps()'s own
    # `cli.get(command, set())` gate. A COMMAND_TO_TOOLS entry may legitimately
    # be registered before its command lands (two PRs landing the CLI half and
    # the MCP half in either order, e.g. #776/#877): that pairing is not a
    # broken enumeration, it is an ordering choice, and the *command* being
    # absent from `cli` is what find_gaps() already treats as "not classified
    # yet" rather than "gap". The canary stays strict for anything the CLI can
    # currently see.
    for command, tools in sorted(COMMAND_TO_TOOLS.items()):
        if command not in cli:
            continue
        for tool in tools:
            if tool not in mcp:
                problems.append(f"mapped MCP tool not found: {tool}")
    if problems:
        print(
            "ERROR: CLI/MCP introspection looks broken, refusing to report success:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\nThis usually means Typer or Click changed shape, or a tool was "
            "renamed. Fix the enumeration rather than deleting the canary.",
            file=sys.stderr,
        )
        raise SystemExit(2)


def _unclassified_commands(cli: dict[str, set[str]]) -> list[str]:
    """Commands that are neither mapped to a tool nor explicitly unmapped."""
    return sorted(set(cli) - set(COMMAND_TO_TOOLS) - set(UNMAPPED_COMMANDS))


def _option_is_satisfied(command: str, option: str, mcp_params: set[str]) -> bool:
    """True if ``option`` is reachable through the mapped tool(s)."""
    alias = OPTION_ALIASES.get(command, {}).get(option)
    if alias is not None:
        return alias in mcp_params
    return option.lstrip("-").replace("-", "_") in mcp_params


def find_gaps(cli: dict[str, set[str]], mcp: dict[str, set[str]]) -> list[tuple[str, str]]:
    """Return ``(item, message)`` pairs for every parity gap found."""
    gaps: list[tuple[str, str]] = []

    for command in _unclassified_commands(cli):
        gaps.append(
            (
                f"{command}:*",
                f"`drt {command}` is new: add it to COMMAND_TO_TOOLS if an MCP tool "
                f"covers it, or to UNMAPPED_COMMANDS with the reason it does not",
            )
        )

    for command, tools in sorted(COMMAND_TO_TOOLS.items()):
        params: set[str] = set()
        for tool in tools:
            params |= mcp.get(tool, set())
        excluded = COMMAND_EXCLUSIONS.get(command, {})
        for option in sorted(cli.get(command, set())):
            if option in PRESENTATION_OPTIONS or option in SELECTION_OPTIONS:
                continue
            if option in excluded:
                continue
            if _option_is_satisfied(command, option, params):
                continue
            tool_list = " / ".join(f"drt_{t}" for t in tools)
            gaps.append(
                (
                    f"{command}:{option}",
                    f"`drt {command} {option}` has no parameter on {tool_list}",
                )
            )
    return gaps


def main() -> int:
    porcelain = "--porcelain" in sys.argv[1:]

    cli = collect_cli_options()
    mcp = collect_mcp_parameters()
    _assert_enumeration_sane(cli, mcp)

    gaps = find_gaps(cli, mcp)

    if porcelain:
        for item, message in gaps:
            print(f"{item}\t{message}")
        return 0

    print(f"== CLI/MCP parity: {len(cli)} commands, {len(mcp)} MCP tools ==")
    print(f"   {len(COMMAND_TO_TOOLS)} mapped, {len(UNMAPPED_COMMANDS)} deliberately unmapped")
    if not gaps:
        print("\nNo parity gaps.")
        return 0
    print(f"\n{len(gaps)} parity gap(s):")
    for item, message in gaps:
        print(f"  {item}\n      {message}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
