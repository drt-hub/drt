"""``drt plan`` -- persist a reviewable, deterministic change set (#1216).

Runs the same read-only path as ``drt run --dry-run --diff`` but keeps the
*complete* key-level change set and writes it as ``plan.json``. It never writes
to the destination, and (like ``--dry-run``) never advances watermarks or
persists run state.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import typer

from drt import __version__
from drt.cli._app import app
from drt.cli._helpers import (
    get_destination,
    get_source,
    get_watermark_storage,
    resolve_profile_name,
)
from drt.cli.output import console, print_error

if TYPE_CHECKING:
    from drt.config.models import SyncConfig


@app.command()
def plan(
    sync_name: str = typer.Argument(..., help="Name of the sync to plan."),
    out: Path | None = typer.Option(
        None, "--out", help="Write the plan to this file (plan.json). Omit to only summarize."
    ),
    output: str = typer.Option(
        "text",
        "--output",
        "-o",
        help="Stdout format: text (summary), json (the full plan) or markdown (PR comment).",
    ),
    detailed_exitcode: bool = typer.Option(
        False,
        "--detailed-exitcode",
        help="Exit 0 when there are no changes, 2 when changes are present (1 stays an error).",
    ),
    redact_keys: bool = typer.Option(
        False,
        "--redact-keys",
        help=(
            "Hash key values in the plan instead of showing them "
            "(sync.mask columns are always hashed)."
        ),
    ),
    cursor_value: str = typer.Option(
        None, "--cursor-value", help="Override the cursor/watermark for incremental syncs."
    ),
    vars_raw: str = typer.Option(None, "--vars", help="Override project vars, as for `drt run`."),
    profile_name: str = typer.Option(None, "--profile", "-p", help="Override profile."),
) -> None:
    """Compute what a sync *would* change, as a reviewable artifact.

    Read-only: nothing is written to the destination, no watermark advances,
    no run state is persisted. A destination that cannot report its current
    contents yields "plan unavailable" with a reason (exit 1) instead of a
    partial plan.

    Examples:
      drt plan orders_to_pg --out plan.json
      drt plan orders_to_pg --out plan.json --detailed-exitcode
    """
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.config.vars import VarError, parse_cli_vars, resolve_vars
    from drt.engine.plan import build_plan, render_markdown, render_text, unsupported_reason
    from drt.engine.sync import run_sync
    from drt.state.factory import build_state_bundle

    if output not in ("text", "json", "markdown"):
        print_error("--output must be 'text', 'json' or 'markdown'.")
        raise typer.Exit(1)

    try:
        project = load_project(Path("."))
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    try:
        profile = load_profile(resolve_profile_name(profile_name, project.profile))
    except (FileNotFoundError, KeyError, ValueError) as e:
        print_error(str(e))
        raise typer.Exit(1)

    try:
        cli_vars = parse_cli_vars(vars_raw) if vars_raw else None
        project_vars = resolve_vars(project.vars, cli_vars)
        syncs = load_syncs(Path("."), vars=project_vars)
    except VarError as e:
        print_error(str(e))
        raise typer.Exit(1)

    matches = [s for s in syncs if s.name == sync_name]
    if not matches:
        print_error(f"Sync '{sync_name}' not found in syncs/.")
        raise typer.Exit(1)
    sync = matches[0]

    blocker = unsupported_reason(sync)
    if blocker is not None:
        print_error(f"Plan unavailable for '{sync.name}': {blocker}.")
        raise typer.Exit(1)

    state_bundle = build_state_bundle(project, Path("."))
    try:
        result = run_sync(
            sync,
            get_source(profile),
            get_destination(sync),
            profile,
            Path("."),
            True,
            state_bundle.state,
            watermark_storage=get_watermark_storage(sync, Path(".")),
            cursor_value_override=cursor_value if sync.sync.mode == "incremental" else None,
            compute_diff=True,
            # Not a sample: `drt plan` needs every changed key.
            diff_limit=sys.maxsize,
            vars=project_vars,
            query_tagging=project.query_tagging,
        )
    except Exception as e:
        print_error(f"Could not compute a plan for '{sync_name}': {e}")
        raise typer.Exit(1)

    if result.diff is None:
        print_error(f"No diff was produced for '{sync_name}'.")
        raise typer.Exit(1)
    if result.failed or result.interrupted:
        # A diff over a partial extraction would read as "nothing else changes".
        print_error(
            f"Plan unavailable for '{sync_name}': extraction did not complete "
            f"({result.failed} failed row(s)"
            f"{', interrupted' if result.interrupted else ''})."
        )
        raise typer.Exit(1)

    destination = sync.destination
    plan_obj = build_plan(
        result.diff,
        sync_name=sync.name,
        sync_mode=sync.sync.mode,
        match_policy=sync.sync.match_policy,
        destination=getattr(destination, "describe_safe", lambda: str(destination.type))(),
        config_fingerprint=_fingerprint(sync),
        drt_version=__version__,
        key_columns=list(getattr(destination, "upsert_key", None) or []),
        mask_columns=set(sync.sync.mask or {}),
        redact_keys=redact_keys,
        cursor_value=result.cursor_value_used,
    )

    if not plan_obj.available:
        if output == "markdown":
            print(render_markdown(plan_obj), end="")
        else:
            console.print(render_text(plan_obj), end="", markup=False)
        raise typer.Exit(1)

    if out is not None:
        out.write_text(plan_obj.to_json(), encoding="utf-8")

    if output == "json":
        print(plan_obj.to_json(), end="")
    elif output == "markdown":
        print(render_markdown(plan_obj), end="")
    else:
        console.print(render_text(plan_obj), end="", markup=False)
        if out is not None:
            console.print(f"  Wrote {out}", markup=False)

    if detailed_exitcode:
        raise typer.Exit(2 if plan_obj.has_changes else 0)


def _fingerprint(sync: SyncConfig) -> str:
    """Hash of the sync file *and* the model SQL it references (#772's fingerprint)."""
    from drt.config.fingerprint import sync_fingerprints
    from drt.engine.plan import config_hash

    from_files = sync_fingerprints(Path(".")).get(sync.name)
    return f"sha256:{from_files}" if from_files else config_hash(sync)
