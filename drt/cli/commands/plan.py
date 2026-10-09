"""``drt plan`` -- persist a reviewable, deterministic change set (#1216).

Runs the same read-only path as ``drt run --dry-run --diff`` but keeps the
*complete* key-level change set and writes it as ``plan.json``. It never writes
to the destination, and (like ``--dry-run``) never advances watermarks or
persists run state.
"""

from __future__ import annotations

from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error


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
    from drt.cli._plan_runner import PlanCliError, compute_plan, parse_vars_option
    from drt.engine.plan import render_markdown, render_text

    if output not in ("text", "json", "markdown"):
        print_error("--output must be 'text', 'json' or 'markdown'.")
        raise typer.Exit(1)

    try:
        plan_obj = compute_plan(
            sync_name,
            redact_keys=redact_keys,
            cursor_value=cursor_value,
            cli_vars=parse_vars_option(vars_raw),
            profile_name=profile_name,
        ).plan
    except PlanCliError as e:
        print_error(str(e))
        raise typer.Exit(1)

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
