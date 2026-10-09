"""``drt apply`` -- execute a saved plan only if the world still matches (#1217).

A thin wrapper: the verification, the single-use claim and the write live in
``drt/cli/_apply_flow.py`` so the MCP ``drt_apply`` tool runs the same code.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error


@app.command()
def apply(
    plan_file: Path = typer.Argument(..., help="plan.json written by `drt plan --out`."),
    auto_approve: bool = typer.Option(
        False, "--auto-approve", help="Do not prompt (required without a terminal, e.g. CI)."
    ),
    approved_by: str = typer.Option(
        None,
        "--approved-by",
        help="Who approved this apply; recorded in the run results and the plan's claim.",
    ),
    max_age: str = typer.Option(
        "24h", "--max-age", help="Refuse a plan older than this (30m, 24h, 7d)."
    ),
    allow_drift_pct: float = typer.Option(
        0.0,
        "--allow-drift-pct",
        help="Proceed if at most this percent of the planned keys drifted (logged). Default 0.",
    ),
    force_guards: bool = typer.Option(
        False,
        "--force-guards",
        help="Apply even though a sync.guards limit trips (recorded in the run results).",
    ),
    cursor_value: str = typer.Option(
        None,
        "--cursor-value",
        help="The cursor override the plan was made with (`drt plan --cursor-value`).",
    ),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    vars_raw: str = typer.Option(None, "--vars", help="Override project vars, as for `drt run`."),
    profile_name: str = typer.Option(None, "--profile", "-p", help="Override profile."),
) -> None:
    """Apply a plan only if recomputing it gives the same change set.

    Nothing is written when the plan is stale, edited, from another drt major
    version, already applied, or when the world drifted since it was made.
    The write itself is the normal `drt run` path (rate limiting, DLQ,
    history, watermarks, alerts).

    Examples:
      drt plan orders_to_pg --out plan.json
      drt apply plan.json
      drt apply plan.json --auto-approve --max-age 2h   # in CI
    """
    from drt.cli._apply_flow import ApplyAborted, ApplyRefused, apply_plan, parse_duration
    from drt.cli._plan_runner import PlanCliError, parse_vars_option

    if output not in ("text", "json"):
        print_error("--output must be 'text' or 'json'.")
        raise typer.Exit(1)
    try:
        max_age_delta = parse_duration(max_age)
    except ValueError as e:
        print_error(f"--max-age: {e}")
        raise typer.Exit(1)
    try:
        plan_text = plan_file.read_text(encoding="utf-8")
        cli_vars = parse_vars_option(vars_raw)
    except OSError as e:
        print_error(f"Cannot apply {plan_file}: {e}")
        raise typer.Exit(1)
    except PlanCliError as e:
        print_error(str(e))
        raise typer.Exit(1)

    def confirm(question: str) -> bool:
        if not typer.get_text_stream("stdin").isatty():
            raise ApplyRefused("--auto-approve is required without a terminal.")
        console.print(question, markup=False)
        return typer.confirm("Apply?")

    try:
        outcome = apply_plan(
            plan_text,
            approved=auto_approve,
            approved_by=approved_by,
            confirm=confirm,
            max_age=max_age_delta,
            allow_drift_pct=allow_drift_pct,
            force_guards=force_guards,
            cursor_value=cursor_value,
            cli_vars=cli_vars,
            profile_name=profile_name,
            json_output=output == "json",
            # stderr, so `--output json` stays one parseable document on stdout.
            notify=lambda message: typer.echo(message, err=True),
        )
    except ApplyRefused as e:
        if isinstance(e, ApplyAborted):  # the person declined: not an error
            console.print(str(e), markup=False)
        else:
            print_error(str(e))
        raise typer.Exit(1)

    if not outcome.applied:
        console.print(outcome.message, markup=False)
        raise typer.Exit(0)
    if output == "json":
        print(
            json.dumps(
                {
                    "plan_id": outcome.plan_id,
                    "run_id": outcome.run_id,
                    "sync": outcome.sync,
                    "result": outcome.entry,
                },
                default=str,
            )
        )
    if outcome.had_error:
        raise typer.Exit(1)
