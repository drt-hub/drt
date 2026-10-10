"""``drt plan`` -- persist a reviewable, deterministic change set (#1216).

Runs the same read-only path as ``drt run --dry-run --diff`` but keeps the
*complete* key-level change set and writes it as ``plan.json``. It never writes
to the destination, and (like ``--dry-run``) never advances watermarks or
persists run state.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

if TYPE_CHECKING:
    from drt.engine.plan import Plan


@app.command()
def plan(
    sync_name: str = typer.Argument(None, help="Name of the sync to plan (or use --all)."),
    all_syncs: bool = typer.Option(
        False,
        "--all",
        help=(
            "Plan every sync (for CI). Writes one <sync>.json per plannable sync into "
            "--out-dir; a sync that cannot be planned is reported, not fatal."
        ),
    ),
    out_dir: Path = typer.Option(
        Path("plans"), "--out-dir", help="Where --all writes the per-sync plans."
    ),
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
      drt plan --all --out-dir plans --output markdown   # one PR comment for every sync
    """
    from drt.cli._plan_runner import PlanCliError, compute_plan, parse_vars_option
    from drt.engine.plan import render_markdown, render_text

    if output not in ("text", "json", "markdown"):
        print_error("--output must be 'text', 'json' or 'markdown'.")
        raise typer.Exit(1)

    if all_syncs or sync_name is None:
        if not all_syncs or sync_name is not None or out is not None:
            print_error("Give a sync name, or --all (not both, and --all uses --out-dir).")
            raise typer.Exit(1)
        _plan_all(
            out_dir, output, detailed_exitcode, redact_keys, cursor_value, vars_raw, profile_name
        )
        return

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


_FAILED = "could not be planned (see the job log for details)"


def _slug(name: str) -> str:
    import re

    return re.sub(r"[^A-Za-z0-9._-]", "_", name).strip(".") or "sync"


def _clear_previous_plans(out_dir: Path) -> None:
    """Remove the plan files a previous ``drt plan --all`` wrote here (named in its manifest)."""
    import json
    import re

    manifest = out_dir / "manifest.json"
    if not manifest.exists():
        return
    try:
        listed = [e.get("file") for e in json.loads(manifest.read_text()).get("syncs", [])]
    except (OSError, ValueError, AttributeError):
        listed = []
    for name in listed:
        if isinstance(name, str) and re.fullmatch(r"\d{3}-[A-Za-z0-9._-]+\.json", name):
            (out_dir / name).unlink(missing_ok=True)
    manifest.unlink(missing_ok=True)


def _plan_all(
    out_dir: Path,
    output: str,
    detailed_exitcode: bool,
    redact_keys: bool,
    cursor_value: str | None,
    vars_raw: str | None,
    profile_name: str | None,
) -> None:
    """``drt plan --all``: every sync, one combined report, never fatal per sync.

    A sync that is *unavailable* (its destination cannot report its contents) is a
    known limitation and is listed with its reason. A sync that *failed* to plan is
    an error: it is recorded in the manifest, its detail goes to stderr (never the
    report, which ends up in a pull-request comment), and the command exits 1 after
    writing everything, so a green run always means every sync was accounted for.
    """
    import json

    from drt.cli._plan_runner import PlanCliError, compute_plan, load_plan_key, parse_vars_option
    from drt.config.parser import load_project, load_syncs
    from drt.config.vars import VarError, resolve_vars
    from drt.engine.plan import build_manifest, render_text
    from drt.mcp._untrusted import message

    try:
        cli_vars = parse_vars_option(vars_raw)
        project = load_project(Path("."))
        names = [s.name for s in load_syncs(Path("."), vars=resolve_vars(project.vars, cli_vars))]
    except (PlanCliError, FileNotFoundError, VarError) as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not names:
        print_error("No syncs found in syncs/.")
        raise typer.Exit(1)

    plan_key = load_plan_key(Path("."))
    results: list[tuple[str, Plan | None, str, str | None]] = []  # name, plan, status, reason
    for name in names:
        try:
            one = compute_plan(
                name,
                redact_keys=redact_keys,
                cursor_value=cursor_value,
                cli_vars=cli_vars,
                profile_name=profile_name,
            ).plan
        except PlanCliError as e:
            typer.echo(f"drt plan: {name}: {message(str(e), plan_key, 800)}", err=True)
            results.append((name, None, "error", _FAILED))
            continue
        if one.available:
            results.append((name, one, "planned", None))
        else:
            results.append((name, None, "unavailable", one.unavailable_reason))

    _clear_previous_plans(out_dir)
    manifest_syncs: list[dict[str, object]] = []
    for index, (name, planned, status, reason) in enumerate(results, start=1):
        entry: dict[str, object] = {"sync": name, "status": status}
        if planned is not None:
            file_name = f"{index:03d}-{_slug(name)}.json"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / file_name).write_text(planned.to_json(), encoding="utf-8")
            entry["file"] = file_name
            entry["plan_id"] = planned.plan_id
        if reason:
            entry["reason"] = reason
        manifest_syncs.append(entry)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "manifest.json").write_text(
        json.dumps(build_manifest(manifest_syncs, plan_key), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    if output == "json":
        documents = [
            planned.to_dict()
            if planned
            else {"sync": {"name": name}, "status": {"available": False, "reason": reason}}
            for name, planned, _status, reason in results
        ]
        print(json.dumps({"plans": documents}, indent=2, default=str))
    elif output == "markdown":
        print(_markdown_report(results), end="")
    else:
        for name, planned, _status, reason in results:
            if planned is not None:
                console.print(render_text(planned), end="", markup=False)
            else:
                console.print(
                    f"Plan for sync '{name}'\n  Plan unavailable: {reason}\n", markup=False
                )
        planned_count = sum(1 for r in results if r[1] is not None)
        console.print(f"Wrote {planned_count} plan(s) to {out_dir}/", markup=False)

    if any(status == "error" for _n, _p, status, _r in results):
        raise typer.Exit(1)
    if detailed_exitcode and any(p is not None and p.has_changes for _n, p, _s, _r in results):
        raise typer.Exit(2)


def _markdown_report(results: list[tuple[str, Plan | None, str, str | None]]) -> str:
    """One comment for every sync. ``<!-- drt-plan -->`` lets CI update it in place.

    Names and reasons are data, so they go through the same inline-code renderer as
    keys (no headings, links or HTML can be forged by a sync name).
    """
    from drt.engine.plan import _code, render_markdown

    parts = ["<!-- drt-plan -->", "## drt plan", ""]
    changed = sum(1 for _n, p, _s, _r in results if p is not None and p.has_changes)
    failed = [n for n, _p, status, _r in results if status == "error"]
    parts.append(f"{len(results)} sync(s); {changed} with changes.")
    if failed:
        parts += [
            "",
            f"**{len(failed)} sync(s) could not be planned, so this run cannot be applied:** "
            + ", ".join(_code(n) for n in failed)
            + ". See the job log.",
        ]
    parts.append("")
    for name, planned, status, reason in results:
        if planned is not None:
            parts.append(render_markdown(planned))
        else:
            label = "Plan unavailable." if status == "unavailable" else "Plan failed."
            parts.append(
                f"### drt plan: {_code(name)}\n\n**{label}** "
                f"{_code(reason or 'no reason given', 500)}\n"
            )
    return "\n".join(parts)
