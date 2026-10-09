"""``drt apply`` -- execute a saved plan only if the world still matches (#1217).

Verify-by-replan: the plan is recomputed through the same code ``drt plan``
uses and compared before anything is written. The plan's ``plan_id`` becomes
the apply run's ``run_id``, which is how a plan is recorded as applied (in run
history and ``run_results.json``) and why a plan can only be applied once.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import typer

from drt import __version__
from drt.cli._app import app
from drt.cli.output import console, print_error

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DRIFT_SHOWN = 10


def _parse_duration(text: str) -> timedelta:
    match = _DURATION.match(text.strip())
    if not match:
        raise typer.BadParameter("use a number and a unit: 30m, 24h, 7d")
    return timedelta(seconds=int(match.group(1)) * _UNITS[match.group(2)])


def _describe_drift(report: dict[str, list[dict[str, Any]]]) -> str:
    lines = ["The world no longer matches the plan:"]
    for label, items in (
        ("appeared since the plan", report["appeared"]),
        ("disappeared since the plan", report["disappeared"]),
        ("changed action since the plan", report["changed"]),
    ):
        if not items:
            continue
        lines.append(f"  {len(items)} key(s) {label}:")
        for item in items[:_DRIFT_SHOWN]:
            lines.append(f"    - {json.dumps(item.get('key'), default=str, sort_keys=True)}")
        if len(items) > _DRIFT_SHOWN:
            lines.append(f"    ... and {len(items) - _DRIFT_SHOWN} more")
    return "\n".join(lines)


@app.command()
def apply(
    plan_file: Path = typer.Argument(..., help="plan.json written by `drt plan --out`."),
    auto_approve: bool = typer.Option(
        False, "--auto-approve", help="Do not prompt (required without a terminal, e.g. CI)."
    ),
    max_age: str = typer.Option(
        "24h", "--max-age", help="Refuse a plan older than this (30m, 24h, 7d)."
    ),
    allow_drift_pct: float = typer.Option(
        0.0,
        "--allow-drift-pct",
        help="Proceed if at most this percent of the planned keys drifted (logged). Default 0.",
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
    from drt.cli._plan_runner import PlanCliError, compute_plan
    from drt.cli.commands.run import _run_one, _RunContext, _write_run_results
    from drt.engine.plan import PlanDocumentError, drift_report, load_plan_document

    if output not in ("text", "json"):
        print_error("--output must be 'text' or 'json'.")
        raise typer.Exit(1)
    if allow_drift_pct < 0 or allow_drift_pct > 100:
        print_error("--allow-drift-pct must be between 0 and 100.")
        raise typer.Exit(1)
    try:
        max_age_delta = _parse_duration(max_age)
    except typer.BadParameter as e:
        print_error(f"--max-age: {e.message}")
        raise typer.Exit(1)

    try:
        doc = load_plan_document(plan_file.read_text(encoding="utf-8"))
    except (OSError, PlanDocumentError) as e:
        print_error(f"Cannot apply {plan_file}: {e}")
        raise typer.Exit(1)

    plan_major = str(doc["drt_version"]).split(".")[0]
    if plan_major != __version__.split(".")[0]:
        print_error(
            f"The plan was made by drt {doc['drt_version']} and cannot be applied by "
            f"drt {__version__} (different major version). Re-run `drt plan`."
        )
        raise typer.Exit(1)

    try:
        created = datetime.fromisoformat(doc["created_at"])
    except ValueError:
        print_error("The plan's created_at is not a valid timestamp.")
        raise typer.Exit(1)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - created
    if age > max_age_delta:
        print_error(f"The plan is {age} old, older than --max-age {max_age}. Re-run `drt plan`.")
        raise typer.Exit(1)

    plan_id = doc["plan_id"]
    sync_name = doc["sync"]["name"]

    def preflight(project: Any, state_bundle: Any, _sync: Any) -> None:
        # Single use is recorded as a history entry whose run_id is the plan_id,
        # so it cannot be enforced without history.
        if not project.history.enabled:
            raise PlanCliError(
                "drt apply needs history enabled (history.enabled: true): an applied plan "
                "is recorded there, and without it a plan could be applied twice."
            )
        for entry in state_bundle.history.read(sync_name, limit=1000):
            if entry.run_id == plan_id:
                raise PlanCliError(
                    f"Plan {plan_id} was already applied ({entry.started_at}). "
                    "A plan is single-use; run `drt plan` again."
                )

    try:
        ctx_plan = compute_plan(
            sync_name,
            redact_keys=bool(doc["options"]["redact_keys"]),
            vars_raw=vars_raw,
            profile_name=profile_name,
            preflight=preflight,
        )
    except PlanCliError as e:
        print_error(str(e))
        raise typer.Exit(1)

    current = ctx_plan.plan
    if not current.available:
        print_error(f"Cannot verify the plan: {current.unavailable_reason}")
        raise typer.Exit(1)
    if current.config_hash != doc["fingerprints"]["config_hash"]:
        print_error(
            "The sync definition (or the model SQL it references) changed since the plan "
            "was made. Re-run `drt plan`."
        )
        raise typer.Exit(1)
    if current.cursor_hash != doc["fingerprints"]["cursor_hash"]:
        print_error(
            "The incremental watermark moved since the plan was made, so the plan covers a "
            "different window. Re-run `drt plan`."
        )
        raise typer.Exit(1)

    if current.digest != doc["digest"]:
        report = drift_report(doc["entries"], [e.to_dict() for e in current.entries])
        drifted = len(report["appeared"]) + len(report["disappeared"]) + len(report["changed"])
        pct = 100.0 * drifted / max(len(doc["entries"]), 1)
        if pct > allow_drift_pct:
            print_error(_describe_drift(report))
            console.print(
                f"  {drifted} key(s) drifted ({pct:.1f}% of the plan; allowed "
                f"{allow_drift_pct}%). Nothing was written.",
                markup=False,
            )
            raise typer.Exit(1)
        console.print(
            f"Proceeding despite drift: {drifted} key(s) ({pct:.1f}% <= {allow_drift_pct}%).",
            markup=False,
        )

    summary = current.summary
    parts = [f"{summary[a]} {a}" for a in summary if summary[a]]
    if not current.has_changes:
        console.print("The plan has no changes; nothing to apply.", markup=False)
        raise typer.Exit(0)
    if not auto_approve:
        if not typer.get_text_stream("stdin").isatty():
            print_error("--auto-approve is required without a terminal.")
            raise typer.Exit(1)
        console.print(
            f"Apply plan {plan_id} to {current.destination}: {', '.join(parts)}.", markup=False
        )
        if not typer.confirm("Apply?"):
            console.print("Aborted. Nothing was written.", markup=False)
            raise typer.Exit(1)

    project = ctx_plan.project
    state_bundle = ctx_plan.state_bundle
    from drt.cli._helpers import get_source

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    ctx = _RunContext(
        source=get_source(ctx_plan.profile),
        state_mgr=state_bundle.state,
        history_mgr=state_bundle.history,
        dlq_store=state_bundle.dlq,
        history_retention_days=project.history.retention_days,
        json_mode=output == "json",
        dry_run=False,
        verbose=False,
        quiet=False,
        log_json=False,
        cursor_value=None,
        vars=ctx_plan.project_vars,
        query_tagging=project.query_tagging,
        run_id=plan_id,
        idempotency_ledger=state_bundle.ledger,
        audit_trail=state_bundle.audit_trail,
        audit_fields=project.state.audit_trail.fields,
        audit_retain_days=project.state.audit_trail.retain_days or 30,
    )
    name, entry, had_error = _run_one(ctx_plan.sync, ctx, ctx_plan.profile)
    entry["plan_id"] = plan_id
    _write_run_results(
        Path("target/drt"),
        run_id=plan_id,
        started_at=started_at,
        results=[entry],
        succeeded=0 if had_error else 1,
        failed=1 if had_error else 0,
        skipped=0,
        total_duration=round(time.monotonic() - t0, 2),
        exit_code=1 if had_error else 0,
    )
    if output == "json":
        print(json.dumps({"plan_id": plan_id, "sync": name, "result": entry}, default=str))
    if had_error:
        raise typer.Exit(1)
