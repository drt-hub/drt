"""The guarded apply, as a function (#1217, #1220).

``drt apply`` and the MCP ``drt_apply`` tool both call :func:`apply_plan`, so a
plan is verified, claimed and written the same way whichever door it comes
through. This module prints nothing and never exits: a refusal is an
:class:`ApplyRefused`, and the caller decides how to show it.

Verify-by-replan: the plan is recomputed through the same code ``drt plan``
uses and compared before anything is written. A plan can be applied once: a
claim file under ``.drt/applied_plans/`` is created atomically (``O_EXCL``)
before the write and updated with the outcome. The claim is local to the
workspace, so it stops concurrent and repeated applies there; coordinating
applies across machines needs a shared claim store (not built yet).
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from drt import __version__

_DURATION = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_DRIFT_SHOWN = 10
_CLOCK_SKEW = timedelta(minutes=5)
_DRIFT_ITEM_CHARS = 300
PLAN_ID_PATTERN = re.compile(r"plan-[0-9a-f]{16}")  # use fullmatch(): `$` allows a trailing newline


class ApplyRefused(Exception):
    """Nothing was written, for the reason in the message."""


class ApplyAborted(ApplyRefused):
    """The person declined the confirmation: nothing was written, and it is not an error."""


@dataclass
class ApplyOutcome:
    plan_id: str
    sync: str
    applied: bool
    message: str = ""
    run_id: str | None = None
    entry: dict[str, Any] = field(default_factory=dict)
    had_error: bool = False
    guards_forced: list[dict[str, Any]] = field(default_factory=list)
    drifted: int = 0


def parse_duration(text: str) -> timedelta:
    match = _DURATION.match(text.strip())
    if not match:
        raise ValueError("use a number and a unit: 30m, 24h, 7d")
    return timedelta(seconds=int(match.group(1)) * _UNITS[match.group(2)])


def describe_drift(report: dict[str, Any]) -> str:
    lines = ["The world no longer matches the plan:"]
    for label, items in (
        ("planned but no longer the case", report["removed"]),
        ("not in the plan", report["added"]),
    ):
        if not items:
            continue
        lines.append(f"  {len(items)} entr{'y' if len(items) == 1 else 'ies'} {label}:")
        for item in items[:_DRIFT_SHOWN]:
            shown = {"action": item.get("action"), "key": item.get("key")}
            text = json.dumps(shown, default=str, sort_keys=True)
            if len(text) > _DRIFT_ITEM_CHARS:
                text = f"{text[:_DRIFT_ITEM_CHARS]}... (+{len(text) - _DRIFT_ITEM_CHARS} chars)"
            lines.append(f"    - {text}")
        if len(items) > _DRIFT_SHOWN:
            lines.append(f"    ... and {len(items) - _DRIFT_SHOWN} more")
    return "\n".join(lines)


def claims_dir(project_dir: Path) -> Path:
    return project_dir / ".drt" / "applied_plans"


def _claim_path(project_dir: Path, plan_id: str) -> Path:
    return claims_dir(project_dir) / f"{plan_id}.json"


def _read_claim(project_dir: Path, plan_id: str) -> dict[str, Any] | None:
    try:
        text = _claim_path(project_dir, plan_id).read_text(encoding="utf-8")
        return json.loads(text)  # type: ignore[no-any-return]
    except (OSError, ValueError):
        return None


def _refuse_if_claimed(project_dir: Path, plan_id: str) -> None:
    from drt.cli._plan_runner import PlanCliError

    claim = _read_claim(project_dir, plan_id)
    if claim is None and not _claim_path(project_dir, plan_id).exists():
        return
    state = (claim or {}).get("state", "unknown")
    when = (claim or {}).get("claimed_at", "?")
    if state == "success":
        message = f"Plan {plan_id} was already applied ({when})."
    elif state == "pending":
        message = (
            f"Plan {plan_id} is being applied, or an earlier apply stopped part-way "
            f"(claimed {when}). Check the destination before doing anything else; "
            f"{_claim_path(project_dir, plan_id)} records the attempt."
        )
    else:
        message = f"Plan {plan_id} was already attempted (state: {state}, {when})."
    raise PlanCliError(f"{message} A plan is single-use; run `drt plan` again.")


def _claim(
    project_dir: Path,
    plan_id: str,
    sync_name: str,
    run_id: str,
    forced: list[str] | None = None,
    approved_by: str | None = None,
) -> None:
    """Atomically take the plan. Raises ``FileExistsError`` if someone already did."""
    claims_dir(project_dir).mkdir(parents=True, exist_ok=True)
    fd = os.open(_claim_path(project_dir, plan_id), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "plan_id": plan_id,
                "sync": sync_name,
                "run_id": run_id,
                "state": "pending",
                "claimed_at": datetime.now(timezone.utc).isoformat(),
                **({"guards_forced": forced} if forced else {}),
                **({"approved_by": approved_by} if approved_by else {}),
            },
            handle,
        )


def _finish_claim(project_dir: Path, plan_id: str, state: str) -> None:
    """Record the outcome. Never raises: by now the destination may already have
    been written, and a bookkeeping failure must not hide that (or an earlier error)."""
    log = logging.getLogger(__name__)
    try:
        claim = _read_claim(project_dir, plan_id) or {"plan_id": plan_id}
        claim.update(state=state, finished_at=datetime.now(timezone.utc).isoformat())
        # Private, exclusive temp file in the same directory (mkstemp is 0600 and
        # does not follow a pre-existing symlink), then an atomic rename.
        fd, tmp_name = tempfile.mkstemp(
            dir=claims_dir(project_dir), prefix=f".{plan_id}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(claim, handle)
            os.replace(tmp_name, _claim_path(project_dir, plan_id))
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    except Exception as e:  # noqa: BLE001 - see docstring
        log.warning(
            "Could not record the outcome of plan %s (%s); %s still says 'pending'.",
            plan_id,
            e,
            _claim_path(project_dir, plan_id),
        )


def apply_plan(
    plan_text: str,
    *,
    project_dir: Path = Path("."),
    approved: bool = False,
    approved_by: str | None = None,
    confirm: Callable[[str], bool] | None = None,
    max_age: timedelta = timedelta(hours=24),
    allow_drift_pct: float = 0.0,
    force_guards: bool = False,
    cursor_value: str | None = None,
    cli_vars: dict[str, Any] | None = None,
    profile_name: str | None = None,
    json_output: bool = False,
    notify: Callable[[str], None] | None = None,
    expect_plan_id: str | None = None,
) -> ApplyOutcome:
    """Verify a plan against the live world and, only if it still matches, write it.

    ``approved`` is the caller's assertion that a human said yes (``--auto-approve``,
    or the MCP client's permission prompt); without it ``confirm`` is asked, and
    with neither nothing is written. ``approved_by`` is recorded, not checked.
    """
    from drt._identifiers import new_run_id
    from drt.cli._helpers import get_source
    from drt.cli._plan_runner import PlanCliError, compute_plan, load_plan_key
    from drt.cli.commands.run import _run_one, _RunContext, _write_run_results
    from drt.engine.plan import PlanDocumentError, drift_report, key_id_of, load_plan_document

    say = notify or (lambda _message: None)

    if not math.isfinite(allow_drift_pct) or allow_drift_pct < 0 or allow_drift_pct > 100:
        raise ApplyRefused("allow_drift_pct must be between 0 and 100.")

    try:
        doc = load_plan_document(plan_text)
    except PlanDocumentError as e:
        raise ApplyRefused(f"Cannot apply the plan: {e}") from e

    plan_major = str(doc["drt_version"]).split(".")[0]
    if plan_major != __version__.split(".")[0]:
        raise ApplyRefused(
            f"The plan was made by drt {doc['drt_version']} and cannot be applied by "
            f"drt {__version__} (different major version). Re-run `drt plan`."
        )

    try:
        created = datetime.fromisoformat(doc["created_at"])
    except ValueError as e:
        raise ApplyRefused("The plan's created_at is not a valid timestamp.") from e
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - created
    if age < -_CLOCK_SKEW:
        raise ApplyRefused(
            "The plan's created_at is in the future; refusing it. Re-run `drt plan`."
        )
    if age > max_age:
        raise ApplyRefused(
            f"The plan is {age} old, older than --max-age {max_age}. Re-run `drt plan`."
        )

    plan_id = doc["plan_id"]
    sync_name = doc["sync"]["name"]
    if expect_plan_id is not None and plan_id != expect_plan_id:
        # The caller asked for one plan and was handed another (a file swapped
        # under a different name); applying it would bypass the approval that
        # named the requested one.
        raise ApplyRefused(
            f"The stored plan is {plan_id}, not the requested {expect_plan_id}; refusing it. "
            "Call drt_plan again."
        )

    if key_id_of(load_plan_key(project_dir)) != doc["options"]["key_id"]:
        raise ApplyRefused(
            "The plan was made with a different plan key, so its hashes cannot be checked "
            "here. Set the same DRT_PLAN_KEY where you plan and where you apply, or apply "
            "from the workspace that made the plan."
        )

    def preflight(_project: Any, _state_bundle: Any, _sync: Any) -> None:
        _refuse_if_claimed(project_dir, plan_id)

    try:
        ctx_plan = compute_plan(
            sync_name,
            redact_keys=bool(doc["options"]["redact_keys"]),
            cursor_value=cursor_value,
            cli_vars=cli_vars,
            profile_name=profile_name,
            project_dir=project_dir,
            preflight=preflight,
        )
    except PlanCliError as e:
        raise ApplyRefused(str(e)) from e

    current = ctx_plan.plan
    if not current.available:
        raise ApplyRefused(f"Cannot verify the plan: {current.unavailable_reason}")
    if current.config_hash != doc["fingerprints"]["config_hash"]:
        raise ApplyRefused(
            "The sync definition (or the model SQL it references) changed since the plan "
            "was made. Re-run `drt plan`."
        )
    if current.environment_hash != doc["fingerprints"]["environment_hash"]:
        raise ApplyRefused(
            "The environment differs from the one the plan was made in (profile, project "
            "vars, environment variables or the resolved destination). Re-run `drt plan` "
            "here, or apply from the environment that made the plan."
        )
    if current.cursor_hash != doc["fingerprints"]["cursor_hash"]:
        raise ApplyRefused(
            "The incremental watermark moved since the plan was made, so the plan covers a "
            "different window. Re-run `drt plan`."
        )

    drifted = 0
    if current.digest != doc["digest"]:
        report = drift_report(doc["entries"], [e.to_dict() for e in current.entries])
        drifted = report["drifted"]
        pct = 100.0 * drifted / max(len(doc["entries"]), 1)
        word = "y" if drifted == 1 else "ies"
        # Zero tolerance means any difference at all; the percentage is only
        # consulted when the operator opted into some drift.
        if allow_drift_pct == 0 or pct > allow_drift_pct:
            raise ApplyRefused(
                describe_drift(report)
                + f"\n  {drifted} entr{word} drifted ({pct:.1f}% of the plan; allowed "
                f"{allow_drift_pct}%). Nothing was written."
            )
        say(f"Proceeding despite drift: {drifted} entr{word} ({pct:.1f}% <= {allow_drift_pct}%).")

    summary = current.summary
    parts = [f"{summary[a]} {a}" for a in summary if summary[a]]
    if not current.has_changes:
        return ApplyOutcome(
            plan_id, sync_name, applied=False, message="The plan has no changes; nothing to apply."
        )

    # Guards are judged on the freshly recomputed plan, not on the file: the file
    # records what was true when it was made, this is what would be written now.
    tripped = [t.to_dict() for t in current.guard_trips]
    if tripped and not force_guards:
        raise ApplyRefused(
            "Change guards tripped; nothing was written:\n"
            + "\n".join(f"  - {t['message']}" for t in tripped)
            + "\nFix the cause, or review and re-run with --force-guards."
        )
    if tripped:
        say(
            "Applying despite tripped guards (--force-guards): "
            + "; ".join(str(t["message"]) for t in tripped)
        )

    if not approved:
        if confirm is None:
            raise ApplyRefused("Approval is required to apply a plan.")
        question = f"Apply plan {plan_id} to {current.destination}: {', '.join(parts)}."
        if not confirm(question):
            raise ApplyAborted("Aborted. Nothing was written.")

    project = ctx_plan.project
    state_bundle = ctx_plan.state_bundle
    run_id = new_run_id()
    try:
        _claim(
            project_dir,
            plan_id,
            sync_name,
            run_id,
            [str(t["guard"]) for t in tripped],
            approved_by,
        )
    except FileExistsError as e:
        raise ApplyRefused(
            f"Plan {plan_id} was just taken by another apply. A plan is single-use; "
            "nothing was written by this command."
        ) from e

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    ctx = _RunContext(
        source=get_source(ctx_plan.profile),
        state_mgr=state_bundle.state,
        history_mgr=state_bundle.history,
        dlq_store=state_bundle.dlq,
        history_retention_days=project.history.retention_days,
        json_mode=json_output,
        dry_run=False,
        verbose=False,
        quiet=json_output,
        log_json=False,
        # The window the plan was verified over, not whatever the watermark says now.
        cursor_value=ctx_plan.cursor_value_used,
        vars=ctx_plan.project_vars,
        query_tagging=project.query_tagging,
        run_id=run_id,
        idempotency_ledger=state_bundle.ledger,
        audit_trail=state_bundle.audit_trail,
        audit_fields=project.state.audit_trail.fields,
        audit_retain_days=project.state.audit_trail.retain_days or 30,
        project_dir=project_dir,
    )
    try:
        _name, entry, had_error = _run_one(ctx_plan.sync, ctx, ctx_plan.profile)
    except BaseException:
        _finish_claim(project_dir, plan_id, "failed")  # never raises: cannot mask this error
        raise
    _finish_claim(project_dir, plan_id, "failed" if had_error else "success")
    entry["plan_id"] = plan_id
    if approved_by:
        entry["approved_by"] = approved_by
    if tripped:
        entry["guards_forced"] = tripped
    _write_run_results(
        project_dir / "target" / "drt",
        run_id=run_id,
        started_at=started_at,
        results=[entry],
        succeeded=0 if had_error else 1,
        failed=1 if had_error else 0,
        skipped=0,
        total_duration=round(time.monotonic() - t0, 2),
        exit_code=1 if had_error else 0,
    )
    return ApplyOutcome(
        plan_id,
        sync_name,
        applied=True,
        run_id=run_id,
        entry=entry,
        had_error=had_error,
        guards_forced=tripped,
        drifted=drifted,
    )
