"""Implementation for the ``drt_apply`` MCP tool (#1220).

Applies a plan that ``drt_plan`` stored, through the same guarded path as
``drt apply`` (``drt/cli/_apply_flow.py``). It cannot apply a plan it was not
given by ``drt_plan`` on this project, nor one that drifted, was edited, is
stale or was already applied.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from drt.mcp._untrusted import DATA_NOTICE, message

if TYPE_CHECKING:
    from drt.mcp._context import McpContext

_MAX_APPROVER = 200
_RESULT_KEYS = ("status", "rows_synced", "rows_failed", "duration_seconds", "error", "run_id")


def apply(
    ctx: McpContext,
    plan_id: str,
    approved_by: str,
    max_age: str = "24h",
    allow_drift_pct: float = 0.0,
    force_guards: bool = False,
    cursor_value: str | None = None,
    profile_name: str | None = None,
    vars: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from drt.cli._apply_flow import PLAN_ID_PATTERN, ApplyRefused, apply_plan, parse_duration
    from drt.cli._plan_runner import load_plan_key
    from drt.mcp.tools.plan import load_stored_plan

    who = (approved_by or "").strip()
    if not who or len(who) > _MAX_APPROVER:
        return {
            "applied": False,
            "error": "approved_by is required: name the person who approved this apply.",
        }
    if not PLAN_ID_PATTERN.fullmatch(plan_id or ""):
        return {"applied": False, "error": "plan_id must be an id returned by drt_plan."}
    try:
        max_age_delta = parse_duration(max_age)
    except ValueError as e:
        return {"applied": False, "error": f"max_age: {e}"}

    text = load_stored_plan(ctx.project_dir, plan_id)
    if text is None:
        return {
            "applied": False,
            "error": (
                f"No plan {plan_id} was fetched with drt_plan on this project. Call drt_plan first."
            ),
        }

    notes: list[str] = []
    try:
        outcome = apply_plan(
            text,
            project_dir=ctx.project_dir,
            # The human approval is the MCP client's permission prompt for this
            # tool plus the named approver; both are recorded with the run.
            approved=True,
            approved_by=who,
            max_age=max_age_delta,
            allow_drift_pct=allow_drift_pct,
            force_guards=force_guards,
            cursor_value=cursor_value,
            cli_vars=vars,
            profile_name=profile_name,
            json_output=True,
            notify=notes.append,
            expect_plan_id=plan_id,
        )
    except ApplyRefused as e:
        plan_key = load_plan_key(ctx.project_dir)
        return {
            "applied": False,
            "plan_id": plan_id,
            "error": message(str(e), plan_key),
            "notes": [message(n, plan_key, 400) for n in notes],
            "data_notice": DATA_NOTICE,
        }

    if not outcome.applied:
        return {"applied": False, "plan_id": plan_id, "message": outcome.message}
    return {
        "applied": True,
        "plan_id": plan_id,
        "run_id": outcome.run_id,
        "sync": outcome.sync,
        "approved_by": who,
        "failed": outcome.had_error,
        "result": _result(outcome.entry, load_plan_key(ctx.project_dir)),
        "guards_forced": [t["guard"] for t in outcome.guards_forced],
        "notes": [message(n, load_plan_key(ctx.project_dir), 400) for n in notes],
        "data_notice": DATA_NOTICE,
    }


def _result(entry: dict[str, Any], plan_key: bytes) -> dict[str, Any]:
    """The run's outcome, with connector error text treated as untrusted data."""
    shown = {k: entry[k] for k in _RESULT_KEYS if k in entry}
    if isinstance(shown.get("error"), str):
        shown["error"] = message(shown["error"], plan_key, 500)
    return shown
