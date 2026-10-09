"""Implementation for the ``drt_plan`` MCP tool (#1220).

Read-only: computes the same change set as ``drt plan`` and stores the full plan
under ``target/drt/plans/<plan_id>.json`` so that ``drt_apply`` can only ever
apply a plan that was fetched here first.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.engine.plan import Plan
    from drt.mcp._context import McpContext

_MAX_ENTRIES_CAP = 1000


def plans_dir(project_dir: Path) -> Path:
    return project_dir / "target" / "drt" / "plans"


def store_plan(project_dir: Path, plan: Plan) -> Path:
    """Write the plan privately (0600, atomic) and return its path."""
    folder = plans_dir(project_dir)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{plan.plan_id}.json"
    fd, tmp = tempfile.mkstemp(dir=folder, prefix=f".{plan.plan_id}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(plan.to_json())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def plan(
    ctx: McpContext,
    sync_name: str,
    redact_keys: bool = False,
    cursor_value: str | None = None,
    profile_name: str | None = None,
    vars: dict[str, Any] | None = None,
    max_entries: int = 100,
) -> dict[str, Any]:
    from drt.cli._plan_runner import PlanCliError, compute_plan

    if max_entries < 0 or max_entries > _MAX_ENTRIES_CAP:
        return {"error": f"max_entries must be between 0 and {_MAX_ENTRIES_CAP}."}

    try:
        planned = compute_plan(
            sync_name,
            redact_keys=redact_keys,
            cursor_value=cursor_value,
            cli_vars=vars,
            profile_name=profile_name,
            project_dir=ctx.project_dir,
        ).plan
    except PlanCliError as e:
        return {"error": str(e)}

    if not planned.available:
        return {
            "sync": sync_name,
            "available": False,
            "reason": planned.unavailable_reason,
            "next_step": "This sync cannot be planned; it cannot be applied through drt_apply.",
        }

    path = store_plan(ctx.project_dir, planned)
    document = planned.to_dict()
    entries = document["entries"]
    shown = [
        {k: e[k] for k in ("key", "action", "changed_columns", "delete_reason") if k in e}
        for e in entries[:max_entries]
    ]
    return {
        "sync": sync_name,
        "available": True,
        "plan_id": planned.plan_id,
        "has_changes": planned.has_changes,
        "summary": document["summary"],
        "guards": document["guards"],
        "destination": planned.destination,
        "created_at": planned.created_at,
        "digest": planned.digest,
        "entries": shown,
        "entries_total": len(entries),
        "entries_truncated": len(entries) > max_entries,
        "plan_file": str(path),
        "next_step": (
            "Review the changes, then call drt_apply(plan_id, approved_by) once a human has "
            "approved. The plan is single-use and is refused if the world drifted."
            if planned.has_changes
            else "No changes: there is nothing to apply."
        ),
    }


def load_stored_plan(project_dir: Path, plan_id: str) -> str | None:
    """The text of a plan previously stored by :func:`plan`, or ``None``."""
    try:
        return (plans_dir(project_dir) / f"{plan_id}.json").read_text(encoding="utf-8")
    except OSError:
        return None

