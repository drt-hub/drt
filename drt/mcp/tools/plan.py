"""Implementation for the ``drt_plan`` MCP tool (#1220).

Read-only: computes the same change set as ``drt plan`` and stores the full plan
under ``target/drt/plans/<plan_id>.json`` so that ``drt_apply`` can only ever
apply a plan that was fetched here first.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from drt.mcp._untrusted import DATA_NOTICE, NAME_CHARS, message, sanitize, shorten

if TYPE_CHECKING:
    from drt.engine.plan import Plan
    from drt.mcp._context import McpContext

_MAX_ENTRIES_CAP = 1000
_RESPONSE_BYTES = 60_000
_MAX_CHANGED_COLUMNS = 20


def _safe_entry(entry: dict[str, Any], plan_key: bytes) -> dict[str, Any]:
    columns = entry.get("changed_columns", [])
    out: dict[str, Any] = {
        "key": sanitize(entry["key"], plan_key),
        "action": entry["action"],
    }
    if columns:
        out["changed_columns"] = [
            shorten(c, plan_key, NAME_CHARS) for c in columns[:_MAX_CHANGED_COLUMNS]
        ]
        if len(columns) > _MAX_CHANGED_COLUMNS:
            out["changed_columns_more"] = len(columns) - _MAX_CHANGED_COLUMNS
    if "delete_reason" in entry:
        out["delete_reason"] = entry["delete_reason"]
    return out


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
    from drt.cli._plan_runner import PlanCliError, compute_plan, load_plan_key

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
        return {
            "error": message(str(e), load_plan_key(ctx.project_dir)),
            "data_notice": DATA_NOTICE,
        }

    plan_key = load_plan_key(ctx.project_dir)
    if not planned.available:
        return {
            "sync": sync_name,
            "available": False,
            "reason": message(planned.unavailable_reason or "", plan_key),
            "data_notice": DATA_NOTICE,
            "next_step": "This sync cannot be planned; it cannot be applied through drt_apply.",
        }

    path = store_plan(ctx.project_dir, planned)
    document = planned.to_dict()
    entries = document["entries"]
    shown: list[dict[str, Any]] = []
    used = 0
    for raw in entries[:max_entries]:
        safe = _safe_entry(raw, plan_key)
        size = len(json.dumps(safe, default=str))
        if used + size > _RESPONSE_BYTES:
            break
        shown.append(safe)
        used += size
    return {
        "sync": sync_name,
        "available": True,
        "plan_id": planned.plan_id,
        "has_changes": planned.has_changes,
        "summary": document["summary"],
        "guards": document["guards"],
        "destination": shorten(planned.destination, plan_key),
        "created_at": planned.created_at,
        "digest": planned.digest,
        "entries": shown,
        "entries_total": len(entries),
        "entries_returned": len(shown),
        "entries_truncated": len(entries) > len(shown),
        "plan_file": str(path),
        "data_notice": DATA_NOTICE,
        "next_step": (
            "Review the changes, then call drt_apply(plan_id, approved_by) once a human has "
            "approved. The plan is single-use and is refused if the world drifted."
            if planned.has_changes
            else "No changes: there is nothing to apply."
        ),
    }


def load_stored_plan(project_dir: Path, plan_id: str) -> str | None:
    """The text of a plan previously stored by :func:`plan`, or ``None``.

    The file is the only evidence that ``drt_plan`` issued this plan, so it is
    opened once without following a symlink and checked on the *open
    descriptor* (not on the path, which could change between a check and a
    read): anything that is not a regular file is treated as absent.
    """
    path = plans_dir(project_dir) / f"{plan_id}.json"
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            return None
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = -1
            return handle.read()
    except (OSError, UnicodeDecodeError):
        return None
    finally:
        if fd != -1:
            os.close(fd)
