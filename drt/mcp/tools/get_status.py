"""Implementation for the ``drt_get_status`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def get_status(
    ctx: McpContext,
    sync_name: str | None = None,
    check: bool = False,
    allow_empty: bool = False,
) -> dict[str, Any]:
    from drt.state.factory import build_state_bundle

    if allow_empty and not check:
        return {"error": "allow_empty only applies together with check=True."}

    bundle = build_state_bundle(ctx.load_project_for_state(), ctx.project_dir)

    if check:
        # Same verdict as `drt status --check` (#1224): freshness against
        # alerts.on_stale, read-only, from run history.
        from drt.engine.freshness import evaluate_freshness, has_failure
        from drt.state._integrity import collect_unreadable as collect_unreadable_evidence

        syncs = ctx.load_syncs()
        if sync_name:
            syncs = [s for s in syncs if s.name == sync_name]
            if not syncs:
                return {"error": f"No sync named '{sync_name}'."}
        with collect_unreadable_evidence() as unreadable:
            verdicts = evaluate_freshness(syncs, bundle.history, bundle.state.get_all())
        monitored = {v.sync_name for v in verdicts}
        return {
            # Fails closed: a check that monitored nothing is not "ok".
            "ok": (bool(verdicts) or allow_empty) and not has_failure(verdicts) and not unreadable,
            "unreadable_evidence": len(unreadable),
            "checked": len(verdicts),
            "unmonitored": sorted(s.name for s in syncs if s.name not in monitored),
            "syncs": [v.to_dict() for v in verdicts],
        }

    states = bundle.state.get_all()

    if sync_name:
        if sync_name not in states:
            return {"error": f"No state found for sync '{sync_name}'."}
        s = states[sync_name]
        return {
            sync_name: {
                "last_run_at": s.last_run_at,
                "records_synced": s.records_synced,
                "status": s.status,
                "last_cursor_value": s.last_cursor_value,
                "error": s.error,
            }
        }

    return {
        name: {
            "last_run_at": s.last_run_at,
            "records_synced": s.records_synced,
            "status": s.status,
            "last_cursor_value": s.last_cursor_value,
            "error": s.error,
        }
        for name, s in states.items()
    }
