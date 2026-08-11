"""Implementation for the ``drt_get_status`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def get_status(ctx: McpContext, sync_name: str | None = None) -> dict[str, Any]:
    from drt.state.factory import build_state_bundle

    states = build_state_bundle(ctx.load_project_for_state(), ctx.project_dir).state.get_all()

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
