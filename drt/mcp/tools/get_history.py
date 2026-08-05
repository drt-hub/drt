"""Implementation for the ``drt_get_history`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def get_history(
    ctx: McpContext, sync_name: str | None = None, limit: int = 20
) -> dict[str, Any]:
    from dataclasses import asdict

    from drt.state.history import HistoryManager

    entries = HistoryManager(ctx.project_dir).read(sync_name=sync_name, limit=limit)
    return {"entries": [asdict(e) for e in entries]}
