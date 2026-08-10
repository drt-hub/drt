"""Implementation for the ``drt_retry`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def retry(
    ctx: McpContext,
    sync_name: str,
    limit: int | None = None,
    dry_run: bool = False,
    clear: bool = False,
) -> dict[str, Any]:
    from drt.cli.commands.retry import replay_dead_letters

    if limit is not None and limit < 0:
        return {"error": "limit must be >= 0."}

    sync = ctx.find_sync(sync_name)
    if sync is None:
        return {"error": f"No sync named '{sync_name}' found."}

    return replay_dead_letters(
        sync,
        project=ctx.load_project_for_state(),
        limit=limit,
        dry_run=dry_run,
        clear=clear,
        project_dir=ctx.project_dir,
    )
