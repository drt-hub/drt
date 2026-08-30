"""Implementation for the ``drt_state_show`` / ``drt_state_reset`` MCP tools.

Public docstrings live on the ``@mcp.tool()`` wrappers in
``drt/mcp/server.py``.

Shipped with the CLI half of #776 rather than retrofitted. The #870 audit
found the v0.8.0 flag wave never reached MCP at all, so an agent driving drt
could not use the safe options a human could — the failure mode this module
exists not to repeat.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def state_show(ctx: McpContext, sync_name: str | None = None) -> dict[str, Any]:
    from drt.state.factory import build_state_bundle

    states = build_state_bundle(ctx.load_project_for_state(), ctx.project_dir).state.get_all()

    if sync_name:
        s = states.get(sync_name)
        if s is None:
            return {"sync_name": sync_name, "state": None}
        return {
            "sync_name": sync_name,
            "state": {
                "last_run_at": s.last_run_at,
                "status": s.status,
                "records_synced": s.records_synced,
                "last_cursor_value": s.last_cursor_value,
            },
        }

    return {
        "states": {
            name: {
                "last_run_at": s.last_run_at,
                "status": s.status,
                "last_cursor_value": s.last_cursor_value,
            }
            for name, s in sorted(states.items())
        }
    }


def state_reset(
    ctx: McpContext,
    sync_name: str,
    watermark: bool = False,
    runs: bool = False,
    tracked_mirror: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Reset one sync's state. At least one level must be requested.

    Deliberately mirrors the CLI's refusal when no level is given. An agent is
    *more* likely to call this with defaults than a human is, so treating "no
    level" as "all of it" would be worse here, not better.

    ``tracked_mirror`` re-baselines the destination's ``_drt_synced_keys``,
    after which rows other systems wrote count as drt's own and become
    deletion candidates on the next mirror pass (#686). The response says so
    explicitly, since an agent has no help text to read.
    """
    levels = [
        name
        for name, on in (
            ("watermark", watermark),
            ("runs", runs),
            ("tracked-mirror", tracked_mirror),
        )
        if on
    ]
    if not levels:
        return {
            "error": (
                "Choose what to reset: pass watermark=true, runs=true and/or "
                "tracked_mirror=true. They are separate because tracked_mirror "
                "re-baselines the destination's key table."
            )
        }

    if dry_run:
        return {"dry_run": True, "would_reset": levels, "sync_name": sync_name}

    result: dict[str, Any] = {"sync_name": sync_name, "reset": levels}

    if watermark:
        from drt.cli._helpers import get_watermark_storage

        for s in ctx.load_syncs():
            if s.name != sync_name:
                continue
            storage = get_watermark_storage(s, ctx.project_dir)
            if storage is not None:
                storage.delete(sync_name)

    if runs:
        from drt.state.factory import build_state_bundle

        build_state_bundle(ctx.load_project_for_state(), ctx.project_dir).state.reset(sync_name)

    if tracked_mirror:
        from drt.cli._helpers import get_destination

        syncs = [s for s in ctx.load_syncs() if s.name == sync_name]
        if syncs:
            dest = get_destination(syncs[0])
            reset = getattr(dest, "reset_tracked_state", None)
            if reset is not None:
                from drt.destinations.sql_base import BaseSqlDestination

                inherited_without_state_hooks = (
                    isinstance(dest, BaseSqlDestination)
                    and getattr(type(dest), "reset_tracked_state", None)
                    is BaseSqlDestination.reset_tracked_state
                    and getattr(type(dest), "_state_table_ident", None)
                    is BaseSqlDestination._state_table_ident
                )
            else:
                inherited_without_state_hooks = False
            if reset is None or inherited_without_state_hooks:
                result["warning"] = (
                    f"{syncs[0].destination.type} does not support tracked mirror."
                )
            else:
                result["keys_removed"] = int(reset(syncs[0].destination, sync_name))
                result["warning"] = (
                    "Tracked-mirror state was re-baselined: the next mirror pass "
                    "treats what is currently in the target as drt's own, so rows "
                    "written by other systems become deletion candidates."
                )

    return result
