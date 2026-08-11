"""Implementation for the ``drt_run_sync`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``. Imports of ``_get_source``/``_get_destination``/
``load_profile``/``resolve_profile_name``/``diff_to_dict`` stay function-local
(not hoisted to module level) — tests monkeypatch these at their original
source modules (e.g. ``drt.cli.main._get_source``), which only works if the
lookup happens fresh on every call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def run_sync(
    ctx: McpContext,
    sync_name: str,
    dry_run: bool = False,
    compute_diff: bool = False,
    diff_limit: int = 20,
    cursor_value: str | None = None,
    profile_name: str | None = None,
    full_refresh: bool = False,
    limit: int | None = None,
    vars: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from drt.cli._helpers import resolve_profile_name
    from drt.cli.main import _get_destination, _get_source
    from drt.config.credentials import load_profile
    from drt.config.parser import project_vars as resolve_project_vars
    from drt.engine.sync import run_sync as engine_run_sync
    from drt.state.factory import build_state_bundle

    if compute_diff and not dry_run:
        return {
            "error": "compute_diff requires dry_run=True (matches the "
            "`drt run --dry-run --diff` CLI contract)."
        }

    if full_refresh and cursor_value is not None:
        # Same exclusion as the CLI: one says "start from nothing", the other
        # "start from here". Picking a winner silently would make a backfill
        # look like it worked.
        return {
            "error": "full_refresh and cursor_value are mutually exclusive."
        }

    if limit is not None and limit < 1:
        return {"error": "limit must be a positive integer."}

    project = ctx.load_project()
    profile = load_profile(resolve_profile_name(profile_name, project.profile))

    # vars: `vars:` block < DRT_VAR_* < this call's `vars` (#870/#783) — same
    # precedence `drt run --vars` resolves, minus the string-parsing step
    # (an MCP caller passes an already-structured dict, not `'k: v, k2: v2'`).
    resolved_vars = resolve_project_vars(ctx.project_dir, cli_vars=vars)
    sync = ctx.find_sync(sync_name, vars=resolved_vars)
    if sync is None:
        return {"error": f"No sync named '{sync_name}' found."}

    if limit is not None and sync.sync.mode in ("mirror", "replace"):
        # Same guard as `drt run --limit` (#774): a sampled mirror would
        # DELETE the destination rows the sample skipped; a sampled replace
        # would truncate to the sample.
        return {
            "error": (
                f"limit is not allowed for mode={sync.sync.mode} syncs "
                "(a sample would delete or replace real rows)."
            )
        }

    source = _get_source(profile)
    dest = _get_destination(sync)
    state_mgr = build_state_bundle(project, ctx.project_dir).state

    if full_refresh and not dry_run:
        # Clear both watermark sources, mirroring `drt run --full-refresh`.
        # engine/sync.py reads watermark_storage first and the state manager's
        # cursor only when no storage is configured, so clearing one alone is
        # silently a no-op in the other configuration.
        #
        # `not dry_run` guard (#876): dry_run is documented as a read-only
        # preview, and a stored watermark is data — clearing it for real
        # under dry_run=True contradicted that contract (same bug as the
        # CLI's `drt run --full-refresh --dry-run`, fixed alongside this).
        from drt.cli._helpers import get_watermark_storage

        storage = get_watermark_storage(sync, ctx.project_dir)
        if storage is not None:
            storage.delete(sync_name)
        state_mgr.reset(sync_name)

    result = engine_run_sync(
        sync,
        source,
        dest,
        profile,
        ctx.project_dir,
        dry_run,
        state_mgr,
        cursor_value_override=(cursor_value if sync.sync.mode == "incremental" else None),
        compute_diff=compute_diff,
        diff_limit=diff_limit,
        extract_limit=limit,
        vars=resolved_vars,
    )

    response: dict[str, Any] = {
        "sync_name": sync_name,
        "dry_run": dry_run,
        "success": result.success,
        "failed": result.failed,
        "errors": result.errors[:10],  # cap at 10 to avoid huge payloads
    }
    if getattr(result, "limit_applied", None) is not None:
        response["limit_applied"] = result.limit_applied
    diff_value = getattr(result, "diff", None)
    if compute_diff and diff_value is not None:
        from drt.cli.output import diff_to_dict

        response["diff"] = diff_to_dict(diff_value)
    return response
