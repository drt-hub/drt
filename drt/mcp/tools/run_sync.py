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
    manifest_path: str | None = None,
    full_refresh: bool = False,
    limit: int | None = None,
    vars: dict[str, Any] | None = None,
    force_guards: bool = False,
    approved_by: str | None = None,
    backfill_start: str | None = None,
    backfill_end: str | None = None,
) -> dict[str, Any]:
    from drt.cli._helpers import get_watermark_storage, resolve_profile_name
    from drt.cli.main import _get_destination, _get_source
    from drt.config.credentials import load_profile, resolve_profile_paths
    from drt.config.parser import project_vars as resolve_project_vars
    from drt.engine.observer import CompositeObserver, DlqObserver, StatePersistingObserver
    from drt.engine.sync import run_sync as engine_run_sync
    from drt.state.factory import build_state_bundle

    approver = (approved_by or "").strip()
    if force_guards and (not approver or len(approver) > 200):
        return {
            "error": (
                "approved_by is required when force_guards=true: name the person "
                "who approved the override."
            )
        }

    if compute_diff and not dry_run:
        return {
            "error": "compute_diff requires dry_run=True (matches the "
            "`drt run --dry-run --diff` CLI contract)."
        }

    if full_refresh and cursor_value is not None:
        # Same exclusion as the CLI: one says "start from nothing", the other
        # "start from here". Picking a winner silently would make a backfill
        # look like it worked.
        return {"error": "full_refresh and cursor_value are mutually exclusive."}

    if limit is not None and limit < 1:
        return {"error": "limit must be a positive integer."}

    range_override = backfill_start is not None or backfill_end is not None
    if range_override and cursor_value is not None:
        return {"error": "backfill_start/backfill_end and cursor_value are mutually exclusive."}
    if range_override and full_refresh:
        return {"error": "backfill_start/backfill_end and full_refresh are mutually exclusive."}
    if range_override and limit is not None:
        return {"error": "backfill_start/backfill_end and limit are mutually exclusive."}

    project = ctx.load_project()

    # vars: `vars:` block < DRT_VAR_* < this call's `vars` (#870/#783) — same
    # precedence `drt run --vars` resolves, minus the string-parsing step
    # (an MCP caller passes an already-structured dict, not `'k: v, k2: v2'`).
    resolved_vars = resolve_project_vars(ctx.project_dir, cli_vars=vars)
    sync = ctx.find_sync(sync_name, vars=resolved_vars, manifest_path=manifest_path)
    if sync is None:
        return {"error": f"No sync named '{sync_name}' found."}
    if range_override and sync.sync.backfill is None:
        return {
            "error": (
                "backfill_start/backfill_end are only valid for a sync with sync.backfill; "
                f"'{sync_name}' has none."
            )
        }
    if sync.sync.backfill is not None:
        if dry_run:
            return {
                "error": (
                    "dry_run is not allowed with sync.backfill; window preview/diff "
                    "support is not available yet."
                )
            }
        if limit is not None:
            return {
                "error": (
                    "limit is not allowed with sync.backfill because a sampled window "
                    "cannot be checkpointed as complete."
                )
            }
        if cursor_value is not None:
            return {
                "error": (
                    "cursor_value is not allowed with sync.backfill; use backfill_start "
                    "and backfill_end to override the range."
                )
            }
        if full_refresh:
            return {"error": "full_refresh is not allowed with sync.backfill."}

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

    profile = resolve_profile_paths(
        load_profile(resolve_profile_name(profile_name, project.profile)),
        ctx.project_dir,
    )
    source = _get_source(profile)
    dest = _get_destination(sync)
    bundle = build_state_bundle(project, ctx.project_dir)
    state_mgr = bundle.state
    watermark_storage = get_watermark_storage(sync, ctx.project_dir)

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
        if watermark_storage is not None:
            watermark_storage.delete(sync_name)
        state_mgr.reset(sync_name)

    checkpoint_observer = StatePersistingObserver(state_mgr, watermark_storage)
    observers: list[Any] = [checkpoint_observer]
    if sync.sync.dlq is not None and sync.sync.dlq.enabled:
        observers.append(DlqObserver(bundle.dlq, max_records=sync.sync.dlq.max_records))
    try:
        result = engine_run_sync(
            sync,
            source,
            dest,
            profile,
            ctx.project_dir,
            dry_run,
            state_mgr,
            observer=CompositeObserver(observers, checkpoint_observer=checkpoint_observer),
            watermark_storage=watermark_storage,
            cursor_value_override=(cursor_value if sync.sync.mode == "incremental" else None),
            compute_diff=compute_diff,
            diff_limit=diff_limit,
            extract_limit=limit,
            vars=resolved_vars,
            idempotency_ledger=bundle.ledger,
            audit_trail=bundle.audit_trail,
            audit_fields=project.state.audit_trail.fields,
            audit_retain_days=project.state.audit_trail.retain_days or 30,
            force_guards=force_guards,
            full_refresh=full_refresh,
            allow_backfill_execution=True,
            backfill_start=backfill_start,
            backfill_end=backfill_end,
        )
    except Exception as exc:
        from drt.engine.guards import GuardTripped, guards_outcome_of

        if not isinstance(exc, GuardTripped):
            raise
        return {
            "sync_name": sync_name,
            "dry_run": dry_run,
            "success": 0,
            "failed": 1,
            "errors": [str(exc)],
            "guards": guards_outcome_of(exc),
        }

    response: dict[str, Any] = {
        "sync_name": sync_name,
        "dry_run": dry_run,
        "success": result.success,
        "failed": result.failed,
        "errors": result.errors[:10],  # cap at 10 to avoid huge payloads
    }
    if result.ddl:
        response["ddl"] = result.ddl
    if getattr(result, "limit_applied", None) is not None:
        response["limit_applied"] = result.limit_applied
    guards_outcome = getattr(result, "guards", None)
    if guards_outcome is not None:
        response["guards"] = guards_outcome
    if force_guards:
        response["approved_by"] = approver
    diff_value = getattr(result, "diff", None)
    if compute_diff and diff_value is not None:
        from drt.cli.output import diff_to_dict

        response["diff"] = diff_to_dict(diff_value)
    return response
