"""drt MCP Server — exposes drt operations as MCP tools.

Start with:
    uvx drt-core[mcp] mcp run          # from a drt project directory
    drt mcp run                         # if drt-core[mcp] is installed

Tools:
    drt_list_syncs      — list all sync definitions
    drt_run_sync        — run a specific sync (dry_run + compute_diff supported)
    drt_run_test        — run post-sync validation tests for a sync
    drt_get_status      — get last sync result for a sync
    drt_get_history     — get recent sync run history (v0.7+)
    drt_validate        — validate all sync YAML configs (per-file errors)
    drt_get_schema      — return JSON Schema for drt_project.yml / sync.yml
    drt_list_connectors — list available source and destination connectors
    drt_dlq             — inspect a sync's Dead Letter Queue (depth + records)
    drt_retry           — replay a sync's Dead Letter Queue (v0.7.9)
    drt_get_manifest    — machine-readable sync catalog + lineage (drt docs)
    drt_list_profiles   — list credential profiles (name + type, no secrets)
    drt_test_profile    — connectivity check for a credential profile
    drt_doctor          — environment diagnostics (mirrors `drt doctor` CLI)
    drt_state_show      — stored watermark + last-run state (#776)
    drt_state_reset     — reset watermark / run state / mirror keys (#776)

Business logic for each tool lives in ``drt/mcp/tools/`` (one module per
tool, independently testable without a running server); this module wires
each one up as a thin ``@mcp.tool()`` closure carrying the public
docstring/signature MCP clients see, plus the shared ``McpContext``
(``drt/mcp/_context.py``) that replaces the project/sync-config loading
that used to be duplicated inline in every closure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from drt.mcp._context import _load_ctx
from drt.mcp.tools.dlq import dlq as _dlq
from drt.mcp.tools.doctor import doctor as _doctor
from drt.mcp.tools.get_history import get_history as _get_history
from drt.mcp.tools.get_manifest import get_manifest as _get_manifest
from drt.mcp.tools.get_schema import get_schema as _get_schema
from drt.mcp.tools.get_status import get_status as _get_status
from drt.mcp.tools.list_connectors import list_connectors as _list_connectors
from drt.mcp.tools.list_profiles import list_profiles as _list_profiles
from drt.mcp.tools.list_syncs import list_syncs as _list_syncs
from drt.mcp.tools.retry import retry as _retry
from drt.mcp.tools.run_sync import run_sync as _run_sync
from drt.mcp.tools.run_test import run_test as _run_test
from drt.mcp.tools.state import state_reset as _state_reset
from drt.mcp.tools.state import state_show as _state_show
from drt.mcp.tools.test_profile import test_profile as _test_profile
from drt.mcp.tools.validate import validate as _validate


def create_server(project_dir: Path | None = None) -> Any:
    """Create and return a configured FastMCP server instance."""
    try:
        from fastmcp import FastMCP
    except ImportError as e:
        raise ImportError("MCP server requires: pip install drt-core[mcp]") from e

    _project_dir = project_dir or Path(".")
    ctx = _load_ctx(_project_dir)

    mcp: Any = FastMCP(
        "drt",
        instructions=(
            "drt is a Reverse ETL CLI tool. "
            "Use these tools to list, run, validate, and monitor data syncs "
            "from a data warehouse to external services."
        ),
    )

    # -----------------------------------------------------------------------
    # drt_list_syncs
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_list_syncs() -> list[dict[str, str]]:
        """List all sync definitions in the current drt project.

        Returns a list of sync summaries including name, description,
        model reference, and destination type.
        """
        return _list_syncs(ctx)

    # -----------------------------------------------------------------------
    # drt_run_sync
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_run_sync(
        sync_name: str,
        dry_run: bool = False,
        compute_diff: bool = False,
        diff_limit: int = 20,
        cursor_value: str | None = None,
        profile_name: str | None = None,
        full_refresh: bool = False,
    ) -> dict[str, Any]:
        """Run a specific drt sync.

        Args:
            sync_name: Name of the sync to run (from drt_list_syncs).
            dry_run: If True, extracts data but does not write to destination.
            compute_diff: When True (requires ``dry_run=True``), compute a
                record-level diff (added / updated / deleted / unchanged)
                against the destination. Queryable destinations get a true
                diff; non-queryable destinations get a sample preview.
                Mirrors ``drt run --dry-run --diff`` (v0.7.1+).
            diff_limit: Cap on records per diff category (default 20).
            cursor_value: Override the incremental watermark for a bounded
                backfill (mirrors ``drt run --cursor-value``, v0.6.2). Ignored
                for non-incremental syncs.
            profile_name: Override the profile resolved from drt_project.yml /
                ``DRT_PROFILE`` (mirrors ``drt run --profile``).
            full_refresh: Clear the stored watermark first, so this run
                re-reads everything and then persists a fresh watermark
                (mirrors ``drt run --full-refresh``, #776). Mutually exclusive
                with ``cursor_value``. Does **not** reset tracked-mirror
                state — use ``drt_state_reset(tracked_mirror=True)`` for that,
                and read its warning before doing so.

        Returns:
            Result summary with success count, failed count, errors, and
            (when ``compute_diff=True``) a ``diff`` field with the
            structured preview.
        """
        return _run_sync(
            ctx,
            sync_name,
            dry_run=dry_run,
            compute_diff=compute_diff,
            diff_limit=diff_limit,
            cursor_value=cursor_value,
            profile_name=profile_name,
            full_refresh=full_refresh,
        )

    # -----------------------------------------------------------------------
    # drt_run_test
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_run_test(sync_name: str | None = None) -> dict[str, Any]:
        """Run post-sync validation tests for one or all syncs.

        Mirrors the `drt test` CLI: for each sync with `tests:` defined,
        executes the test queries against the destination and reports
        per-test pass/fail.

        Args:
            sync_name: Restrict to one sync. If omitted, runs tests for
                every sync that has tests defined.

        Returns:
            Dict with `status` ("passed" | "failed" | "no_tests" | "no_syncs"),
            and `results` — a list of per-sync result objects, each with:
                - `sync`: sync name
                - `tests`: list of {name, passed, value, severity} or
                  {name, passed: false, error, severity} — severity is
                  "warn" | "error" (#779); a "warn" failure is reported here
                  but never flips the top-level `status` to "failed"
                - `skipped` (optional): true when destination type isn't queryable
                - `reason` (optional): why the sync was skipped
        """
        return _run_test(ctx, sync_name)

    # -----------------------------------------------------------------------
    # drt_get_status
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_get_status(sync_name: str | None = None) -> dict[str, Any]:
        """Get the last sync run result(s).

        Args:
            sync_name: Name of a specific sync. If omitted, returns all syncs.

        Returns:
            Dict of sync_name → last run status (last_run_at, records_synced,
            status, last_cursor_value).
        """
        return _get_status(ctx, sync_name)

    # -----------------------------------------------------------------------
    # drt_state_show / drt_state_reset (#776)
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_state_show(sync_name: str | None = None) -> dict[str, Any]:
        """Show drt's stored state for a sync (watermark + last run).

        Args:
            sync_name: Sync to inspect. If omitted, returns all syncs.

        Returns:
            The stored watermark, last run time, status and row count.
        """
        return _state_show(ctx, sync_name)

    @mcp.tool()
    def drt_state_reset(
        sync_name: str,
        watermark: bool = False,
        runs: bool = False,
        tracked_mirror: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Reset one sync's durable state. At least one level is required.

        The levels are separate on purpose and none is implied by another.

        Args:
            sync_name: Sync whose state to reset.
            watermark: Clear the stored watermark, so the next run re-reads
                everything. This is the equivalent of `drt run --full-refresh`.
            runs: Clear recorded run state (last status, row count, and the
                fallback cursor used when no watermark backend is configured).
            tracked_mirror: Re-baseline the destination's `_drt_synced_keys`.
                **Destructive in a non-obvious way**: afterwards, rows written
                by other systems count as drt's own and become deletion
                candidates on the next mirror pass. Ask for this only when
                that is intended.
            dry_run: Report what would be reset without changing anything.

        Returns:
            Which levels were reset, or an error if none was requested.
        """
        return _state_reset(ctx, sync_name, watermark, runs, tracked_mirror, dry_run)

    # -----------------------------------------------------------------------
    # drt_get_history
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_get_history(
        sync_name: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Get past sync execution entries (newest first).

        Each entry corresponds to one ``drt run`` invocation against a sync.
        Use this to answer questions like "did the daily user_sync run last
        night and how many rows were transferred?".

        Args:
            sync_name: Restrict to one sync. If omitted, all syncs are merged
                and re-sorted by start time.
            limit: Maximum number of entries to return (default 20).

        Returns:
            Dict with ``entries`` list, each entry containing sync_name,
            started_at, completed_at, duration_seconds, status, records_synced,
            records_failed, errors (truncated), and cursor_value_used.
        """
        return _get_history(ctx, sync_name, limit)

    # -----------------------------------------------------------------------
    # drt_validate
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_validate() -> dict[str, Any]:
        """Validate all sync YAML configs in the current project.

        Returns:
            Dict with 'valid' list of sync names and 'errors' dict of
            sync_name → list of error messages for any invalid configs.
        """
        return _validate(ctx)

    # -----------------------------------------------------------------------
    # drt_get_schema
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_get_schema(schema_type: str = "sync") -> dict[str, Any]:
        """Return the JSON Schema for drt configuration files.

        Args:
            schema_type: "sync" for sync YAML schema, "project" for
                         drt_project.yml schema.

        Returns:
            JSON Schema as a dict.
        """
        return _get_schema(schema_type)

    # -----------------------------------------------------------------------
    # drt_list_connectors
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_list_connectors() -> dict[str, list[dict[str, str]]]:
        """List all available source and destination connectors.

        Returns:
            Dict with 'sources' and 'destinations' lists, each containing
            connector name, type key, and install extras (if any).
        """
        return _list_connectors()

    # -----------------------------------------------------------------------
    # drt_dlq
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_dlq(sync_name: str | None = None, limit: int = 20) -> dict[str, Any]:
        """Inspect a sync's Dead Letter Queue — records that failed to load and
        are persisted for replay (``sync.dlq.enabled: true``, v0.7.9).

        Args:
            sync_name: Restrict to one sync. If omitted, returns queue depth for
                every sync that has a non-empty DLQ.
            limit: Max queued records to return for a single sync (default 20).

        Returns:
            Without ``sync_name``: ``{"depths": {sync_name: depth, ...}}``.
            With ``sync_name``: the queue ``depth`` plus up to ``limit`` records
            (each with the failed payload, error_message, http_status, timestamp,
            attempts) and a ``truncated`` flag.
        """
        return _dlq(ctx, sync_name, limit)

    # -----------------------------------------------------------------------
    # drt_retry
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_retry(
        sync_name: str,
        limit: int | None = None,
        dry_run: bool = False,
        clear: bool = False,
    ) -> dict[str, Any]:
        """Replay records from a sync's Dead Letter Queue (mirrors ``drt retry``).

        Re-sends queued records (stored post-mapping, so they replay verbatim),
        drops the ones that now succeed, and writes the rest back with a bumped
        attempt count.

        Args:
            sync_name: Sync whose DLQ to replay.
            limit: Only retry the oldest N queued records (default: all).
            dry_run: Report what would be retried without sending anything.
            clear: Discard the queue without replaying (records are lost).

        Returns:
            A summary with ``status`` ("empty" | "cleared" | "dry_run" | "ok")
            and, for a real run, ``succeeded`` / ``still_failing`` /
            ``remaining_depth`` counts.
        """
        return _retry(ctx, sync_name, limit=limit, dry_run=dry_run, clear=clear)

    # -----------------------------------------------------------------------
    # drt_get_manifest
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_get_manifest(
        include_state: bool = False, full_labels: bool = False, history_depth: int = 10
    ) -> dict[str, Any]:
        """Return the drt docs manifest — the machine-readable sync catalog and
        lineage graph (the ``--format json`` artifact of ``drt docs generate``).

        This is the structured view of the whole project: every sync, its source
        model and destination, the source→sync→destination edges, and each
        sync's declared column facts (``fields`` — renames + masks, schema v2).

        Args:
            include_state: Also embed what is machine-local: each sync's
                last-run state (status, records synced, timestamps), recent
                run history (``runs``), and DLQ depth.
            full_labels: Keep verbatim connection details (endpoints, senders,
                buckets) in destination labels and embedded error text.
                Defaults to the same docs-safe output as the CLI (#696/#698);
                enable only when the manifest stays in a trusted context,
                mirroring ``drt docs generate --full-labels``.
            history_depth: Recent runs per sync to embed when *include_state*
                (newest first; 0 disables). Mirrors ``--history-depth``.

        Returns:
            The manifest as a JSON-serializable dict (schema-versioned).
        """
        return _get_manifest(
            ctx,
            include_state=include_state,
            full_labels=full_labels,
            history_depth=history_depth,
        )

    # -----------------------------------------------------------------------
    # drt_list_profiles
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_list_profiles() -> dict[str, Any]:
        """List credential profiles from ``~/.drt/profiles.yml`` (v0.7.9).

        Read-only and secret-free — returns only each profile's name and source
        type, never credential values.

        Returns:
            ``{"profiles": [{"name": ..., "type": ...}, ...]}``.
        """
        return _list_profiles()

    # -----------------------------------------------------------------------
    # drt_test_profile
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_test_profile(name: str) -> dict[str, Any]:
        """Check connectivity for a credential profile (mirrors ``drt profile test``).

        Runs the profile's source ``test_connection`` — a lightweight diagnostic
        that complements ``drt_doctor``.

        Args:
            name: Profile name (from drt_list_profiles).

        Returns:
            ``{"name": ..., "type": ..., "ok": bool}`` and, on failure, an
            ``error`` message.
        """
        return _test_profile(name)

    # -----------------------------------------------------------------------
    # drt_doctor
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_doctor() -> dict[str, Any]:
        """Run environment diagnostics — the MCP equivalent of ``drt doctor``.

        Mirrors the CLI ``drt doctor`` (v0.7.0+) but returns a structured
        report instead of a console table. Useful for "why won't this drt
        project run?" before reading any code — catches missing env vars,
        malformed profile, uninstalled extras, etc.

        Returns:
            ``{"passed": bool, "checks": [{"category", "name", "ok",
            "message"}, ...]}`` where ``passed`` is False if any required
            check failed (project file / profile / Python version).
        """
        return _doctor()

    return mcp


def run() -> None:
    """Entry point for `drt mcp run`."""
    server = create_server()
    server.run()
