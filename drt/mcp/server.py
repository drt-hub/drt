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
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def create_server(project_dir: Path | None = None) -> Any:
    """Create and return a configured FastMCP server instance."""
    try:
        from fastmcp import FastMCP
    except ImportError as e:
        raise ImportError("MCP server requires: pip install drt-core[mcp]") from e

    _project_dir = project_dir or Path(".")

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
        from drt.config.parser import load_syncs

        syncs = load_syncs(_project_dir)
        return [
            {
                "name": s.name,
                "description": s.description,
                "model": s.model,
                "destination_type": s.destination.type,
                "mode": s.sync.mode,
            }
            for s in syncs
        ]

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

        Returns:
            Result summary with success count, failed count, errors, and
            (when ``compute_diff=True``) a ``diff`` field with the
            structured preview.
        """
        from drt.cli._helpers import resolve_profile_name
        from drt.cli.main import _get_destination, _get_source
        from drt.config.credentials import load_profile
        from drt.config.parser import load_project, load_syncs
        from drt.engine.sync import run_sync
        from drt.state.manager import StateManager

        if compute_diff and not dry_run:
            return {
                "error": "compute_diff requires dry_run=True (matches the "
                "`drt run --dry-run --diff` CLI contract)."
            }

        project = load_project(_project_dir)
        profile = load_profile(resolve_profile_name(profile_name, project.profile))
        syncs = load_syncs(_project_dir)

        matched = [s for s in syncs if s.name == sync_name]
        if not matched:
            return {"error": f"No sync named '{sync_name}' found."}

        sync = matched[0]
        source = _get_source(profile)
        dest = _get_destination(sync)
        state_mgr = StateManager(_project_dir)

        result = run_sync(
            sync,
            source,
            dest,
            profile,
            _project_dir,
            dry_run,
            state_mgr,
            cursor_value_override=(cursor_value if sync.sync.mode == "incremental" else None),
            compute_diff=compute_diff,
            diff_limit=diff_limit,
        )

        response: dict[str, Any] = {
            "sync_name": sync_name,
            "dry_run": dry_run,
            "success": result.success,
            "failed": result.failed,
            "errors": result.errors[:10],  # cap at 10 to avoid huge payloads
        }
        diff_value = getattr(result, "diff", None)
        if compute_diff and diff_value is not None:
            from drt.cli.output import diff_to_dict

            response["diff"] = diff_to_dict(diff_value)
        return response

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
                - `tests`: list of {name, passed, value} or {name, passed: false, error}
                - `skipped` (optional): true when destination type isn't queryable
                - `reason` (optional): why the sync was skipped
        """
        from drt.config.parser import load_syncs
        from drt.destinations.query import (
            execute_test_query,
            get_table_name,
            is_queryable,
        )
        from drt.engine.test_runner import build_test_query, test_display_name

        syncs = load_syncs(_project_dir)
        if not syncs:
            return {"status": "no_syncs", "results": []}

        if sync_name is not None:
            syncs = [s for s in syncs if s.name == sync_name]
            if not syncs:
                return {"error": f"No sync named '{sync_name}' found."}

        syncs_with_tests = [s for s in syncs if s.tests]
        if not syncs_with_tests:
            return {"status": "no_tests", "results": []}

        had_failures = False
        results: list[dict[str, Any]] = []

        for sync in syncs_with_tests:
            sync_result: dict[str, Any] = {"sync": sync.name, "tests": []}

            if not is_queryable(sync.destination):
                sync_result["skipped"] = True
                sync_result["reason"] = (
                    f"tests not supported for {sync.destination.type} destinations"
                )
                results.append(sync_result)
                continue

            table = get_table_name(sync.destination)
            for test_def in sync.tests:
                test_name = test_display_name(test_def)
                try:
                    query, check = build_test_query(test_def, table)
                    result_val = execute_test_query(sync.destination, query)
                    passed = check(result_val)
                    sync_result["tests"].append(
                        {"name": test_name, "passed": passed, "value": str(result_val)}
                    )
                    if not passed:
                        had_failures = True
                except Exception as e:
                    sync_result["tests"].append(
                        {"name": test_name, "passed": False, "error": str(e)}
                    )
                    had_failures = True

            results.append(sync_result)

        return {
            "status": "failed" if had_failures else "passed",
            "results": results,
        }

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
        from drt.state.manager import StateManager

        states = StateManager(_project_dir).get_all()

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
        from dataclasses import asdict

        from drt.state.history import HistoryManager

        entries = HistoryManager(_project_dir).read(sync_name=sync_name, limit=limit)
        return {"entries": [asdict(e) for e in entries]}

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
        from drt.config.parser import load_syncs_safe

        result = load_syncs_safe(_project_dir)
        return {
            "valid": [s.name for s in result.syncs],
            "errors": result.errors,
        }

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
        from drt.config.schema import generate_project_schema, generate_sync_schema

        if schema_type == "project":
            return generate_project_schema()
        return generate_sync_schema()

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
        # Derived from the drt.config.connectors SSoT (kept in lockstep with
        # drt/connectors/registry.py by test_cli_list_connectors), so this
        # inventory can never fall out of sync with the registry.
        from drt.config.connectors import connector_inventory

        return connector_inventory()

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
        from dataclasses import asdict

        from drt.state.dlq import DlqStore

        store = DlqStore(_project_dir)
        if sync_name is None:
            return {"depths": store.all_depths()}

        depth = store.depth(sync_name)
        records = [asdict(e) for e in store.read(sync_name)[:limit]]
        return {
            "sync_name": sync_name,
            "depth": depth,
            "records": records,
            "truncated": depth > len(records),
        }

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
        from drt.cli.commands.retry import replay_dead_letters
        from drt.config.parser import load_syncs

        if limit is not None and limit < 0:
            return {"error": "limit must be >= 0."}

        syncs = load_syncs(_project_dir)
        sync = next((s for s in syncs if s.name == sync_name), None)
        if sync is None:
            return {"error": f"No sync named '{sync_name}' found."}

        return replay_dead_letters(
            sync, limit=limit, dry_run=dry_run, clear=clear, project_dir=_project_dir
        )

    # -----------------------------------------------------------------------
    # drt_get_manifest
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_get_manifest(include_state: bool = False, full_labels: bool = False) -> dict[str, Any]:
        """Return the drt docs manifest — the machine-readable sync catalog and
        lineage graph (the ``--format json`` artifact of ``drt docs generate``).

        This is the structured view of the whole project: every sync, its source
        model and destination, and the source→sync→destination edges.

        Args:
            include_state: Also embed each sync's last-run state (status, records
                synced, timestamps) when available.
            full_labels: Keep verbatim connection details (endpoints, senders,
                buckets) in destination labels. Defaults to the same docs-safe
                labels as the CLI (#696); enable only when the manifest stays
                in a trusted context, mirroring ``drt docs generate --full-labels``.

        Returns:
            The manifest as a JSON-serializable dict (schema-versioned).
        """
        from drt.docs.builder import build_manifest

        return build_manifest(
            _project_dir, include_state=include_state, full_labels=full_labels
        ).to_dict()

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
        from drt.config.credentials import load_raw_profiles

        profiles = load_raw_profiles()
        return {
            "profiles": [
                {"name": name, "type": (raw.get("type") if isinstance(raw, dict) else None)}
                for name, raw in profiles.items()
            ]
        }

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
        from drt.config.credentials import load_profile
        from drt.connectors.registry import get_source

        try:
            profile = load_profile(name)
        except (FileNotFoundError, KeyError, ValueError) as e:
            return {"name": name, "ok": False, "error": str(e)}

        source = get_source(profile)
        try:
            ok = source.test_connection(profile)
        except Exception as e:
            return {"name": name, "type": profile.type, "ok": False, "error": str(e)}

        return {"name": name, "type": profile.type, "ok": bool(ok)}

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
        from drt import __version__ as drt_version
        from drt.cli.doctor import (
            _check_env_vars,
            _check_extras,
            _check_profile,
            _check_project_file,
            _check_python,
            _check_syncs,
        )

        checks: list[dict[str, Any]] = []
        required_ok = True

        py_ok, py_msg = _check_python()
        checks.append(
            {"category": "runtime", "name": "Python version", "ok": py_ok, "message": py_msg}
        )
        required_ok = required_ok and py_ok

        checks.append(
            {
                "category": "runtime",
                "name": "drt version",
                "ok": True,
                "message": drt_version,
            }
        )

        proj_ok, proj_msg, project_data = _check_project_file()
        checks.append(
            {"category": "project", "name": "Project file", "ok": proj_ok, "message": proj_msg}
        )
        required_ok = required_ok and proj_ok

        if project_data:
            prof_ok, prof_msg = _check_profile(project_data)
            checks.append(
                {"category": "project", "name": "Profile", "ok": prof_ok, "message": prof_msg}
            )
            required_ok = required_ok and prof_ok

            _, syncs_ok, syncs_msg = _check_syncs(project_data)
            checks.append(
                {"category": "project", "name": "Syncs", "ok": syncs_ok, "message": syncs_msg}
            )

        for label, ok, msg in _check_extras():
            # Extras are optional — they affect ``ok`` of the row but not
            # overall ``passed``. A user can run a duckdb-only project with
            # no other extras installed and that's fine.
            checks.append({"category": "extras", "name": label, "ok": ok, "message": msg})

        for var, ok, msg in _check_env_vars(project_data):
            checks.append({"category": "env", "name": var, "ok": ok, "message": msg})

        return {"passed": required_ok, "checks": checks}

    return mcp


def run() -> None:
    """Entry point for `drt mcp run`."""
    server = create_server()
    server.run()
