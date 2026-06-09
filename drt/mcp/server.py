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

        Returns:
            Result summary with success count, failed count, errors, and
            (when ``compute_diff=True``) a ``diff`` field with the
            structured preview.
        """
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
        profile = load_profile(project.profile)
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
        return {
            "sources": [
                {"name": "BigQuery", "type": "bigquery", "install": "drt-core[bigquery]"},
                {"name": "DuckDB", "type": "duckdb", "install": "(core)"},
                {"name": "SQLite", "type": "sqlite", "install": "(core)"},
                {"name": "PostgreSQL", "type": "postgres", "install": "drt-core[postgres]"},
                {"name": "Redshift", "type": "redshift", "install": "drt-core[redshift]"},
                {"name": "ClickHouse", "type": "clickhouse", "install": "drt-core[clickhouse]"},
                {"name": "Snowflake", "type": "snowflake", "install": "drt-core[snowflake]"},
                {"name": "MySQL", "type": "mysql", "install": "drt-core[mysql]"},
            ],
            "destinations": [
                {"name": "REST API", "type": "rest_api", "install": "(core)"},
                {"name": "Slack", "type": "slack", "install": "(core)"},
                {"name": "Discord", "type": "discord", "install": "(core)"},
                {"name": "Microsoft Teams", "type": "teams", "install": "(core)"},
                {"name": "GitHub Actions", "type": "github_actions", "install": "(core)"},
                {"name": "HubSpot", "type": "hubspot", "install": "(core)"},
                {"name": "Amplitude", "type": "amplitude", "install": "(core)"},
                {"name": "Zendesk", "type": "zendesk", "install": "(core)"},
                {"name": "Google Sheets", "type": "google_sheets", "install": "drt-core[sheets]"},
                {"name": "PostgreSQL", "type": "postgres", "install": "drt-core[postgres]"},
                {"name": "MySQL", "type": "mysql", "install": "drt-core[mysql]"},
                {"name": "ClickHouse", "type": "clickhouse", "install": "drt-core[clickhouse]"},
                {"name": "Parquet", "type": "parquet", "install": "drt-core[parquet]"},
                {"name": "CSV/JSON/JSONL", "type": "file", "install": "(core)"},
                {"name": "Jira", "type": "jira", "install": "(core)"},
                {"name": "Linear", "type": "linear", "install": "(core)"},
                {"name": "SendGrid", "type": "sendgrid", "install": "(core)"},
                {"name": "Notion", "type": "notion", "install": "(core)"},
            ],
        }

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
