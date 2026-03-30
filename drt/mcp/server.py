"""drt MCP Server — exposes drt operations as MCP tools.

Start with:
    uvx drt-core[mcp] mcp run          # from a drt project directory
    drt mcp run                         # if drt-core[mcp] is installed

Tools:
    drt_list_syncs   — list all sync definitions
    drt_run_sync     — run a specific sync (dry_run supported)
    drt_get_status   — get last sync result for a sync
    drt_validate     — validate all sync YAML configs
    drt_get_schema   — return JSON Schema for drt_project.yml / sync.yml
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def create_server(project_dir: Path | None = None) -> Any:
    """Create and return a configured FastMCP server instance."""
    try:
        from fastmcp import FastMCP
    except ImportError as e:
        raise ImportError(
            "MCP server requires: pip install drt-core[mcp]"
        ) from e

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
    def drt_run_sync(sync_name: str, dry_run: bool = False) -> dict[str, Any]:
        """Run a specific drt sync.

        Args:
            sync_name: Name of the sync to run (from drt_list_syncs).
            dry_run: If True, extracts data but does not write to destination.

        Returns:
            Result summary with success count, failed count, and any errors.
        """
        from drt.cli.main import _get_destination, _get_source
        from drt.config.credentials import load_profile
        from drt.config.parser import load_project, load_syncs
        from drt.engine.sync import run_sync
        from drt.state.manager import StateManager

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
            sync, source, dest, profile, _project_dir, dry_run, state_mgr  # type: ignore[arg-type]
        )

        return {
            "sync_name": sync_name,
            "dry_run": dry_run,
            "success": result.success,
            "failed": result.failed,
            "errors": result.errors[:10],  # cap at 10 to avoid huge payloads
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
    # drt_validate
    # -----------------------------------------------------------------------

    @mcp.tool()
    def drt_validate() -> dict[str, Any]:
        """Validate all sync YAML configs in the current project.

        Returns:
            Dict with 'valid' list of sync names and 'errors' dict of
            sync_name → error message for any invalid configs.
        """
        from drt.config.parser import load_syncs

        try:
            syncs = load_syncs(_project_dir)
            return {
                "valid": [s.name for s in syncs],
                "errors": {},
            }
        except Exception as e:
            return {
                "valid": [],
                "errors": {"_project": str(e)},
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

    return mcp


def run() -> None:
    """Entry point for `drt mcp run`."""
    server = create_server()
    server.run()
