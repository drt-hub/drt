"""Implementation for the ``drt_run_test`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def run_test(ctx: McpContext, sync_name: str | None = None) -> dict[str, Any]:
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query, test_display_name

    syncs = ctx.load_syncs()
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
            sync_result["reason"] = f"tests not supported for {sync.destination.type} destinations"
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
                    {
                        "name": test_name,
                        "passed": passed,
                        "value": str(result_val),
                        "severity": test_def.severity,
                    }
                )
                # severity: warn (#779) is reported but must not flip
                # `status` to "failed" — same rule drt test / drt build
                # apply, so this tool doesn't drift from the CLI (#400).
                if not passed and test_def.severity != "warn":
                    had_failures = True
            except Exception as e:
                sync_result["tests"].append(
                    {
                        "name": test_name,
                        "passed": False,
                        "error": str(e),
                        "severity": test_def.severity,
                    }
                )
                if test_def.severity != "warn":
                    had_failures = True

        results.append(sync_result)

    return {
        "status": "failed" if had_failures else "passed",
        "results": results,
    }
