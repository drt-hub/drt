"""Implementation for the ``drt_run_test`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.mcp._context import McpContext


def _run_unit_tests(syncs: list[SyncConfig]) -> dict[str, Any]:
    """``unit=True`` path — sync.unit_tests (#780), no destination touched.

    Separate from the ``sync.tests:`` loop below for the same reason
    ``drt test --unit`` keeps its own function: no destination connection,
    no ``severity`` tier, none of that loop's machinery applies. Delegates
    to :func:`drt.engine.unit_test_runner.run_unit_test` — the same engine
    function the CLI calls, so the two surfaces can't drift on what a unit
    test result means, only on how it's presented.
    """
    from drt.engine.unit_test_runner import UnitTestLookupsUnsupportedError, run_unit_test

    syncs_with_unit_tests = [s for s in syncs if s.unit_tests]
    if not syncs_with_unit_tests:
        return {"status": "no_tests", "results": []}

    had_failures = False
    results: list[dict[str, Any]] = []

    for sync in syncs_with_unit_tests:
        sync_result: dict[str, Any] = {"sync": sync.name, "tests": []}
        try:
            for test_def in sync.unit_tests:
                result = run_unit_test(sync, test_def)
                sync_result["tests"].append(
                    {
                        "name": test_def.name,
                        "passed": result.passed,
                        "mismatches": result.mismatches,
                    }
                )
                if not result.passed:
                    had_failures = True
        except UnitTestLookupsUnsupportedError as e:
            sync_result["tests"].append({"name": sync.name, "passed": False, "error": str(e)})
            had_failures = True
        results.append(sync_result)

    return {"status": "failed" if had_failures else "passed", "results": results}


def run_test(
    ctx: McpContext,
    sync_name: str | None = None,
    unit: bool = False,
    dry_run: bool = False,
    fail_fast: bool = False,
    store_failures: bool = False,
    store_failures_limit: int = 10,
) -> dict[str, Any]:
    # #851: the per-test loop (connect, build query, execute, check,
    # severity, error handling) lives in exactly one place —
    # `execute_tests_for_sync`, which `drt test` and `drt build` already
    # share. Re-implementing it here is how #400 happened. `run_test_suite`
    # (#870) is the same sharing one level up, for the multi-sync
    # `--fail-fast` loop `drt test` already had. This tool is now only the
    # selection + envelope around both.
    from drt.cli.commands.test import run_test_suite

    if unit and (dry_run or store_failures):
        return {"error": "unit cannot be combined with dry_run or store_failures."}

    syncs = ctx.load_syncs()
    if not syncs:
        return {"status": "no_syncs", "results": []}

    if sync_name is not None:
        syncs = [s for s in syncs if s.name == sync_name]
        if not syncs:
            return {"error": f"No sync named '{sync_name}' found."}

    if unit:
        return _run_unit_tests(syncs)

    syncs_with_tests = [s for s in syncs if s.tests]
    if not syncs_with_tests:
        return {"status": "no_tests", "results": []}

    results, had_failures = run_test_suite(
        syncs_with_tests,
        dry_run=dry_run,
        # An MCP tool returns structured data over the transport and has
        # never printed to the console; `json_mode` alone already
        # silences it, and `quiet` keeps that true if the two ever come
        # apart.
        json_mode=True,
        quiet=True,
        # `--store-failures` (#870) writes .drt/test_failures/… on disk;
        # the returned entry's `failures_stored.path` is a filesystem path
        # under the project directory an MCP client with its own file
        # access (the common case — an agent driving drt via MCP usually
        # also has Read/Bash on the same checkout) can open directly. MCP
        # itself never reads it back.
        store_failures=store_failures,
        store_failures_limit=store_failures_limit,
        fail_fast=fail_fast,
        # An MCP server is created with an explicit project_dir and its
        # process cwd is whatever the client happened to spawn it from.
        # Everything else in this tool already resolves through `ctx` for
        # that reason.
        project_dir=ctx.project_dir,
    )

    response: dict[str, Any] = {
        "status": "failed" if had_failures else "passed",
        "results": results,
    }
    if dry_run:
        response["dry_run"] = True
    return response
