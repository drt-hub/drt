"""Implementation for the ``drt_run_test`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.mcp._context import McpContext


def _run_unit_tests(syncs: list[SyncConfig], *, fail_fast: bool = False) -> dict[str, Any]:
    """``unit=True`` path — sync.unit_tests (#780), no destination touched.

    Delegates to :func:`drt.cli.commands.test.run_unit_test_suite`, shared
    with ``drt test --unit`` (#870) — this used to be a third inline copy
    of the per-sync unit-test loop with no ``--fail-fast`` support at all,
    which is exactly the #400/#851 failure mode one level up.
    """
    from drt.cli.commands.test import run_unit_test_suite

    syncs_with_unit_tests = [s for s in syncs if s.unit_tests]
    if not syncs_with_unit_tests:
        return {"status": "no_tests", "results": []}

    results, had_failures = run_unit_test_suite(
        syncs_with_unit_tests, fail_fast=fail_fast, json_mode=True, quiet=True
    )
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

    if store_failures and store_failures_limit < 1:
        # Matches the CLI's `min=1` (#870 review): unvalidated, a
        # non-positive limit reaches `fetch_failing_rows` as a SQL `LIMIT`
        # — 0 silently returns no sample, negative is invalid SQL on
        # several destinations.
        return {"error": "store_failures_limit must be a positive integer."}

    syncs = ctx.load_syncs()
    if not syncs:
        return {"status": "no_syncs", "results": []}

    if sync_name is not None:
        syncs = [s for s in syncs if s.name == sync_name]
        if not syncs:
            return {"error": f"No sync named '{sync_name}' found."}

    if unit:
        return _run_unit_tests(syncs, fail_fast=fail_fast)

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
