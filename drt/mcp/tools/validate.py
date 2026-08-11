"""Implementation for the ``drt_validate`` MCP tool.

Public docstring lives on the ``@mcp.tool()`` wrapper in
``drt/mcp/server.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.mcp._context import McpContext


def validate(
    ctx: McpContext,
    check_connection: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    """Mirrors ``drt validate``'s secret-scan + strict + connection-test
    behaviour (#870) — the pre-#870 version only re-ran schema parsing,
    silently dropping the hardcoded-secret warnings ``--strict`` promotes
    to errors on the CLI, so this tool couldn't answer the question
    ``--strict`` exists to answer.
    """
    from drt.cli.commands.validate import _group_secret_findings, _run_connection_test
    from drt.config.secrets import find_hardcoded_secrets

    result = ctx.load_syncs_safe()
    secret_findings = find_hardcoded_secrets(ctx.project_dir)
    secret_warnings_by_sync = _group_secret_findings(secret_findings)

    errors = dict(result.errors)
    warnings: dict[str, list[str]] = {}
    valid: list[str] = []

    for s in result.syncs:
        sync_warnings = [f.message for f in secret_warnings_by_sync.get(s.name, [])]
        if sync_warnings:
            warnings[s.name] = sync_warnings
        if strict and sync_warnings:
            errors[s.name] = sync_warnings
        else:
            valid.append(s.name)

    response: dict[str, Any] = {"valid": valid, "errors": errors}
    if warnings:
        response["warnings"] = warnings
    if check_connection:
        response["connection_tests"] = {
            s.name: _run_connection_test(s) for s in result.syncs
        }
    return response
