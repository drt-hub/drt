"""``drt validate`` — sync schema validation + secret scan + optional connection probe.

Extracted from ``drt/cli/main.py`` in Phase 2b PR (b) of the #546 split
(tracked under #573). The two private helpers (``_group_secret_findings``,
``_run_connection_test``) move along with the command since they are not
used anywhere else.

Back-compat: ``drt.cli.main`` re-exports the underscore-prefixed helpers
so existing ``from drt.cli.main import _run_connection_test`` (used by
tests and library callers) keeps working.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.config.secrets import SecretFinding


from drt.cli._app import app
from drt.cli.output import (
    console,
    print_error,
    print_validation_error,
    print_validation_ok,
)


@app.command()
def validate(
    select: str = typer.Option(None, "--select", "-s", help="Validate a specific sync by name."),
    emit_schema: bool = typer.Option(  # noqa: E501
        False, "--emit-schema", help="Write JSON Schemas to .drt/schemas/."
    ),
    check_connection: bool = typer.Option(
        False, "--check-connection", help="Test connectivity to SQL destinations."
    ),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    strict: bool = typer.Option(False, "--strict", help="Treat warnings as validation errors."),
) -> None:
    """Validate sync definitions against the JSON Schema.

    Examples:
      drt validate
      drt validate --select post_users
      drt validate --emit-schema
      drt validate --strict
    """

    from drt.config.parser import load_syncs_safe
    from drt.config.schema import write_schemas
    from drt.config.secrets import find_hardcoded_secrets

    result = load_syncs_safe(Path("."))
    secret_findings = find_hardcoded_secrets(Path("."))

    if select:
        result.syncs = [s for s in result.syncs if s.name == select]
        result.errors = {k: v for k, v in result.errors.items() if k == select}
        result.deprecations = {k: v for k, v in result.deprecations.items() if k == select}
        secret_findings = [finding for finding in secret_findings if finding.sync_name == select]
        if not result.syncs and not result.errors:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    secret_warnings_by_sync = _group_secret_findings(secret_findings)

    if output == "json":
        # Collect all deprecations into a flat list for JSON output
        all_deprecations = []
        for sync_name, sync_deprecations in result.deprecations.items():
            all_deprecations.extend(sync_deprecations)

        results_json = []
        for s in result.syncs:
            entry = {
                "name": s.name,
                "valid": True,
                "deprecations": result.deprecations.get(s.name, []),
                "warnings": [
                    finding.to_dict() for finding in secret_warnings_by_sync.get(s.name, [])
                ],
            }
            if strict and entry["warnings"]:
                entry["valid"] = False
                entry["errors"] = [
                    finding.message for finding in secret_warnings_by_sync.get(s.name, [])
                ]
            if check_connection:
                entry["connection_test"] = _run_connection_test(s)
            results_json.append(entry)

        for name, errs in result.errors.items():
            results_json.append(
                {
                    "name": name,
                    "valid": False,
                    "errors": errs,
                    "warnings": [
                        finding.to_dict() for finding in secret_warnings_by_sync.get(name, [])
                    ],
                }
            )

        print(
            json.dumps(
                {"results": results_json},
                indent=2,
            )
        )
        if result.errors or (strict and secret_findings):
            raise typer.Exit(code=1)
        return

    if not result.syncs and not result.errors:
        console.print("[dim]No syncs found.[/dim]")
        return

    for sync in result.syncs:
        if strict and sync.name in secret_warnings_by_sync:
            continue
        print_validation_ok(sync.name)
        # Print deprecation warnings for this sync
        if sync.name in result.deprecations:
            for deprecation in result.deprecations[sync.name]:
                console.print(
                    f"  [yellow]⚠️  {deprecation['key']} is deprecated "
                    f"(removed in {deprecation['removed_in']})[/yellow]"
                )
                console.print(f"       Use {deprecation['replacement']} instead.")
                if deprecation["docs_link"]:
                    console.print(f"       See {deprecation['docs_link']}")

        for finding in secret_warnings_by_sync.get(sync.name, []):
            console.print(f"  [yellow]WARNING[/yellow] {finding.message}")

        if check_connection:
            from drt.cli.output import print_connection_test_result

            conn_res = _run_connection_test(sync)
            print_connection_test_result(
                sync.name,
                success=conn_res["success"],
                error=conn_res["error"],
            )

    for name, errors in result.errors.items():
        print_validation_error(name, errors)

    if strict:
        for name, findings in secret_warnings_by_sync.items():
            print_validation_error(name, [finding.message for finding in findings])

    if result.errors or (strict and secret_findings):
        raise typer.Exit(code=1)

    if emit_schema:
        schema_dir = Path(".") / ".drt" / "schemas"
        written = write_schemas(schema_dir)
        console.print(f"\n[dim]Schemas written to {schema_dir}/[/dim]")
        for p in written:
            console.print(f"  {p}")


def _group_secret_findings(
    findings: list[SecretFinding],
) -> dict[str, list[SecretFinding]]:
    grouped: dict[str, list[SecretFinding]] = {}
    for finding in findings:
        grouped.setdefault(finding.sync_name, []).append(finding)
    return grouped


def _run_connection_test(sync: SyncConfig) -> dict[str, Any]:
    """Internal helper to test connectivity for a sync's destination."""
    from drt.config.models import (
        ClickHouseDestinationConfig,
        MySQLDestinationConfig,
        PostgresDestinationConfig,
        SnowflakeDestinationConfig,
    )
    from drt.connectors.registry import get_destination
    from drt.destinations.base import ConnectionTestable

    dest_config = sync.destination
    is_sql = isinstance(
        dest_config,
        (
            PostgresDestinationConfig,
            MySQLDestinationConfig,
            ClickHouseDestinationConfig,
            SnowflakeDestinationConfig,
        ),
    )

    if not is_sql:
        return {"success": None, "error": None, "skipped": True}

    try:
        dest = get_destination(dest_config)
        if isinstance(dest, ConnectionTestable):
            dest.test_connection(dest_config)
            return {"success": True, "error": None, "skipped": False}
        else:
            return {
                "success": False,
                "error": "test_connection method missing",
                "skipped": False,
            }
    except Exception as e:
        return {"success": False, "error": str(e), "skipped": False}
