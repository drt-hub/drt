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
from drt.cli._selection import (
    SelectionError,
    complete_selector,
    is_state_only_select,
    select_syncs,
)
from drt.cli.output import (
    console,
    print_error,
    print_validation_error,
    print_validation_ok,
)


def _fnmatch_token(name: str, token: str) -> bool:
    """Bare-name/glob select tokens only — method tokens (tag:, destination:)
    cannot match unparseable syncs, which exist solely as error-dict keys."""
    if token in ("*", "all"):
        return True
    if ":" in token:
        return False
    from fnmatch import fnmatchcase

    return fnmatchcase(name, token)


def _match_error_keys(errors: dict[str, Any], select: list[str]) -> set[str]:
    return {k for k in errors if any(_fnmatch_token(k, t) for t in select)}


@app.command()
def validate(
    select: list[str] = typer.Option(
        None,
        "--select",
        "-s",
        help=(
            "Select syncs: name or glob, tag:<pattern>, destination:<type>, "
            'state:modified/state:new, or "*" / "all". Repeat to union.'
        ),
        autocompletion=complete_selector,
    ),
    exclude: list[str] = typer.Option(
        None,
        "--exclude",
        help="Subtract syncs from the selection (same grammar as --select). Repeatable.",
        autocompletion=complete_selector,
    ),
    state: Path | None = typer.Option(
        None,
        "--state",
        help=(
            "Baseline manifest path for state:modified/state:new selectors "
            "(for example, a prior `drt docs generate --format json` CI artifact)."
        ),
    ),
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
      drt validate --select state:modified --state ci-baseline/manifest.json
      drt validate --emit-schema
      drt validate --strict
    """

    from drt.config.parser import load_syncs_safe
    from drt.config.schema import write_schemas
    from drt.config.secrets import find_hardcoded_secrets

    result = load_syncs_safe(Path("."))
    secret_findings = find_hardcoded_secrets(Path("."))

    if select or exclude:
        # Resolve method/glob selectors against the parseable syncs. Broken
        # syncs never parse, so bare-name/glob select tokens additionally
        # match error keys directly — `drt validate --select broken_sync`
        # must still surface that sync's errors.
        try:
            if state is not None:
                from drt.cli._state_selection import load_state_diff

                state_diff = load_state_diff(state, result.syncs, Path("."))
                selected = select_syncs(
                    result.syncs, select, exclude, state_diff=state_diff
                )
            else:
                selected = select_syncs(result.syncs, select, exclude)
        except SelectionError as e:
            error_keys: set[str] = set()
            if select:
                error_keys = _match_error_keys(result.errors, select)
            if not error_keys:
                print_error(str(e))
                raise typer.Exit(1)
            selected = []
        selected_names = {s.name for s in selected}
        if state is not None:
            # A broken sync has no SyncConfig, so it was never in the
            # `current_syncs` passed to load_state_diff() and can't be
            # matched via state-diff membership; unlike tag:/destination:,
            # error-dict keys are also file *stems*, a different identifier
            # space than the fingerprint map's resolved sync names, so there
            # is no safe way to reconcile "was this broken file part of the
            # change" by name. Always keep every parse/validation error when
            # a state: selector is active rather than risk silently dropping
            # a changed-but-broken sync from a green `drt validate` run —
            # a false positive here (an unrelated broken sync reported) is
            # safe; a false negative (a changed broken sync going unreported)
            # is exactly the failure mode state-aware CI must not have.
            error_names = set(result.errors)
        else:
            error_names = _match_error_keys(result.errors, select) if select else set(result.errors)
        for token in exclude or ():
            error_names = {k for k in error_names if not _fnmatch_token(k, token)}
        result.syncs = selected
        result.errors = {k: v for k, v in result.errors.items() if k in error_names}
        result.deprecations = {
            k: v for k, v in result.deprecations.items() if k in selected_names
        }
        secret_findings = [f for f in secret_findings if f.sync_name in selected_names]
        if not result.syncs and not result.errors:
            if is_state_only_select(select):
                console.print(
                    "[dim]No syncs changed relative to the baseline — nothing to validate.[/dim]"
                )
                raise typer.Exit()
            print_error("Selection matched no syncs (after --exclude).")
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
    from drt.connectors.registry import get_destination
    from drt.destinations.base import ConnectionTestable

    dest_config = sync.destination
    try:
        dest = get_destination(dest_config)
        if isinstance(dest, ConnectionTestable):
            dest.test_connection(dest_config)
            return {"success": True, "error": None, "skipped": False}
        return {"success": None, "error": None, "skipped": True}
    except Exception as e:
        return {"success": False, "error": str(e), "skipped": False}
