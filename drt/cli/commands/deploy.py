"""`drt deploy github-actions` — scaffold a scheduled sync workflow (#785).

Generates ``.github/workflows/drt-sync.yml`` wired to the official
``drt-hub/drt-action``, with connector extras inferred from the project's
profiles + sync definitions and every required secret enumerated as
``${{ secrets.NAME }}`` — the part users otherwise transcribe by hand from
connector docs. Prior art: ``dlt deploy github-action``.

The scanner reads YAML *raw* (no ``${VAR}`` expansion), so scaffolding works
in a fresh checkout where none of the runtime env vars are set yet.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import typer
import yaml

from drt.cli._app import app
from drt.cli.output import console, print_error

deploy_app = typer.Typer(
    name="deploy",
    help="Scaffold CI/CD deployment files for this project.",
    no_args_is_help=True,
)
app.add_typer(deploy_app)


# drt-core extras required per connector ``type`` (sources and destinations
# share names where both exist). Types not listed here ship in base
# drt-core. Guarded against connector-registry / pyproject drift by
# tests/unit/test_cli_deploy.py.
_TYPE_TO_EXTRA: dict[str, str] = {
    "azure_blob": "azure",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "deltalake": "deltalake",
    "duckdb": "duckdb",
    "gcs": "gcs",
    "google_sheets": "sheets",
    "iceberg": "iceberg",
    "mysql": "mysql",
    "parquet": "parquet",
    "postgres": "postgres",
    "redshift": "redshift",
    "s3": "s3",
    "snowflake": "snowflake",
    "sqlserver": "sqlserver",
}

_ENV_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_DEFAULT_OUTPUT = Path(".github/workflows/drt-sync.yml")


def _collect_env_refs(node: Any, envs: set[str]) -> None:
    """Recursively collect the *values* of ``*_env`` keys (env var names)."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(key, str) and key.endswith("_env") and isinstance(value, str):
                envs.add(value)
            _collect_env_refs(value, envs)
    elif isinstance(node, list):
        for item in node:
            _collect_env_refs(item, envs)


def _scan_project(project_dir: Path) -> tuple[set[str], set[str], bool]:
    """Return (env var names, connector types, repo profiles.yml present)."""
    envs: set[str] = set()
    types: set[str] = set()

    sync_files = sorted((project_dir / "syncs").glob("*.yml")) + sorted(
        (project_dir / "syncs").glob("*.yaml")
    )
    texts: list[str] = []
    for path in sync_files:
        text = path.read_text(encoding="utf-8")
        texts.append(text)
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError:
            continue  # drt validate owns YAML errors; the scaffolder skips
        if not isinstance(data, dict):
            continue
        destination = data.get("destination")
        if isinstance(destination, dict) and isinstance(destination.get("type"), str):
            types.add(destination["type"])
        _collect_env_refs(data, envs)

    project_file = project_dir / "drt_project.yml"
    if project_file.exists():
        texts.append(project_file.read_text(encoding="utf-8"))

    # drt-action stages a repo-committed profiles.yml (its `profiles-file`
    # input, default "profiles.yml") to ~/.drt — scan it for the source
    # connector type + credential env vars.
    profiles_file = project_dir / "profiles.yml"
    has_profiles = profiles_file.exists()
    if has_profiles:
        text = profiles_file.read_text(encoding="utf-8")
        texts.append(text)
        try:
            profiles = yaml.safe_load(text)
        except yaml.YAMLError:
            profiles = None
        if isinstance(profiles, dict):
            entries = profiles.get("profiles", profiles)
            if isinstance(entries, dict):
                for entry in entries.values():
                    if isinstance(entry, dict):
                        if isinstance(entry.get("type"), str):
                            types.add(entry["type"])
                        _collect_env_refs(entry, envs)

    # ${VAR} placeholders anywhere in the raw YAML are runtime env vars too.
    for text in texts:
        envs.update(_ENV_PLACEHOLDER.findall(text))

    return envs, types, has_profiles


def _render_workflow(
    select: str,
    schedule: str | None,
    profile: str,
    extras: str,
    secrets: list[str],
) -> str:
    on_lines = ["on:", "  workflow_dispatch:"]
    if schedule:
        on_lines += ["  schedule:", f'    - cron: "{schedule}"']
    else:
        on_lines += [
            "  # Uncomment to run on a schedule (UTC):",
            "  # schedule:",
            '  #   - cron: "40 3 * * *"',
        ]

    with_lines = [f'          select: "{select}"']
    if extras:
        with_lines.append(f'          extras: "{extras}"')
    if profile:
        with_lines.append(f'          profile: "{profile}"')

    env_lines: list[str] = []
    if secrets:
        env_lines.append("        env:")
        env_lines += [f"          {name}: ${{{{ secrets.{name} }}}}" for name in secrets]

    lines = [
        "# Generated by `drt deploy github-actions` — review before committing.",
        "# Secrets below must exist in Settings → Secrets and variables → Actions.",
        "name: drt sync",
        "",
        *on_lines,
        "",
        "permissions:",
        "  contents: read",
        "",
        "jobs:",
        "  drt-sync:",
        "    runs-on: ubuntu-latest",
        "    steps:",
        "      - uses: actions/checkout@v4",
        "",
        "      - name: Run drt syncs",
        "        uses: drt-hub/drt-action@v1",
        "        with:",
        *with_lines,
        *env_lines,
        "",
    ]
    return "\n".join(lines)


@deploy_app.command(name="github-actions")
def deploy_github_actions(
    schedule: str = typer.Option(
        None,
        "--schedule",
        "-s",
        help='Cron schedule (UTC), e.g. "40 3 * * *". Omit for manual dispatch only.',
    ),
    select: str = typer.Option("*", "--select", help="Sync selector passed to drt-action."),
    profile: str = typer.Option(
        "", "--profile", "-p", help="Profile name passed to drt-action (empty = project default)."
    ),
    extras: str = typer.Option(
        None,
        "--extras",
        help="Override the inferred drt-core extras (comma-separated).",
    ),
    output: Path = typer.Option(
        _DEFAULT_OUTPUT, "--output", "-o", help="Workflow file path to write."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the workflow instead of writing it."
    ),
    force: bool = typer.Option(False, "--force", help="Overwrite an existing workflow file."),
) -> None:
    """Scaffold a GitHub Actions workflow that runs this project's syncs."""
    project_dir = Path(".")
    if not (project_dir / "drt_project.yml").exists():
        print_error(
            "No drt_project.yml found in the current directory. "
            "Run this from your drt project root (or `drt init` first)."
        )
        raise typer.Exit(code=1)

    if schedule is not None and len(schedule.split()) != 5:
        print_error(
            f'"{schedule}" does not look like a 5-field cron expression '
            '(e.g. "40 3 * * *").'
        )
        raise typer.Exit(code=1)

    envs, types, has_profiles = _scan_project(project_dir)
    inferred_extras = ",".join(
        sorted({_TYPE_TO_EXTRA[t] for t in types if t in _TYPE_TO_EXTRA})
    )
    effective_extras = extras if extras is not None else inferred_extras
    secrets = sorted(envs)

    content = _render_workflow(select, schedule, profile, effective_extras, secrets)

    if dry_run:
        console.print(content)
        return

    if output.exists() and not force:
        print_error(f"{output} already exists. Re-run with --force to overwrite.")
        raise typer.Exit(code=1)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")

    console.print(f"[green]✓ Wrote {output}[/green]")
    console.print("\n[bold]Next steps:[/bold]")
    step = 1
    if not has_profiles:
        console.print(
            f"  {step}. Commit a profiles.yml at the project root — drt-action stages it to "
            "~/.drt/profiles.yml (use *_env references, never inline secrets)."
        )
        step += 1
    if secrets:
        console.print(
            f"  {step}. Add these {len(secrets)} secret(s) in "
            "Settings → Secrets and variables → Actions:"
        )
        for name in secrets:
            console.print(f"       gh secret set {name}")
        step += 1
    console.print(f"  {step}. Commit the workflow and push — then run it from the Actions tab.")
    if not schedule:
        console.print(
            "     (No --schedule given: the workflow is manual-dispatch only; "
            "uncomment the cron block to schedule it.)"
        )
